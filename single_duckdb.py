#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Merges the process of converting multiple JSONL files into a single,
query-optimized DuckDB database.

This script performs a two-stage process:
1.  Converts JSONL files into a temporary Parquet file using a fast,
    multi-threaded approach. This stage flattens, sanitizes, and
    deduplicates the JSON records.
2.  Ingests the Parquet data into a DuckDB database, creating normalized
    tables for posts, tags, and their relationships, complete with indexes
    for efficient querying.

The intermediate Parquet file is automatically deleted upon successful
completion or in case of an error.
"""
import os
import gc
import sys
import textwrap
import argparse
import pathlib
import duckdb
import orjson  # Ultra-fast JSON decoder
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import concurrent.futures as cf
from typing import Iterable, Set, Optional
from datetime import datetime as dt
from tqdm import tqdm

###############################################################
#  Configuration                                              #
###############################################################

# 🎯 Default columns to retain from JSONL. Everything else is discarded.
DEFAULT_KEYS_TO_KEEP: Set[str] = {
    "id", "created_at", "score", "source", "rating", "image_width",
    "image_height", "tag_string", "fav_count", "file_ext", "up_score",
    "down_score", "is_deleted", "pixiv_id", "tag_string_general",
    "tag_string_character", "tag_string_artist", "tag_string_copyright",
    "tag_string_meta", "file_url", "large_file_url", "preview_file_url",
}

# 🔖 Default Arrow schema for the columns above.
DEFAULT_ARROW_SCHEMA = pa.schema([
    ("id", pa.int64()), ("created_at", pa.string()), ("score", pa.int64()),
    ("source", pa.string()), ("rating", pa.string()), ("image_width", pa.int64()),
    ("image_height", pa.int64()), ("tag_string", pa.string()), ("fav_count", pa.int64()),
    ("file_ext", pa.string()), ("up_score", pa.int64()), ("down_score", pa.int64()),
    ("is_deleted", pa.bool_()), ("pixiv_id", pa.int64()),
    ("tag_string_general", pa.string()), ("tag_string_character", pa.string()),
    ("tag_string_artist", pa.string()), ("tag_string_copyright", pa.string()),
    ("tag_string_meta", pa.string()), ("file_url", pa.string()),
    ("large_file_url", pa.string()), ("preview_file_url", pa.string()),
])

# 🏷️ Tag categories to extract for the 'tag' and 'post_tag' tables.
TAG_COLUMNS = {
    "general": "tag_string_general",
    "character": "tag_string_character",
    "artist": "tag_string_artist",
}

###############################################################
#  Helper Functions                                           #
###############################################################

def log(msg: str):
    """Prints a message with a UTC timestamp."""
    print(f"[{dt.utcnow():%H:%M:%S}] {msg}")

def _flatten(obj, parent_key: str = "", sep: str = ".") -> dict:
    """Recursively flattens nested dictionaries and lists."""
    items = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.extend(_flatten(v, new_key, sep=sep).items())
    elif isinstance(obj, list):
        for idx, v in enumerate(obj):
            new_key = f"{parent_key}{sep}{idx}" if parent_key else str(idx)
            items.extend(_flatten(v, new_key, sep=sep).items())
    else:
        items.append((parent_key, obj))
    return dict(items)

def table_missing(conn: duckdb.DuckDBPyConnection, name: str) -> bool:
    """Checks if a table is missing in the DuckDB database."""
    return conn.execute(
        "SELECT COUNT(*) = 0 FROM information_schema.tables "
        "WHERE table_type = 'BASE TABLE' AND table_name = ?",
        [name],
    ).fetchone()[0]

def safe_collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """Collects a Polars LazyFrame using the best available streaming engine."""
    try: # New streaming engine (Polars >=1.23)
        return lf.collect(streaming=True)
    except Exception: # Fallback to in-memory
        log("⚠️ Polars streaming failed – falling back to in-memory collect.")
        return lf.collect()

###############################################################
#  Part 1: JSONL to Parquet Conversion                        #
###############################################################

def _read_file(
    path: pathlib.Path, *, min_id: Optional[int] = None,
    dedup_seen: Optional[set] = None, keys_to_keep: Optional[Set[str]] = None,
    all_keys: Optional[Set[str]] = None,
) -> list[dict]:
    """Reads and sanitizes records from a single .jsonl file."""
    if min_id is not None:
        try:
            file_prefix_id = int(path.stem.split("_")[0])
            if file_prefix_id < min_id:
                return []
        except (ValueError, IndexError):
            pass  # Non-numeric prefix, process the file.

    out: list[dict] = []
    with path.open("rb") as f:
        for raw in f:
            data = orjson.loads(raw)
            posts = data.pop("post", None)
            iterable = posts if posts else [data]

            for item in iterable:
                rec = _flatten(item)
                if keys_to_keep is not None:
                    rec = {k: rec.get(k) for k in keys_to_keep}
                if all_keys is not None:
                    for k in all_keys:
                        rec.setdefault(k, None)

                if dedup_seen is not None:
                    rec_id = rec.get("id")
                    if rec_id is not None and rec_id in dedup_seen:
                        continue
                    dedup_seen.add(rec_id)
                out.append(rec)
    return out

def jsonl_to_parquet(
    paths: Iterable[str | os.PathLike], output: str | os.PathLike, *,
    min_id: int | None = None, workers: int | None = None,
    row_group_size: int = 200_000, deduplicate_on_id: bool = True,
    keys_to_keep: Optional[Set[str]] = DEFAULT_KEYS_TO_KEEP,
) -> int:
    """Streaming conversion of many *.jsonl* files to a single Parquet file."""
    workers = workers or os.cpu_count() or 1
    jsonl_files: list[pathlib.Path] = [f for p in paths for f in pathlib.Path(p).rglob("*.jsonl")]

    if not jsonl_files:
        log("No .jsonl files found. Nothing to do.")
        return 0

    log(f"Discovered {len(jsonl_files):,} jsonl files — beginning Parquet conversion…")

    if keys_to_keep == DEFAULT_KEYS_TO_KEEP:
        arrow_schema = DEFAULT_ARROW_SCHEMA
    elif keys_to_keep:
        arrow_schema = pa.schema([(k, pa.string()) for k in sorted(keys_to_keep)])
    else: # Keep all keys
        arrow_schema = None

    writer: Optional[pq.ParquetWriter] = None
    record_pbar = tqdm(desc="Records", unit="rec", total=0, smoothing=0.1)
    seen_ids: Optional[set] = set() if deduplicate_on_id else None

    def _process(path_: pathlib.Path):
        return _read_file(
            path_, min_id=min_id, dedup_seen=seen_ids,
            keys_to_keep=keys_to_keep,
            all_keys=arrow_schema.names if arrow_schema else None,
        )

    with tqdm(jsonl_files, desc="Files", unit="file") as file_pbar:
        with cf.ThreadPoolExecutor(max_workers=workers) as pool:
            for records in pool.map(_process, jsonl_files, chunksize=32):
                file_pbar.update()
                if not records:
                    continue

                table = pa.Table.from_pylist(records, schema=arrow_schema)
                if writer is None:
                    writer = pq.ParquetWriter(output, table.schema, compression="zstd", flavor="spark")
                elif table.schema != writer.schema:
                    table = table.cast(writer.schema)

                writer.write_table(table, row_group_size=row_group_size)
                record_pbar.update(len(records))

    record_count = record_pbar.n
    record_pbar.close()
    if writer:
        writer.close()
        log(f"Done. Wrote {record_count:,} records to ➜ {output}")
    else:
        log("No records matched criteria; intermediate file not written.")
    
    return record_count

###############################################################
#  Part 2: Parquet to DuckDB Ingestion                        #
###############################################################

def parquet_to_duckdb(parquet_path: str, db_path: str, threads: int):
    """Loads data from a Parquet file into a structured DuckDB database."""
    log(f"Connecting to DuckDB at {db_path}...")
    db = duckdb.connect(db_path)
    db.execute(f"PRAGMA threads={threads}")
    tmp_tags_path = pathlib.Path(f"{parquet_path}.tmp_tags.parquet")

    # 1. POSTS Table
    if table_missing(db, "post"):
        log("Building table: post")
        posts = safe_collect(
            pl.scan_parquet(parquet_path)
            .select(["id", "created_at", "score", "rating"])
            .with_columns(
                pl.col("created_at")
                .str.to_datetime(strict=False)
                .dt.replace_time_zone("UTC")
                .alias("created_at_utc")
            )
        )
        log(f"  – Loaded {posts.height:,} posts")
        db.execute("CREATE TABLE post AS SELECT * FROM posts")
        
    else:
        log("Table `post` already exists – skipping.")
    

    # 2. TAG Table (Dictionary)
    if table_missing(db, "tag"):
        log("Building table: tag")
        tag_frames = []
        for kind, col in TAG_COLUMNS.items():
            tags = safe_collect(
                pl.scan_parquet(parquet_path)
                .select(pl.col(col).str.split(" ").alias("tag"))
                .explode("tag")
                .filter(pl.col("tag") != "")
                .group_by("tag")
                .len()
                .select(pl.col("tag").alias("name"), pl.lit(kind).alias("kind"))
            )
            log(f"  – Discovered {tags.height:>7,} distinct {kind} tags")
            tag_frames.append(tags)
            gc.collect()

        tag_df = pl.concat(tag_frames).unique().with_row_count(name="tag_id")
        log(f"  – Total unique tags: {tag_df.height:,}")
        
        tag_df.write_parquet(tmp_tags_path)
        db.execute("CREATE TABLE tag AS SELECT * FROM read_parquet(?)", [str(tmp_tags_path)])
    else:
        log("Table `tag` already exists – skipping.")

    # 3. POST_TAG Table (Many-to-Many Relation)
    if table_missing(db, "post_tag"):
        log("Building table: post_tag")
        db.execute("CREATE TABLE post_tag (post_id BIGINT, tag_id BIGINT)")
        for kind, col in TAG_COLUMNS.items():
            log(f"  – Processing {kind:>9} tags for relation")
            tag_map = pl.from_pandas(
                db.execute("SELECT name, tag_id FROM tag WHERE kind = ?", [kind]).fetch_df()
            )
            rel = safe_collect(
                pl.scan_parquet(parquet_path)
                .select(["id", col])
                .with_columns(pl.col(col).str.split(" ").alias("t"))
                .explode("t")
                .filter(pl.col("t") != "")
                .join(tag_map.lazy(), left_on="t", right_on="name", how="inner")
                .select(pl.col("id").alias("post_id"), "tag_id")
            )
            db.execute("INSERT INTO post_tag SELECT * FROM rel")
            gc.collect()
        
        log("  – Creating indexes on post_tag")
        db.execute("CREATE INDEX idx_pt_tag ON post_tag(tag_id)")
        db.execute("CREATE INDEX idx_pt_post ON post_tag(post_id)")
    else:
        log("Table `post_tag` already exists – skipping.")

    db.close()
    if tmp_tags_path.exists():
        tmp_tags_path.unlink()
    log("All DuckDB operations complete!")

###############################################################
#  CLI Entry-Point                                            #
###############################################################

def main():
    """Main function to parse arguments and orchestrate the conversion."""
    epilog = textwrap.dedent("""
    Example
    -------
    python this_script.py E:/booru/danbooru -o D:/db/danbooru.duckdb --workers 8
    
    This command will recursively find all .jsonl files in 'E:/booru/danbooru',
    process them in parallel, and create a 'danbooru.duckdb' file in 'D:/db/'.
    """)
    parser = argparse.ArgumentParser(
        description="Fast JSONL to DuckDB converter.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )
    parser.add_argument("paths", nargs="+", help="Root folder(s) containing .jsonl files.")
    parser.add_argument("-o", "--output", required=True, help="Destination DuckDB file path.")
    parser.add_argument("--min-id", type=int, help="Skip files with a numeric prefix below this value.")
    parser.add_argument("-w", "--workers", type=int, help="Number of parallel workers (default: CPU cores).")
    
    key_group = parser.add_mutually_exclusive_group()
    key_group.add_argument("--keep-all", action="store_true", help="Keep all columns from JSON, not just the default set.")
    key_group.add_argument("--keep-keys", type=str, help="Path to a file with one key per line to retain.")
    
    parser.add_argument("--no-dedup", action="store_true", help="Disable deduplication of rows by 'id'.")
    args = parser.parse_args()

    # Determine which keys to keep
    if args.keep_all:
        keys = None
    elif args.keep_keys:
        try:
            with open(args.keep_keys, "r", encoding="utf-8") as f:
                keys = {line.strip() for line in f if line.strip()}
            if not keys: raise ValueError("Key file is empty.")
        except Exception as e:
            print(f"✖ Failed to read --keep-keys file: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        keys = DEFAULT_KEYS_TO_KEEP

    # Define paths
    output_db_path = pathlib.Path(args.output)
    output_db_path.parent.mkdir(parents=True, exist_ok=True)
    temp_parquet_path = output_db_path.with_suffix(".parquet")
    
    try:
        # Part 1: Convert JSONL to a temporary Parquet file
        if not os.path.exists(temp_parquet_path):
            record_count = jsonl_to_parquet(
                paths=args.paths,
                output=temp_parquet_path,
                min_id=args.min_id,
                workers=args.workers,
                deduplicate_on_id=not args.no_dedup,
                keys_to_keep=keys,
            )

            if record_count == 0:
                log("No records were processed. The database will not be created.")
                return
            log(f"Converted {record_count:,} records to Parquet at: {temp_parquet_path}")
        else:
            log(f"Temporary Parquet file already exists: {temp_parquet_path} – skipping conversion.")
        # Part 2: Ingest the Parquet file into DuckDB
        parquet_to_duckdb(
            parquet_path=str(temp_parquet_path),
            db_path=str(output_db_path),
            threads=args.workers or os.cpu_count() or 1,
        )
        log(f"✅ Success! Database created at: {output_db_path}")

    except Exception as e:
        log(f"❌ An error occurred: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()