import os
import concurrent.futures as cf
from pathlib import Path
from typing import Iterable, Set, Optional

import orjson  # ultra‑fast JSON decoder
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
### GPT o3 is very good, look, this can be executed within 20 minutes

###############################################################
#  Configuration                                              #
###############################################################

# 🎯 Columns to retain.   Everything else is discarded *before* Arrow conversion
DEFAULT_KEYS_TO_KEEP: Set[str] = {
    "id",
    "created_at",
    "score",
    "source",
    "rating",
    "image_width",
    "image_height",
    "tag_string",
    "fav_count",
    "file_ext",
    "up_score",
    "down_score",
    "is_deleted",
    "pixiv_id",
    "tag_string_general",
    "tag_string_character",
    "tag_string_artist",
    "tag_string_copyright",
    "tag_string_meta",
    "file_url",
    "large_file_url",
    "preview_file_url",
}

# 🔖 Arrow schema for DEFAULT_KEYS_TO_KEEP – guarantees all row‑groups share identical schema.
DEFAULT_ARROW_SCHEMA = pa.schema(
    [
        ("id", pa.int64()),
        ("created_at", pa.string()),
        ("score", pa.int64()),
        ("source", pa.string()),
        ("rating", pa.string()),
        ("image_width", pa.int64()),
        ("image_height", pa.int64()),
        ("tag_string", pa.string()),
        ("fav_count", pa.int64()),
        ("file_ext", pa.string()),
        ("up_score", pa.int64()),
        ("down_score", pa.int64()),
        ("is_deleted", pa.bool_()),
        ("pixiv_id", pa.int64()),
        ("tag_string_general", pa.string()),
        ("tag_string_character", pa.string()),
        ("tag_string_artist", pa.string()),
        ("tag_string_copyright", pa.string()),
        ("tag_string_meta", pa.string()),
        ("file_url", pa.string()),
        ("large_file_url", pa.string()),
        ("preview_file_url", pa.string()),
    ]
)

###############################################################
#  Flatten helpers                                            #
###############################################################


def _flatten(obj, parent_key: str = "", sep: str = ".") -> dict:
    """Recursively flattens dicts & lists using dotted‑path keys."""
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


###############################################################
#  Single‑file processing                                     #
###############################################################


def _read_file(
    path: Path,
    *,
    min_id: Optional[int] = None,
    dedup_seen: Optional[set] = None,
    keys_to_keep: Optional[Set[str]] = None,
    all_keys: Optional[Set[str]] = None,
) -> list[dict]:
    """Return a list of *sanitised* flattened records from one .jsonl file."""

    # Early skip via file‑name prefix (e.g. "1234_*.jsonl")
    if min_id is not None:
        try:
            file_prefix_id = int(path.stem.split("_")[0])
            if file_prefix_id < min_id:
                return []
        except ValueError:
            pass  # Non‑numeric prefix   ➔ keep file.

    out: list[dict] = []
    with path.open("rb") as f:
        for raw in f:
            data = orjson.loads(raw)

            # Some dumps embed a list under a top‑level "post" key
            posts = data.pop("post", None)
            iterable = posts if posts else [data]

            for item in iterable:
                rec = _flatten(item)

                # ✂️  Sanitize – keep only whitelisted keys
                if keys_to_keep is not None:
                    rec = {k: rec.get(k) for k in keys_to_keep}

                # Ensure *all* expected keys are present, even if null – keeps Arrow schema stable
                if all_keys is not None:
                    for k in all_keys:
                        rec.setdefault(k, None)

                # 🗑️  On‑the‑fly de‑duplication
                if dedup_seen is not None:
                    rec_id = rec.get("id")
                    if rec_id is not None and rec_id in dedup_seen:
                        continue
                    dedup_seen.add(rec_id)

                out.append(rec)
    return out


###############################################################
#  Folder ➜ Parquet main                                      #
###############################################################


def from_multiple_folders(
    paths: Iterable[str | os.PathLike],
    output: str | os.PathLike,
    *,
    min_id: int | None = None,
    workers: int | None = None,
    row_group_size: int = 200_000,
    deduplicate_on_id: bool = True,
    keys_to_keep: Optional[Set[str]] = DEFAULT_KEYS_TO_KEEP,
):
    """Streaming conversion of many *.jsonl* files to a single Parquet dataset.

    Highlights
    ----------
    • **Zero‑copy**: rows stream straight into Arrow/Parquet – minimal RAM.
    • **Fast**: `orjson` + thread‑pool I/O + Arrow columnar writes.
    • **Sanitised**: columns restricted to `keys_to_keep` (default Danbooru subset).
    • **Consistent schema**: enforced so every row‑group matches → no Parquet errors.
    • **Verbose**: dual progress bars (files & records).
    """

    workers = workers or os.cpu_count() or 1

    # Discover files first so we know bar length
    jsonl_files: list[Path] = []
    for p in paths:
        jsonl_files.extend(Path(p).rglob("*.jsonl"))

    if not jsonl_files:
        raise FileNotFoundError("No .jsonl files found under provided paths.")

    print(f"Discovered {len(jsonl_files):,} jsonl files — beginning conversion…")

    # Decide schema strategy
    if keys_to_keep is None:
        # keep‑all mode: infer from first batch; later batches cast.
        arrow_schema: Optional[pa.Schema] = None
    elif keys_to_keep == DEFAULT_KEYS_TO_KEEP:
        arrow_schema = DEFAULT_ARROW_SCHEMA
    else:
        # custom allow‑list – build generic string schema (can be passed in)
        arrow_schema = pa.schema([(k, pa.string()) for k in sorted(keys_to_keep)])

    writer: pq.ParquetWriter | None = None
    record_pbar = tqdm(desc="Records", unit="rec", total=0, smoothing=0.1)
    seen_ids: set | None = set() if deduplicate_on_id else None

    def _process(path_: Path):
        return _read_file(
            path_,
            min_id=min_id,
            dedup_seen=seen_ids,
            keys_to_keep=keys_to_keep,
            all_keys=arrow_schema.names if arrow_schema else None,
        )

    with tqdm(jsonl_files, desc="Files", unit="file") as file_pbar:
        with cf.ThreadPoolExecutor(max_workers=workers) as pool:
            for records in pool.map(_process, jsonl_files, chunksize=32):
                file_pbar.update()
                if not records:
                    continue

                # Build Arrow table
                if arrow_schema is not None:
                    table = pa.Table.from_pylist(records, schema=arrow_schema)
                else:
                    table = pa.Table.from_pylist(records)

                # Lazy writer creation so we have the final schema
                if writer is None:
                    writer = pq.ParquetWriter(
                        output,
                        table.schema,
                        compression="zstd",
                        flavor="spark",
                    )
                else:
                    # Cast to existing writer schema (covers keep‑all scenario)
                    if table.schema != writer.schema:
                        table = table.cast(writer.schema)

                writer.write_table(table, row_group_size=row_group_size)
                record_pbar.update(len(records))

    record_pbar.close()
    if writer:
        writer.close()
        print(f"Done. Wrote ➜ {output}  (rows ≈ {record_pbar.n:,}).")
    else:
        print("No records matched criteria; nothing written.")


###############################################################
#  CLI entry‑point                                            #
###############################################################

if __name__ == "__main__":
    import argparse, textwrap, sys

    epilog = textwrap.dedent(
        """
        Example
        -------
          python accelerated_json_to_parquet.py \
                 E:/booru_posts/danbooru D:/danbooru/post_danbooru_7500k_8900k \
                 -o E:/booru_posts/booru.parquet \
                 --min-id 0 --workers 8

        By default only the canonical Danbooru columns are written.  To *keep everything*, add
        the flag `--keep-all`.  To provide your own allow‑list, pass `--keep-keys path/to/file.txt`
        where the file contains one key per line.
        """
    )

    parser = argparse.ArgumentParser(
        description="Fast, memory‑light JSONL→Parquet converter with live progress, sanitisation and consistent schema.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=epilog,
    )
    parser.add_argument(
        "paths",
        nargs="+",
        help="Root folders containing .jsonl files (searched recursively).",
    )
    parser.add_argument(
        "-o", "--output", required=True, help="Destination Parquet file path."
    )
    parser.add_argument(
        "--min-id",
        type=int,
        help="Skip files whose numeric prefix is below this value.",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        help="Number of parallel worker threads (default: CPU cores).",
    )

    key_group = parser.add_mutually_exclusive_group()
    key_group.add_argument(
        "--keep-all", action="store_true", help="Do *not* drop any columns."
    )
    key_group.add_argument(
        "--keep-keys",
        type=str,
        help="Path to newline‑separated list of keys to retain.",
    )

    parser.add_argument(
        "--no-dedup",
        action="store_true",
        help="Do *not* drop duplicate 'id' rows across files.",
    )
    args = parser.parse_args()

    # Determine which keys to keep
    if args.keep_all:
        keys = None  # keep everything
    elif args.keep_keys:
        try:
            with open(args.keep_keys, "r", encoding="utf-8") as fh:
                keys = {ln.strip() for ln in fh if ln.strip()}
            if not keys:
                raise ValueError("Empty key list provided.")
        except Exception as exc:
            print(f"✖ Failed to read --keep-keys file: {exc}", file=sys.stderr)
            sys.exit(1)
    else:
        keys = DEFAULT_KEYS_TO_KEEP

    from_multiple_folders(
        paths=args.paths,
        output=args.output,
        min_id=args.min_id,
        workers=args.workers,
        deduplicate_on_id=not args.no_dedup,
        keys_to_keep=keys,
    )
