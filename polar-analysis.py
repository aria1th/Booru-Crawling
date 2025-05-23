import os, gc, pathlib, duckdb, polars as pl
from datetime import datetime as dt

PARQUET = pathlib.Path("danbooru-simplified.parquet")
TMP_TAGS = pathlib.Path("tmp_tag_map.parquet")
DB_FILE = "danbooru.duckdb"


# ─────────────────────────── helpers ────────────────────────────
def log(msg: str):
    print(f"[{dt.utcnow():%H:%M:%S}] {msg}")


def table_missing(conn: duckdb.DuckDBPyConnection, name: str) -> bool:
    return conn.execute(
        "SELECT COUNT(*) = 0 FROM information_schema.tables "
        "WHERE table_type = 'BASE TABLE' AND table_name = ?",
        [name],
    ).fetchone()[0]


def safe_collect(lf: pl.LazyFrame) -> pl.DataFrame:
    """
    Collect a LazyFrame with the fastest engine that actually works on the
    current Polars build.

    Tries, in order:
      1. New streaming engine       (.collect(engine="polars"))
      2. Old streaming engine       (.collect(streaming=True))
      3. Plain in-memory collect    (.collect())
    Whichever succeeds first is returned.
    """
    # 1️⃣ new streaming engine (Polars ≥1.23 – may be experimental)
    try:
        return lf.collect(engine="streaming")
    except (TypeError, NotImplementedError):
        # the keyword isn't recognised on older Polars → try next option
        pass
    except BaseException as err:
        log(f"⚠️  new engine failed: {err.__class__.__name__} – falling back")

    # 2️⃣ old streaming engine (deprecated, may panic on Parquet)
    try:
        # keep the deprecation warning quiet by using the same code-path
        return lf.collect(streaming=True)
    except BaseException as err:
        log(f"⚠️  old engine failed: {err.__class__.__name__} – disabling streaming")

    # 3️⃣ last resort – no streaming (always works, just slower & heavier)
    return lf.collect()


# ────────────────────────── main script ─────────────────────────
DB = duckdb.connect(DB_FILE)
DB.execute("PRAGMA threads=4")

# 1. POSTS ───────────────────────────────────────────────────────
if table_missing(DB, "post"):
    log("Building table  post …")
    posts = safe_collect(
        pl.scan_parquet(PARQUET)
        .select(["id", "created_at", "score", "rating"])
        .with_columns(
            pl.col("created_at")
            .str.strptime(pl.Datetime, strict=False)
            .dt.convert_time_zone("UTC")
            .alias("created_at_utc")
        )
    )
    log(f"  – loaded {posts.shape[0]:,} posts")
    DB.execute("CREATE TABLE post AS SELECT * FROM posts")
    DB.execute(
        "CREATE INDEX IF NOT EXISTS idx_post_month "
        "ON post(date_trunc('month', created_at_utc))"
    )
else:
    log("Table  post  already present – skipping.")

# 2. TAG dictionary ──────────────────────────────────────────────
TAG_COLUMNS = {
    "general": "tag_string_general",
    "character": "tag_string_character",
    "artist": "tag_string_artist",
}

if table_missing(DB, "tag"):
    log("Building table  tag …")
    tag_frames = []
    for kind, col in TAG_COLUMNS.items():
        tags = safe_collect(
            pl.scan_parquet(PARQUET)
            .select(pl.col(col).str.split(" ").alias("tag"))
            .explode("tag")
            .filter(pl.col("tag") != "")
            .group_by("tag")
            .len()
            .select(pl.col("tag").alias("name"), pl.lit(kind).alias("kind"))
        )
        log(f"  – {tags.height:>7,} distinct {kind} tags")
        tag_frames.append(tags)
        gc.collect()

    tag_df = (
        pl.concat(tag_frames)
        .unique()
        .with_row_index(name="tag_id")  # modern API, no deprecation
    )
    log(f"  – {tag_df.height:,} unique tags in total")

    tag_df.write_parquet(TMP_TAGS)
    DB.execute("CREATE TABLE tag AS SELECT * FROM read_parquet(?)", [str(TMP_TAGS)])
else:
    log("Table  tag  already present – skipping.")

# 3. POST_TAG relation ───────────────────────────────────────────
if table_missing(DB, "post_tag"):
    log("Building table  post_tag …")
    for kind, col in TAG_COLUMNS.items():
        log(f"  – processing {kind:>9} tags")
        tag_map = pl.from_pandas(
            DB.execute("SELECT name, tag_id FROM tag WHERE kind = ?", [kind]).fetch_df()
        )

        rel = safe_collect(
            pl.scan_parquet(PARQUET)
            .select(["id", col])
            .with_columns(pl.col(col).str.split(" ").alias("t"))
            .explode("t")
            .filter(pl.col("t") != "")
            .join(tag_map.lazy(), left_on="t", right_on="name", how="inner")
            .select(pl.col("id").alias("post_id"), "tag_id")
        )

        if table_missing(DB, "post_tag"):
            DB.execute("CREATE TABLE post_tag AS SELECT * FROM rel")
        else:
            DB.execute("INSERT INTO post_tag SELECT * FROM rel")

    DB.execute("CREATE INDEX IF NOT EXISTS idx_pt_tag  ON post_tag(tag_id)")
    DB.execute("CREATE INDEX IF NOT EXISTS idx_pt_post ON post_tag(post_id)")
else:
    log("Table  post_tag  already present – skipping.")

DB.close()
log("All done – database ready to query!")
