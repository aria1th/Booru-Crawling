import duckdb, datetime as dt, pandas as pd

DB_FILE = "danbooru.duckdb"
START = dt.datetime(2024, 11, 1, tzinfo=dt.timezone.utc)  # 2024-11-01 UTC

db = duckdb.connect(DB_FILE)

# ───────────────────────────── 1. build / refresh ──────────────────────────
# A.  Create (or refresh) one compact table that holds, for every
#     character-type tag:
#       • earliest post date  →  first_seen
#       • total posts that use it   → post_count
#
#    You can run this once after each bulk import, or wrap it in a
#    “CREATE TABLE IF NOT EXISTS …” guard if you prefer.

db.execute(
    """
CREATE OR REPLACE TABLE tag_character_stats AS
SELECT
    t.tag_id,
    t.name                       AS tag_name,
    MIN(p.created_at_utc)        AS first_seen,    -- earliest appearance
    COUNT(*)                     AS post_count
FROM tag      AS t
JOIN post_tag AS pt ON t.tag_id = pt.tag_id
JOIN post     AS p  ON p.id     = pt.post_id
WHERE t.kind = 'character'
GROUP BY t.tag_id, t.name
"""
)

db.execute(
    """
CREATE OR REPLACE TABLE tag_general_stats AS
SELECT
    t.tag_id,
    t.name                       AS tag_name,
    MIN(p.created_at_utc)        AS first_seen,    -- earliest appearance
    COUNT(*)                     AS post_count
FROM tag      AS t
JOIN post_tag AS pt ON t.tag_id = pt.tag_id
JOIN post     AS p  ON p.id     = pt.post_id
WHERE t.kind = 'general'
GROUP BY t.tag_id, t.name
"""
)

db.execute(
    """
CREATE OR REPLACE TABLE tag_artist_stats AS
SELECT
    t.tag_id,
    t.name                       AS tag_name,
    MIN(p.created_at_utc)        AS first_seen,    -- earliest appearance
    COUNT(*)                     AS post_count
FROM tag      AS t
JOIN post_tag AS pt ON t.tag_id = pt.tag_id
JOIN post     AS p  ON p.id     = pt.post_id
WHERE t.kind = 'artist'
GROUP BY t.tag_id, t.name
"""
)


# helpful index if you’ll query by date a lot
db.execute(
    """
CREATE INDEX IF NOT EXISTS idx_char_first_seen
ON tag_character_stats(first_seen)
"""
)

db.execute(
    """
CREATE INDEX IF NOT EXISTS idx_general_first_seen
ON tag_general_stats(first_seen)
"""
)
db.execute(
    """
CREATE INDEX IF NOT EXISTS idx_artist_first_seen
ON tag_artist_stats(first_seen)
"""
)

# ───────────────────────────── 2. simple query ─────────────────────────────
# Tags whose *first appearance* is on/after 2024-11-01 UTC

tags_character = db.execute(
    """
SELECT tag_id,
       tag_name,
       first_seen,
       post_count
FROM tag_character_stats
WHERE first_seen >= ?
AND post_count > 100
ORDER BY post_count DESC
""",
    [START],
).fetch_df()
print(tags_character.shape)
print(tags_character.head(20))  # peek at the first 20 rows

tags_general = db.execute(
    
    """
SELECT tag_id,
       tag_name,
       first_seen,
       post_count
FROM tag_general_stats
WHERE first_seen >= ?
AND post_count > 100
ORDER BY post_count DESC
""",
    [START],
).fetch_df()
print(tags_general.shape)
print(tags_general.head(20))  # peek at the first 20 rows

tags_artist = db.execute(
    """
SELECT tag_id,
       tag_name,
       first_seen,
       post_count
FROM tag_artist_stats
WHERE first_seen >= ?
AND post_count > 100
ORDER BY post_count DESC
""",
    [START],
).fetch_df()
print(tags_artist.shape)
print(tags_artist.head(20))  # peek at the first 20 rows
POST_ID = 5639214
post = db.execute("""
    SELECT id,
            created_at_utc,
            score,
            rating
    FROM post
    WHERE id = ?
""", [POST_ID]).fetch_df()
print("Post record")
print(post, end="\n\n")

# ── 2. all tags grouped by kind (artist / character / general / copyright)
tags = db.execute("""
    SELECT
        t.kind,
        string_agg(t.name, ' ' ORDER BY t.name) AS tags
    FROM post_tag AS pt
    JOIN tag      AS t ON t.tag_id = pt.tag_id
    WHERE pt.post_id = ?
    GROUP BY t.kind
    ORDER BY t.kind
""", [POST_ID]).fetch_df()
print("Tags grouped by kind")
print(tags)

db.close()

# export tag names to json as {"characters": ["name1", "name2", ...]}

import json
data = {}
data["characters"] = tags_character["tag_name"].tolist()
data["general"] = tags_general["tag_name"].tolist()
data["artist"] = tags_artist["tag_name"].tolist()

with open("tags.json", "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=4)
