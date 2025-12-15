from __future__ import annotations
import argparse
import json
import os
import queue
import sys
import threading
import time
import duckdb
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

import requests
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    MofNCompleteColumn,
    TimeRemainingColumn,
    TaskID,
)
from rich.table import Table

try:
    import pyperclip  # type: ignore
except ImportError:  # pragma: no cover – keep import optional
    pyperclip = None

from utils.proxyhandler import ProxyHandler

# Global console for styled output
console = Console()

CHUNK = 1 << 15  # 32 KiB
PER_REQUEST_POSTS = 20  # Danbooru caps at 20 per request for anon users (100 for logged in)


def _safe_filename(url: str, post_id: int, exts_to_none: Iterable[str] = None) -> str:
    """Turn *file_url* into "<id>.<ext>"."""
    ext = Path(url).suffix or ""
    if exts_to_none and ext in exts_to_none:
        return None
    return f"{post_id}{ext}"


def build_danbooru_bulk_url(
    index: int,
    *,
    api_root: str,
    login: Optional[str] = None,
    api_key: Optional[str] = None,
) -> str:
    start = index - index % PER_REQUEST_POSTS
    end = start + PER_REQUEST_POSTS - 1
    url = f"{api_root}/posts.json?tags=id%3A{start}..{end}"
    if login and api_key:
        url += f"&login={login}&api_key={api_key}"
    return url


def build_gelbooru_bulk_url(index: int) -> str:
    start = index - index % PER_REQUEST_POSTS
    end = start + PER_REQUEST_POSTS - 1
    return (
        "https://gelbooru.com/index.php?page=dapi&s=post&q=index"
        f"&tags=id:%3E={start}+id:%3C={end}+score:%3E3&json=1&limit={PER_REQUEST_POSTS}"
    )


DAN_API_ROOT = "https://danbooru.donmai.us"


class DanbooruClient:
    caption_key = "tag_string"

    def __init__(
        self,
        login: Optional[str] = None,
        api_key: Optional[str] = None,
        *,
        timeout: int = 20,
        handler: Optional[ProxyHandler] = None,
    ) -> None:
        self.login = login
        self.api_key = api_key
        self.auth = (login, api_key) if login and api_key else None
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "buru-downloader/3.1"})
        self.timeout = timeout
        self.handler = handler

    def _get(self, url: str) -> Any:
        """Make GET request, optionally through proxy handler."""
        if self.handler:
            result = self.handler.get_response(url)
            if result is None:
                raise requests.RequestException(f"Proxy request failed for {url}")
            return result
        else:
            r = self.session.get(url, auth=self.auth, timeout=self.timeout)
            r.raise_for_status()
            return r.json()

    # ---- Tag search ------------------------------------------------------
    def search_posts(
        self,
        tags: str,
        *,
        limit: int = 20,
        page: int = 1,
        random: bool = False,
        raw: bool = False,
        min_score: Optional[int] = None,
    ) -> List[Mapping[str, Any]]:
        params_str = f"tags={tags}&limit={limit}&page={page}"
        if random:
            params_str += "&random=true"
        if self.login and self.api_key:
            params_str += f"&login={self.login}&api_key={self.api_key}"
        url = f"{DAN_API_ROOT}/posts.json?{params_str}"
        posts = self._get(url)
        return posts if raw else self._trim(posts)

    # ---- Bulk window -----------------------------------------------------
    def bulk_posts(self, index: int, *, raw: bool = False) -> List[Mapping[str, Any]]:
        url = build_danbooru_bulk_url(
            index, api_root=DAN_API_ROOT, login=self.login, api_key=self.api_key
        )
        posts = self._get(url)
        return posts if raw else self._trim(posts)

    # ---- Helpers ---------------------------------------------------------
    def _trim(self, posts: List[Mapping[str, Any]]):
        keep = (
            "id",
            "created_at",
            "file_url",
            "rating",
            "score",
            "tag_string",
            "tag_string_artist",
            "tag_string_character",
            "tag_string_copyright",
            "tag_string_meta",
            "tag_string_general",
        )
        return [{k: p.get(k) for k in keep} for p in posts]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.session.close()


DB_PATH = Path("danbooru.duckdb")


class LocalDanbooruClient:
    caption_key = "tag_string"

    def __init__(self, db_path: Path = DB_PATH):
        if not db_path.exists():
            raise FileNotFoundError(f"Database file not found: {db_path}")
        self.db = duckdb.connect(str(db_path))

    def search_posts(
        self,
        tags: str,
        *,
        limit: int = 20,
        page: int = 1,
        random: bool = False,
        raw: bool = False,
        score_min: Optional[int] = None,
    ) -> List[Mapping[str, Any]]:
        tag_list = tags.strip().split()
        tag_query = " INTERSECT ".join(
            [
                f"SELECT pt.post_id FROM post_tag pt JOIN tag t ON pt.tag_id = t.tag_id WHERE t.name = '{tag}'"
                for tag in tag_list
            ]
        )
        if score_min is not None:
            tag_query += f" AND pt.score >= {score_min}"

        offset = (page - 1) * limit
        post_ids = self.db.execute(
            f"SELECT post_id FROM ({tag_query}) LIMIT {limit} OFFSET {offset}"
        ).fetchall()

        posts = [self._build_post_dict(pid[0]) for pid in post_ids]
        return posts if raw else self._trim(posts)

    def bulk_posts(self, index: int, *, raw: bool = False) -> List[Mapping[str, Any]]:
        start = index - index % 100
        end = start + 99
        query = f"SELECT id FROM post WHERE id BETWEEN {start} AND {end}"
        ids = self.db.execute(query).fetchall()
        posts = [self._build_post_dict(pid[0]) for pid in ids]
        return posts if raw else self._trim(posts)

    def _build_post_dict(self, post_id: int) -> Mapping[str, Any]:
        post_df = self.db.execute(
            """
            SELECT id, created_at_utc, score, rating, file_url
            FROM post
            WHERE id = ?
            """,
            [post_id],
        ).fetchdf()

        if post_df.empty:
            return {}

        tag_df = self.db.execute(
            """
            SELECT t.kind, string_agg(t.name, ' ' ORDER BY t.name) AS tags
            FROM post_tag pt
            JOIN tag t ON pt.tag_id = t.tag_id
            WHERE pt.post_id = ?
            GROUP BY t.kind
            """,
            [post_id],
        ).fetchdf()

        tag_map = {row["kind"]: row["tags"] for _, row in tag_df.iterrows()}

        post = post_df.iloc[0].to_dict()
        return {
            "id": post["id"],
            "created_at": post["created_at_utc"],
            "file_url": post["file_url"],
            "rating": post["rating"],
            "score": post["score"],
            "tag_string_artist": tag_map.get("artist", ""),
            "tag_string_character": tag_map.get("character", ""),
            "tag_string_copyright": tag_map.get("copyright", ""),
            "tag_string_meta": tag_map.get("meta", ""),
            "tag_string_general": tag_map.get("general", ""),
            "tag_string": " ".join(tag_map.values()),
        }

    def _trim(self, posts: List[Mapping[str, Any]]) -> List[Mapping[str, Any]]:
        keep = (
            "id",
            "created_at",
            "file_url",
            "rating",
            "score",
            "tag_string",
            "tag_string_artist",
            "tag_string_character",
            "tag_string_copyright",
            "tag_string_meta",
            "tag_string_general",
        )
        return [{k: p.get(k) for k in keep} for p in posts]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.db.close()

GEL_API_ROOT = "https://gelbooru.com/index.php"


class GelbooruClient:
    caption_key = "tags"

    def __init__(self, *, timeout: int = 20):
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "buru-downloader/3.1"})
        self.timeout = timeout

    # ---- Tag search ------------------------------------------------------
    def search_posts(
        self,
        tags: str,
        *,
        limit: int = 100,
        page: int = 1,
        raw: bool = False,
        random: bool = False,  # not supported
        id_min: Optional[int] = None,
        id_max: Optional[int] = None,
        min_score: Optional[int] = None,
    ) -> List[Mapping[str, Any]]:
        pid = max(page - 1, 0)
        params = {
            "page": "dapi",
            "s": "post",
            "q": "index",
            "json": 1,
            "tags": tags,
            "limit": limit,
            "pid": pid,
            "id": f"10500000..11943167",
        }
        r = self.session.get(GEL_API_ROOT, params=params, timeout=self.timeout)
        r.raise_for_status()
        posts_raw = r.json().get("post", [])
        if isinstance(posts_raw, dict):  # single‑item quirk
            posts_raw = [posts_raw]
        return posts_raw if raw else self._trim(posts_raw)

    # ---- Bulk window -----------------------------------------------------
    def bulk_posts(self, index: int, *, raw: bool = False) -> List[Mapping[str, Any]]:
        url = build_gelbooru_bulk_url(index)
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        posts_raw = r.json().get("post", [])
        if isinstance(posts_raw, dict):
            posts_raw = [posts_raw]
        return posts_raw if raw else self._trim(posts_raw)

    # ---- Helpers ---------------------------------------------------------
    def _trim(self, posts: List[Mapping[str, Any]]):
        keep = ("id", "created_at", "file_url", "rating", "score", "tags")
        return [{k: p.get(k) for k in keep} for p in posts]

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.session.close()


def get_score_value(score: int) -> str:
    if score > 100:
        return "masterpiece"
    elif score > 50:
        return "best quality"
    elif score < 0:
        return "worst quality"
    return ""


def download_post_single(
    post: Mapping[str, Any],
    folder: Path,
    caption_key: str,
    overwrite: bool = False,
    sess: requests.Session | None = None,
    exts_to_none: Iterable[str] = (".webm", ".mp4", ".gif", ".zip"),
    min_score: Optional[int] = None,
):
    url, pid = post.get("file_url"), post.get("id")
    if not url or pid is None:
        return
    if min_score is not None and post.get("score", 0) < min_score:
        console.print(f"[dim]· Skipping {pid} (score {post.get('score', 0)} < {min_score})[/dim]")
        return
    fname = _safe_filename(url, int(pid), exts_to_none=exts_to_none)
    if fname is None:
        print(f"! Skipping {pid} with unsupported file type")
        return
    fpath = folder / fname
    cpath = folder / f"{pid}.txt"

    if not fpath.exists() or overwrite:
        try:
            with (sess or requests).get(url, stream=True, timeout=30) as r:  # type: ignore[arg-type]
                r.raise_for_status()
                with open(fpath, "wb") as fp:
                    for chunk in r.iter_content(CHUNK):
                        if chunk:
                            fp.write(chunk)
            console.print(f"[green]✓ Saved {fname}[/green]")
        except requests.RequestException as e:
            console.print(f"[red]! File failed {fname}: {e}[/red]")
            return
    else:
        console.print(f"[dim]· Skipping existing {fname}[/dim]")

    caption = format_caption(post)
    try:
        with open(cpath, "w", encoding="utf-8") as fp:
            fp.write(caption + "\n")
    except OSError as e:
        console.print(f"[red]! Caption failed for {pid}: {e}[/red]")


def download_post_handler(
    post: Mapping[str, Any],
    folder: Path,
    caption_key: str,
    overwrite: bool = False,
    handler: ProxyHandler | None = None,
    max_retry: int = 3,
    caption_only: bool = False,
    min_id: Optional[int] = None,
    max_id: Optional[int] = None,
    exts_to_none: Iterable[str] = (".webm", ".mp4", ".gif", ".zip"),
) -> None:
    url, pid = post.get("file_url"), post.get("id")
    if not url or pid is None:
        return
    if min_id and pid < min_id:
        console.print(f"[dim]· Skipping {pid} < {min_id}[/dim]")
        return
    if max_id and pid > max_id:
        console.print(f"[dim]· Skipping {pid} > {max_id}[/dim]")
        return
    fname = _safe_filename(url, int(pid), exts_to_none=exts_to_none)
    if fname is None:
        console.print(f"[yellow]! Skipping {pid} (unsupported file type)[/yellow]")
        return
    fpath = folder / fname
    cpath = folder / f"{pid}.txt"

    if not caption_only and (not fpath.exists() or overwrite):
        for i in range(max_retry):
            try:
                file_response = (
                    handler.get(url) if handler else requests.get(url, timeout=30)
                )
                if file_response and file_response.status_code in (200, 206):
                    content = file_response.content
                    with open(fpath, "wb") as f:
                        f.write(content)
                    console.print(f"[green]✓ Saved {fname}[/green]")
                    break
                console.print(f"[yellow]Retry {i + 1}/{max_retry} for {url}[/yellow]")
            except Exception as e:  # noqa: BLE001
                if isinstance(e, KeyboardInterrupt):
                    raise
                console.print(
                    f"[yellow]Exception {e} when downloading {url} — retrying {i + 1}/{max_retry}[/yellow]"
                )
        else:
            console.print(f"[red]! File failed {fname}: exceeded retries[/red]")
            return
    elif fpath.exists():
        console.print(f"[dim]· Skipping existing {fname}[/dim]")

    caption = format_caption(post)
    try:
        with open(cpath, "w", encoding="utf-8") as fp:
            fp.write(caption + "\n")
    except OSError as e:
        console.print(f"[red]! Caption failed for {pid}: {e}[/red]")


def format_caption(row: dict) -> str:
    """Build a human‑readable caption string from a Danbooru‑style row."""
    pieces: list[str] = []

    def add_tags(tag_blob: str | None, label: str | None = None):
        if not tag_blob:
            return
        for tag in tag_blob.split():
            tag = tag.replace("_", " ")
            pieces.append(f"{label}{tag}" if label else tag)

    if not "tag_string_character" in row and "tags" in row:
        pieces.append(row["tags"].replace(" ", ", ").replace("_", " "))
    # 1) Tags --------------------------------------------------------------
    add_tags(row.get("tag_string_artist"), "artist: ")
    add_tags(row.get("tag_string_character"), "character: ")
    add_tags(row.get("tag_string_copyright"), "copyright: ")
    add_tags(row.get("tag_string_meta"), "meta: ")
    add_tags(row.get("tag_string_general"))

    # 2) Rating ------------------------------------------------------------
    rating_map = {
        "g": "general",
        "s": "sensitive",
        "q": "questionable",
        "e": "explicit",
    }
    rating_code = (row.get("rating") or "").lower()
    if rating_code in rating_map:
        pieces.append(f"rating:{rating_map[rating_code]}")
    elif rating_code:
        pieces.append(f"rating:{rating_code}")

    # 3) Score quality -----------------------------------------------------
    if (score := row.get("score")) is not None:
        if score_string := get_score_value(score):
            pieces.append(f"score:{score_string}")

    # 4) Year --------------------------------------------------------------
    if row.get("year"):
        pieces.append(f"year:{row['year']}")

    # Final pass -----------------------------------------------------------
    pieces = [p for p in pieces if p.strip()]
    return ", ".join(pieces).replace(", ,", ", ")


def download_posts(
    posts: List[Mapping[str, Any]],
    folder: str | Path,
    *,
    caption_key: str,
    overwrite: bool = False,
    handler: ProxyHandler | None = None,
    max_workers: int = 20,
    caption_only: bool = False,
    min_id: Optional[int] = None,
    max_id: Optional[int] = None,
    exts_to_none: Iterable[str] = (".webm", ".mp4", ".gif", ".zip"),
) -> None:
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)

    if handler is None:
        console.print("[yellow]No proxy handler provided — using a single requests session[/yellow]")
        sess = requests.Session()
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Downloading posts", total=len(posts))
            for post in posts:
                download_post_single(
                    post, folder, caption_key, overwrite, sess, exts_to_none=exts_to_none
                )
                progress.advance(task)
    else:
        console.print(f"[cyan]Using ThreadPoolExecutor with {max_workers} workers[/cyan]")
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Downloading posts", total=len(posts))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(
                        download_post_handler,
                        post,
                        folder,
                        caption_key,
                        overwrite,
                        handler,
                        3,
                        caption_only,
                        min_id,
                        max_id,
                        exts_to_none=exts_to_none,
                    )
                    for post in posts
                ]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:  # noqa: BLE001
                        console.print(f"[red]Error: {e}[/red]")
                    finally:
                        progress.advance(task)


CUR_DIR = Path(__file__).parent
RESULT_DIR = CUR_DIR / "result"
# ---------------------------------------------------------------------------
# Clipboard monitor ---------------------------------------------------------
# ---------------------------------------------------------------------------


def _sanitize_tag_for_folder(tag: str) -> str:
    """Make *tag* safe as a path component."""
    forbidden = '<>:"/|?*'
    for ch in forbidden:
        tag = tag.replace(ch, "_")
    return tag.replace(" ", "_").replace(",", "_")


def _process_tags_worker(
    tag_q: "queue.Queue[str]",
    ns: argparse.Namespace,
    client: DanbooruClient | GelbooruClient,
    *,
    caption_key: str,
    handler: ProxyHandler,
):
    """
    Background thread: pulls tags from `tag_q` and runs the
    existing download pipeline.
    """
    while True:
        tags = tag_q.get()
        if tags is None: 
            break

        try:
            limit = ns.limit or (200 if ns.site == "danbooru" else 100) 
            if ns.all:
                generator = (
                    client.search_posts(
                        tags,
                        limit=limit,
                        page=pg,
                        random=ns.random,
                        raw=ns.raw,
                        min_score=ns.min_score,
                    )
                    for pg in range(ns.page, 1_000_000)
                )
                all_posts = []
                for batch in generator:
                    if not batch:
                        break
                    all_posts.extend(batch)
                    console.print(f"[cyan]Found {len(all_posts)} posts so far[/cyan]")
                    if ns.max_posts and len(all_posts) >= ns.max_posts:
                        all_posts = all_posts[: ns.max_posts]
                        break
                    time.sleep(1.1)
            else:
                all_posts = client.search_posts(
                    tags, limit=limit, page=ns.page, random=ns.random, raw=ns.raw
                )
            posts = all_posts
            console.print(f"[green]Found {len(posts)} posts for clipboard tags '{tags}'[/green]")
            base_dir = ns.download or RESULT_DIR
            if not base_dir:
                sys.exit(
                    "No download folder specified. Use --download or --download-auto."
                )
            tag_folder = _sanitize_tag_for_folder(tags)
            outdir = Path(base_dir) / tag_folder

            download_posts(
                posts,
                outdir,
                caption_key=caption_key,
                handler=handler,
                caption_only=ns.caption_only,
                min_id=ns.min_id,
                max_id=ns.max_id,
                exts_to_none=list(ns.exts_to_none) if ns.exts_to_none else tuple(),
            )
            console.print(f"[green]Downloaded {len(posts)} posts → {outdir}[/green]\n")
        except Exception as exc:  # noqa: BLE001
            console.print(f"[red]! Error during clipboard download: {exc}[/red]")
        finally:
            tag_q.task_done()


def monitor_clipboard(
    ns: argparse.Namespace,
    client: DanbooruClient | GelbooruClient,
    *,
    caption_key: str,
    handler: ProxyHandler,
    poll_interval: float | int | None = None,  # ← 1 s default
):
    """
    Lightweight clipboard poller.
    Every poll only reads the clipboard, normalises the string,
    and (if new) puts it onto a queue that a worker thread consumes.
    """
    if pyperclip is None:
        sys.exit(
            "pyperclip is required for --clip-monitor, but it is not installed.\n"
            "Install with: pip install pyperclip"
        )

    poll_interval = poll_interval or max(1.0, ns.clip_interval)

    seen: set[str] = set()
    tag_q: queue.Queue[str] = queue.Queue(maxsize=100) 

    worker = threading.Thread(
        target=_process_tags_worker,
        args=(tag_q, ns, client),
        kwargs=dict(caption_key=caption_key, handler=handler),
        daemon=True,
    )
    worker.start()

    console.print("[cyan]Entering clipboard monitor — press Ctrl-C to stop.[/cyan]")
    try:
        while True:
            try:
                clip: str = pyperclip.paste().strip()  # type: ignore[attr-defined]
            except pyperclip.PyperclipException as exc:  # type: ignore[attr-defined]
                console.print(f"[red]Clipboard access error: {exc}[/red]")
                time.sleep(poll_interval)
                continue

            if clip:
                clip = clip.replace(" ", "_")
                if clip not in seen:
                    seen.add(clip)
                    console.print(f"\n[cyan]📋 Clipboard changed — new tags: {clip}[/cyan]")
                    try:
                        tag_q.put_nowait(clip)
                    except queue.Full:
                        console.print("[yellow]! Tag queue is full — skipping this clipboard entry[/yellow]")

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        console.print("\n[yellow]Stopping clipboard monitor …[/yellow]")
        tag_q.put(None) 
        worker.join(timeout=5)


# ---------------------------------------------------------------------------
# CLI ----------------------------------------------------------------------
# ---------------------------------------------------------------------------


def _parse_cli() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Search / download from Danbooru or Gelbooru (tag, bulk, or clipboard mode)."
    )
    p.add_argument(
        "--site",
        choices=["danbooru", "gelbooru", "local-danbooru"],
        required=True,
        help="booru site",
    )

    # Tag‑search args ------------------------------------------------------
    p.add_argument("--tags", nargs="?", help="Tag query (omit with --bulk)")
    p.add_argument("--limit", type=int, help="Posts per page in tag mode")
    p.add_argument("--page", type=int, default=1, help="Page index (tag mode)")
    p.add_argument(
        "--random", action="store_true", help="Randomize (Danbooru tag mode)"
    )
    p.add_argument("--all", action="store_true", help="Stream all pages (tag mode)")
    p.add_argument("--max-posts", type=int, help="Cap total (tag mode)")

    # ID range filters -----------------------------------------------------
    p.add_argument("--min-id", type=int, help="Minimum post ID (bulk mode)")
    p.add_argument("--max-id", type=int, help="Maximum post ID (bulk mode)")

    # Bulk mode -----------------------------------------------------------
    p.add_argument(
        "--bulk",
        type=int,
        action="append",
        metavar="ID",
        help="Window containing this post ID; repeatable",
    )

    # Clipboard monitor ---------------------------------------------------
    p.add_argument(
        "--clip-monitor",
        action="store_true",
        help="Monitor clipboard for tags and auto‑download",
    )
    p.add_argument(
        "--clip-interval",
        type=float,
        default=2.0,
        help="Polling interval (seconds) for clipboard monitor",
    )

    # Proxy / networking --------------------------------------------------
    p.add_argument("--ips", type=str, default="ips", help="Proxy IP list file")
    p.add_argument(
        "--proxy_auth",
        type=str,
        default="user:password_notdefault",
        help="Proxy BASIC auth user:pass",
    )
    p.add_argument("--port", type=int, default=80, help="Proxy port")
    p.add_argument(
        "--wait_time",
        type=float,
        default=0.12,
        help="Delay (seconds) between requests per proxy (0.12s = ~8 req/s/proxy)",
    )
    p.add_argument(
        "--timeouts", type=int, default=50, help="Request timeout in seconds"
    )

    # Output / download ---------------------------------------------------
    p.add_argument("--download", metavar="FOLDER", help="Save files & captions here")
    p.add_argument(
        "--download-auto",
        action="store_true",
        help="Auto‑download to default folder structure",
    )
    p.add_argument("-o", "--output", help="Write metadata JSONL file")
    p.add_argument("--raw", action="store_true", help="Return full API JSON")
    p.add_argument("--caption-only", action="store_true", help="Only save captions")

    # File extensions to ignore ----------------------------------------
    p.add_argument(
        "--exts-to-none",
        nargs="*",
        default=[".webm", ".mp4", ".gif", ".zip"],
        help="File extensions to ignore (e.g. .webm, .mp4, .gif, .zip)",
    )

    p.add_argument(
        "--min-score",
        type=int,
        default=0,
        help="Minimum score to include in results (Local-Danbooru only)",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Entry‑point ---------------------------------------------------------------
# ---------------------------------------------------------------------------


def main() -> None:
    ns = _parse_cli()

    # Proxy handler -------------------------------------------------------
    handler = ProxyHandler(
        ns.ips,
        port=ns.port,
        wait_time=ns.wait_time,
        timeout=ns.timeouts,
        proxy_auth=ns.proxy_auth,
    )
    handler.check()
    console.print(f"[green]✓ Loaded {len(handler)} proxies[/green]")

    # Client selection ----------------------------------------------------
    if ns.site == "danbooru":
        client = DanbooruClient(
            os.getenv("DANBOORU_LOGIN"), os.getenv("DANBOORU_API_KEY"),
            handler=handler,
        )
        caption_key = DanbooruClient.caption_key
    elif ns.site == "local-danbooru":
        client = LocalDanbooruClient()
        caption_key = LocalDanbooruClient.caption_key
    else:
        client = GelbooruClient()
        caption_key = GelbooruClient.caption_key
    if ns.clip_monitor:
        monitor_clipboard(ns, client, caption_key=caption_key, handler=handler)
        return  # never returns unless monitor exits

    # ---------------------------------------------------------------------
    # Regular tag / bulk processing
    # ---------------------------------------------------------------------
    jsonl_already_written = False  # Track if we wrote JSONL incrementally
    try:
        # Auto-generate bulk windows if min-id/max-id specified without explicit --bulk
        if ns.min_id and ns.max_id and not ns.bulk:
            # Generate bulk window indices for the entire range
            bulk_indices = list(range(ns.min_id, ns.max_id + 1, PER_REQUEST_POSTS))
            num_workers = len(handler) * 3  # 3 concurrent requests per proxy
            console.print(f"[cyan]Fetching {len(bulk_indices)} bulk windows (IDs {ns.min_id}..{ns.max_id}) with {num_workers} workers[/cyan]")
            
            # Open output file for incremental writing if specified
            output_file = None
            if ns.output:
                output_file = open(ns.output, "w", encoding="utf-8")
                console.print(f"[cyan]Writing metadata incrementally to {ns.output}[/cyan]")
                jsonl_already_written = True  # Mark that we're writing incrementally
            
            all_posts: List[Mapping[str, Any]] = []
            seen: set[int] = set()
            seen_lock = threading.Lock()
            batch_counter = 0
            
            def fetch_bulk(idx: int, max_retries: int = 3) -> List[Mapping[str, Any]]:
                """Fetch a single bulk window with retry logic."""
                for attempt in range(max_retries):
                    try:
                        return client.bulk_posts(idx, raw=ns.raw)
                    except Exception as e:
                        if attempt < max_retries - 1:
                            wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                            console.print(f"[yellow]Warning: Batch {idx} failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {wait_time}s...[/yellow]")
                            time.sleep(wait_time)
                        else:
                            console.print(f"[red]Error: Batch {idx} failed after {max_retries} attempts: {e}[/red]")
                            return []
                return []
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                MofNCompleteColumn(),
                console=console,
            ) as progress:
                task = progress.add_task("Fetching bulk windows", total=len(bulk_indices))
                
                with ThreadPoolExecutor(max_workers=num_workers) as executor:
                    futures = {executor.submit(fetch_bulk, idx): idx for idx in bulk_indices}
                    
                    for future in as_completed(futures):
                        posts = future.result()
                        new_posts = []
                        with seen_lock:
                            for p in posts:
                                pid = p.get("id")
                                if pid not in seen and ns.min_id <= pid <= ns.max_id:
                                    all_posts.append(p)
                                    seen.add(pid)
                                    new_posts.append(p)
                                    # Write to JSONL incrementally
                                    if output_file:
                                        output_file.write(json.dumps(p, ensure_ascii=False) + "\n")
                                        output_file.flush()  # Ensure it's written to disk
                            
                            batch_counter += 1
                            # Verbose CLI output every 10 batches
                            if batch_counter % 10 == 0:
                                print(f"Progress: {batch_counter}/{len(bulk_indices)} batches, {len(all_posts)} posts found", flush=True)
                        
                        progress.update(task, description=f"Found {len(all_posts)} posts")
                        progress.advance(task)
            
            # Close the output file
            if output_file:
                output_file.close()
                console.print(f"[green]✓ Incremental JSONL write complete: {ns.output}[/green]")
        elif ns.bulk:
            all_posts = []
            seen: set[int] = set()
            for idx in ns.bulk:
                posts = client.bulk_posts(idx, raw=ns.raw)
                for p in posts:
                    pid = p.get("id")
                    if pid not in seen:
                        all_posts.append(p)
                        seen.add(pid)
        else:
            limit = ns.limit or (200 if ns.site == "danbooru" else 100)
            if ns.all:
                generator = (
                    client.search_posts(
                        ns.tags,
                        limit=limit,
                        page=pg,
                        random=ns.random,
                        raw=ns.raw,
                        min_score=ns.min_score,
                    )
                    for pg in range(ns.page, 1_000_000)
                )
                all_posts = []
                for batch in generator:
                    if not batch:
                        break
                    all_posts.extend(batch)
                    console.print(f"[cyan]Found {len(all_posts)} posts so far[/cyan]")
                    if ns.max_posts and len(all_posts) >= ns.max_posts:
                        all_posts = all_posts[: ns.max_posts]
                        break
                    time.sleep(1.1)
            else:
                all_posts = client.search_posts(
                    ns.tags, limit=limit, page=ns.page, random=ns.random, raw=ns.raw
                )
    except requests.RequestException as e:
        sys.exit(f"Request failed: {e}")

    console.print(f"[green]Found {len(all_posts)} posts[/green]")

    # ---------------------------------------------------------------------
    # Download -------------------------------------------------------------
    # ---------------------------------------------------------------------
    if ns.download:
        download_posts(
            all_posts,
            ns.download,
            caption_key=caption_key,
            handler=handler,
            caption_only=ns.caption_only,
            min_id=ns.min_id,
            max_id=ns.max_id,
            exts_to_none=list(ns.exts_to_none) if ns.exts_to_none else tuple(),
        )
        console.print(f"[green]Downloaded {len(all_posts)} posts to {ns.download}[/green]")
    elif ns.download_auto:
        base_dir = RESULT_DIR
        tag_folder = _sanitize_tag_for_folder(ns.tags)
        outdir = Path(base_dir) / tag_folder
        outdir.mkdir(parents=True, exist_ok=True)
        download_posts(
            all_posts,
            outdir,
            caption_key=caption_key,
            handler=handler,
            caption_only=ns.caption_only,
            min_id=ns.min_id,
            max_id=ns.max_id,
            exts_to_none=list(ns.exts_to_none) if ns.exts_to_none else tuple(),
        )
        console.print(f"[green]Downloaded {len(all_posts)} posts to {outdir}[/green]")

    # ---------------------------------------------------------------------
    # Metadata output ------------------------------------------------------
    # ---------------------------------------------------------------------
    if ns.output and not jsonl_already_written:
        with open(ns.output, "w", encoding="utf-8") as fp:
            for post in all_posts:
                fp.write(json.dumps(post, ensure_ascii=False) + "\n")
        console.print(f"[green]Wrote metadata for {len(all_posts)} posts → {ns.output}[/green]")
    elif not ns.download and not ns.download_auto:
        console.print_json(json.dumps(all_posts, ensure_ascii=False))


if __name__ == "__main__":
    main()
