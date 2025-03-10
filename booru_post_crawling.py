import argparse
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import cache
from pathlib import Path
from typing import List

from tqdm import tqdm

from utils.proxyhandler import ProxyHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Global variables
handler = None
PER_REQUEST_POSTS = 100
post_ids = set()


@cache
def split_query(start: int, end: int, mode: str = "danbooru") -> List[str]:
    """
    Returns the list of queries (URLs) in increments of PER_REQUEST_POSTS for Danbooru/Gelbooru.

    :param start: Starting ID
    :param end:   Ending ID
    :param mode:  'danbooru' or 'gelbooru'
    :return:      List of query URLs
    """
    assert mode in ["danbooru", "gelbooru"], f"Mode {mode} not supported."

    queries = []
    # Ensure the end is aligned with the PER_REQUEST_POSTS step
    if end % PER_REQUEST_POSTS != 0:
        end += PER_REQUEST_POSTS - (end % PER_REQUEST_POSTS)

    skipped = 0
    for i in tqdm(range(start, end, PER_REQUEST_POSTS), desc="Preparing queries"):
        if i in post_ids:
            skipped += 1
            continue
        if mode == "danbooru":
            queries.append(get_query_bulk(i))
        else:  # gelbooru
            queries.append(get_query_bulk_gelbooru(i))

    logger.info(
        f"Skipped {skipped} query segments because they were already in post_ids."
    )
    return queries


def get_query_bulk(index: int) -> str:
    """
    Returns the Danbooru query URL for a given index in increments of PER_REQUEST_POSTS.
    """
    start_idx = index - (index % PER_REQUEST_POSTS)
    end_idx = start_idx + PER_REQUEST_POSTS - 1
    return (
        f"https://danbooru.donmai.us/posts.json?"
        f"tags=id%3A{start_idx}..{end_idx}&limit={PER_REQUEST_POSTS}"
    )


def get_query_bulk_gelbooru(index: int) -> str:
    """
    Returns the Gelbooru query URL for a given index in increments of PER_REQUEST_POSTS.
    """
    start_idx = index - (index % PER_REQUEST_POSTS)
    end_idx = start_idx + PER_REQUEST_POSTS - 1
    return (
        "https://gelbooru.com/index.php?page=dapi&s=post&q=index&"
        f"tags=id:%3E={start_idx}+id:%3C={end_idx}+score:%3E3&json=1&limit={PER_REQUEST_POSTS}"
    )


def get_response(url: str):
    """
    Fetches the response for a given URL via the global `handler` ProxyHandler.

    :param url: Target URL for the API call
    :return:    JSON-decoded response or None on failure
    """
    try:
        resp = handler.get_response(url)
        return resp
    except Exception as exc:
        logger.error(f"Exception when getting response for {url}: {exc}")
        return None


def write_to_file(data, post_file: str = "posts.jsonl"):
    """
    Writes post data to a newline-delimited JSON file (JSONL).

    :param data:      Data to write (list or dict) as per Danbooru/Gelbooru API response.
    :param post_file: Path to output file.
    """
    output_path = Path(post_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # If file already exists, skip writing.
    if output_path.is_file():
        logger.debug(f"File {post_file} already exists. Skipping write.")
        return

    try:
        # Danbooru's JSON can have a 'post' key; Gelbooru's is typically a list.
        if isinstance(data, dict) and "post" in data:
            data = data["post"]
        if not isinstance(data, list):
            logger.warning(f"Expected a list of posts but got: {data}")

        with output_path.open("w", encoding="utf-8") as f:
            for post in data:
                if "id" not in post:
                    logger.warning(f"Post data has no 'id': {post}")
                    continue
                if post["id"] in post_ids:
                    continue
                f.write(json.dumps(post) + "\n")
                post_ids.add(post["id"])

    except Exception as exc:
        logger.error(f"Exception while writing to file {post_file}: {exc}")


def get_posts(query: str, post_file: str, pbar: tqdm = None):
    """
    Fetches posts from a given query URL and writes them to a JSONL file.

    :param query:     API request URL
    :param post_file: Output file to store the results
    :param pbar:      tqdm progress bar to update upon completion
    """
    output_path = Path(post_file)
    if output_path.is_file():
        if pbar:
            pbar.update(1)
        return

    resp = get_response(query)
    if resp is not None:
        write_to_file(resp, post_file=post_file)
    else:
        logger.error(f"Failed to fetch data from: {query}")

    if pbar:
        pbar.update(1)


def get_post_range(query: str, mode: str = "danbooru") -> tuple:
    """
    Extracts the numeric start/end range from the query URL for naming the output file.

    :param query: The query URL.
    :param mode:  'danbooru' or 'gelbooru'.
    :return:      (start_id, end_id) as integers.
    """
    if mode == "danbooru":
        # Example: "id%3A{start}..{end}"
        # Splitting on 'id%3A' -> "... {start}..{end} ..."
        ids_part = query.split("id%3A")[1].split("&")[0]
        start_str, end_str = ids_part.split("..")
        return int(start_str), int(end_str)
    else:
        # Gelbooru example: "id:%3E={start}+id:%3C={end}"
        start_part = query.split("id:%3E=")[1].split("+")[0]
        end_part = query.split("id:%3C=")[1].split("+")[0]
        return int(start_part), int(end_part)


def get_filename_for_query(
    query: str, post_save_dir: str, mode: str = "danbooru"
) -> str:
    """
    Returns a filename based on the query's range. Groups files into subdirectories by million.

    :param query:          The query URL
    :param post_save_dir:  Base directory for saving output files
    :param mode:           'danbooru' or 'gelbooru'
    :return:               A path string (e.g. "post_save_dir/{X}M/{start}_{end}.jsonl")
    """
    start_id, end_id = get_post_range(query, mode=mode)
    subdir = f"{start_id // 1_000_000}M"
    return str(Path(post_save_dir) / subdir / f"{start_id}_{end_id}.jsonl")


def get_posts_threaded(
    queries: List[str], post_save_dir: str = "post", mode: str = "danbooru"
):
    """
    Fetches posts from a list of queries in parallel using ThreadPoolExecutor.

    :param queries:        List of query URLs
    :param post_save_dir:  Base directory where JSONL files are saved
    :param mode:           'danbooru' or 'gelbooru'
    """
    logger.info(f"Fetching posts for {len(queries)} queries...")
    query_pbar = tqdm(total=len(queries), desc="Downloading queries")
    max_workers = max(1, len(handler.proxy_list) * 5)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                get_posts,
                query,
                get_filename_for_query(query, post_save_dir, mode),
                pbar=query_pbar,
            )
            for query in queries
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except KeyboardInterrupt:
                logger.error("Interrupted by user.")
                raise
            except Exception as exc:
                logger.error(f"Exception in worker: {exc}")

    query_pbar.close()


def main():
    parser = argparse.ArgumentParser(
        description="Downloads the posts from Danbooru/Gelbooru."
    )
    parser.add_argument("--start_id", type=int, required=True, help="The start ID.")
    parser.add_argument("--end_id", type=int, required=True, help="The end ID.")
    parser.add_argument(
        "--post_save_dir",
        type=str,
        required=True,
        help="Directory for saving post data.",
    )
    parser.add_argument(
        "--ips", type=str, required=True, help="File containing proxy IPs."
    )
    parser.add_argument(
        "--proxy_auth",
        type=str,
        help='Proxy authentication credentials (e.g., "user:pass").',
    )
    parser.add_argument(
        "--wait_time", type=float, default=0.1, help="Delay between requests."
    )
    parser.add_argument(
        "--timeouts", type=int, default=15, help="Request timeout in seconds."
    )
    parser.add_argument("--port", type=int, default=80, help="Proxy port.")
    parser.add_argument(
        "--mode",
        type=str,
        default="danbooru",
        choices=["danbooru", "gelbooru"],
        help="Mode (danbooru or gelbooru).",
    )

    args = parser.parse_args()

    # Global reference to the ProxyHandler
    global handler
    handler = ProxyHandler(
        proxy_list_file=args.ips,
        port=args.port,
        wait_time=args.wait_time,
        timeouts=args.timeouts,
        proxy_auth=args.proxy_auth,
    )
    handler.check()

    logger.info(f"Proxy handler initialized with {len(handler.proxy_list)} proxies.")

    # Prepare queries and download
    queries = split_query(args.start_id, args.end_id, mode=args.mode)
    get_posts_threaded(queries, post_save_dir=args.post_save_dir, mode=args.mode)
    logger.info("All downloads completed.")


if __name__ == "__main__":
    main()
