import os
import json
import logging
import requests
from PIL import Image
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from utils.proxyhandler import ProxyHandler

# Configure logging
LOG_FILE = "download_post.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def yield_posts(file_dir: str, from_id: int = 0, end_id: int = 7110548):
    """
    Generator that yields posts from JSONL files within the specified directory.

    :param file_dir: Directory containing JSONL files with post data.
    :param from_id:  Starting post ID to consider.
    :param end_id:   Ending post ID to consider.
    :yield:          JSON string lines representing individual posts.
    """
    files = []
    # Walk through all files; collect only those that match the from_id/end_id range.
    for root, dirs, filenames in os.walk(file_dir):
        for filename in filenames:
            if "_" not in filename:
                continue
            try:
                start, finish = filename.split(".")[0].split("_")
                start_id = int(start)
                finish_id = int(finish)
            except ValueError:
                continue

            # Skip if ID range doesn't overlap with desired range
            if start_id > finish_id or finish_id < from_id:
                continue
            files.append(os.path.join(root, filename))

    logging.info(f"Found {len(files)} file(s) in {file_dir}.")
    for file_path in files:
        logging.info(f"Reading {file_path}")
        with open(file_path, "r", encoding="utf-8") as f:
            yield from f.readlines()


def download_post(
    post_dict: dict,
    proxyhandler: ProxyHandler,
    pbar=None,
    no_split: bool = False,
    save_location: str = "G:/danbooru2023-c/",
    split_size: int = 1_000_000,
    max_retry: int = 10,
):
    """
    Download a single post (image) based on information in `post_dict`.

    :param post_dict:      Dictionary containing post information (id, file_ext, file_url, etc.).
    :param proxyhandler:   Instance of ProxyHandler for handling requests.
    :param pbar:           tqdm progress bar instance for updating progress.
    :param no_split:       If True, download file in a single request instead of chunks.
    :param save_location:  Base directory where files will be saved.
    :param split_size:     Chunk size (in bytes) for partial downloads (only when no_split=False).
    :param max_retry:      Maximum number of retries for size checks and downloads.
    """
    post_id = post_dict.get("id")
    if post_id is None:
        logging.warning("Skipping a post with no 'id' key.")
        return

    ext = post_dict.get("file_ext")
    download_target = post_dict.get("file_url", post_dict.get("large_file_url"))
    if not download_target:
        logging.info(f"Post {post_id} has no valid download URL.")
        return

    # Determine save path
    modulus = post_id % 1000
    save_location = save_location.rstrip("/") + "/"
    sub_dir = f"{modulus:04d}/"
    save_dir = os.path.join(save_location, sub_dir)
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, f"{post_id}.{ext}")

    # Skip videos
    if ext.lower() in ["webm", "mp4", "mov", "avi"]:
        logging.info(f"Skipping {post_id} because it's a video: extension {ext}")
        return

    # Validate existing file (if any)
    if os.path.exists(save_path):
        try:
            with Image.open(save_path) as im:
                im.load()
            if pbar:
                pbar.update(1)
            return
        except Exception:
            logging.warning(f"Invalid image found for post {post_id}, removing.")
            os.remove(save_path)

    # Confirm final extension from download link
    if "." in os.path.basename(download_target):
        ext_from_url = download_target.split(".")[-1].lower()
        # If the URL file extension is different from what the metadata says, use the URL's extension
        if ext_from_url not in (
            "jpg",
            "jpeg",
            "png",
            "gif",
            "webp",
            "webm",
            "mp4",
            "mov",
            "avi",
        ):
            ext_from_url = ext  # fallback if the extension from URL is unusual
        ext = ext_from_url

    # Get file size for partial download support
    filesize = None
    for attempt in range(max_retry):
        try:
            filesize = proxyhandler.filesize(download_target)
            if filesize is not None:
                break
        except KeyboardInterrupt:
            raise
        except Exception as e:
            logging.error(
                f"Attempt {attempt}/{max_retry} - Error getting filesize for {post_id}: {e}"
            )

    if filesize is None:
        logging.error(
            f"File size could not be determined for post {post_id}. Skipping."
        )
        return

    # Check if an existing file has the correct size
    if os.path.exists(save_path):
        if os.path.getsize(save_path) == filesize:
            if pbar:
                pbar.update(1)
            return
        else:
            logging.warning(
                f"Existing file for post {post_id} has incorrect size. "
                f"Expected {filesize}, got {os.path.getsize(save_path)}. Removing."
            )
            os.remove(save_path)

    # Download without splitting
    if no_split:
        content = None
        for attempt in range(max_retry):
            try:
                resp = proxyhandler.get(download_target)
                if resp and resp.status_code == 200:
                    content = resp.content
                    break
                else:
                    logging.warning(
                        f"Attempt {attempt}/{max_retry} - Invalid response for {post_id}: "
                        f"status={resp.status_code if resp else 'No response'}"
                    )
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logging.error(
                    f"Attempt {attempt}/{max_retry} - Error downloading {post_id}: {e}"
                )

        if not content:
            logging.error(
                f"Failed to download post {post_id} after {max_retry} attempts."
            )
            return

        # Validate size
        content_length = len(content)
        if content_length != filesize:
            logging.error(
                f"Size mismatch for post {post_id} in single download. "
                f"Expected {filesize}, got {content_length}."
            )
            return

        with open(save_path, "wb") as f:
            f.write(content)
        if pbar:
            pbar.update(1)
        return

    # Download in chunks
    offset_ranges = [
        (i, min(filesize, i + split_size)) for i in range(0, filesize, split_size)
    ]
    current_filesize = os.path.getsize(save_path) if os.path.exists(save_path) else 0

    with open(save_path, "ab" if current_filesize else "wb") as f:
        for start, end in offset_ranges:
            if start < current_filesize:
                # Already downloaded this chunk
                continue

            chunk_data = None
            for attempt in range(max_retry):
                try:
                    resp = proxyhandler.get_filepart(download_target, start, end - 1)
                    if resp and resp.status_code == 200:
                        chunk_data = resp.content
                        break
                    else:
                        logging.warning(
                            f"Attempt {attempt}/{max_retry} - Invalid response for {post_id} "
                            f"range {start}-{end}: status={resp.status_code if resp else 'No response'}"
                        )
                except KeyboardInterrupt:
                    raise
                except Exception as e:
                    logging.error(
                        f"Attempt {attempt}/{max_retry} - Error downloading {post_id} chunk: {e}"
                    )

            if not chunk_data:
                logging.error(
                    f"Failed to download chunk {start}-{end} for {post_id} after {max_retry} attempts."
                )
                return

            # Verify chunk size
            expected_size = end - start
            if len(chunk_data) != expected_size:
                logging.error(
                    f"Chunk size mismatch for {post_id}. Expected {expected_size}, "
                    f"got {len(chunk_data)}. Aborting download."
                )
                return

            f.write(chunk_data)

    # Verify final file size
    if os.path.getsize(save_path) != filesize:
        logging.error(
            f"Final size mismatch for post {post_id}. Expected {filesize}, got {os.path.getsize(save_path)}."
        )
        os.remove(save_path)
        return

    if pbar:
        pbar.update(1)


if __name__ == "__main__":
    proxy_list_file = r"D:\danbooru\ips.txt"
    missing_ids_file = r"E:\missing_ids.json"

    with open(missing_ids_file, "r", encoding="utf-8") as f:
        missing_ids = set(json.load(f))

    save_location = "D:/danbooru2025-update"
    allowed_file_exts = ["jpg", "jpeg", "png", "gif", "webp"]

    # Define the range of IDs you want to add to the "missing" list
    start_id = 8835715
    end_id = 8972455
    missing_ids.update(range(start_id, end_id))

    # Initialize proxy handler
    proxyhandler = ProxyHandler(
        proxy_list_file=proxy_list_file,
        wait_time=0.1,
        timeouts=20,
        proxy_auth="user:password_notdefault",
    )
    proxyhandler.check()

    total_posts_to_download = end_id - start_id
    pbar = tqdm(total=total_posts_to_download, desc="Downloading posts")

    # Multi-threaded download
    with ThreadPoolExecutor(max_workers=80) as executor:
        for post_line in yield_posts(
            from_id=start_id,
            end_id=end_id,
            file_dir=r"D:\danbooru\post_danbooru_7500k_8900k",
        ):
            try:
                post = json.loads(post_line)
                post_id = post.get("id")
                # Skip if this ID isn't missing
                if post_id not in missing_ids:
                    continue

                # Skip if it's animated
                if "animated" in post.get(
                    "tag_string_meta", ""
                ) or "ugoira" in post.get("tag_string_meta", ""):
                    continue

                # Skip if unsupported file extension
                if post.get("file_ext", "").lower() not in allowed_file_exts:
                    continue

                # Submit download task
                executor.submit(
                    download_post,
                    post_dict=post,
                    proxyhandler=proxyhandler,
                    pbar=pbar,
                    no_split=False,
                    save_location=save_location,
                    split_size=1_000_000,
                    max_retry=10,
                )

            except KeyboardInterrupt:
                raise
            except Exception as e:
                logging.error(f"Error parsing post line: {post_line}. Exception: {e}")

    pbar.close()
    logging.info("All downloads completed.")
