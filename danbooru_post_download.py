from __future__ import annotations

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Optional

from PIL import Image
from rich.console import Console
from rich.panel import Panel
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    BarColumn,
    MofNCompleteColumn,
    TimeRemainingColumn,
)
from rich.table import Table

from utils.proxyhandler import ProxyHandler

# Global console for styled output
console = Console()

# Optional file logging (in addition to console)
LOG_FILE = "download_post.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(message)s")

def yield_posts(file_dir:str, from_id=0, last_id=7110548):
    """
    Yields the posts from JSONL files.
    Handles filename formats: 0_19.jsonl or posts_START_END.jsonl
    """
    files = []
    for root, _, filenames in os.walk(file_dir):
        for filename in filenames:
            if not filename.endswith('.jsonl'):
                continue
            # Handle different filename formats
            basename = filename.split(".")[0]
            parts = basename.split("_")
            
            # Try to extract start/end IDs from filename
            try:
                if len(parts) == 2 and parts[0].isdigit():
                    # Format: 0_19.jsonl
                    starting_id, file_last_id = int(parts[0]), int(parts[1])
                elif len(parts) == 3 and parts[0] == "posts":
                    # Format: posts_10237105_10404895.jsonl
                    starting_id, file_last_id = int(parts[1]), int(parts[2])
                else:
                    # Unknown format, include file anyway
                    files.append(os.path.join(root, filename))
                    continue
                    
                if starting_id > file_last_id:
                    continue
                if file_last_id < from_id:
                    continue
                files.append(os.path.join(root, filename))
            except ValueError:
                # Couldn't parse IDs, include file anyway
                files.append(os.path.join(root, filename))
    console.print(f"[cyan]Total {len(files)} files to process[/cyan]")
    for file in files:
        #print(f"Reading {file}")
        with open(file, 'r', encoding='utf-8') as f:
            yield from f.readlines()

def download_post(post_dict, proxyhandler: ProxyHandler, *, no_split=False, save_location="/tmp/danbooru/", split_size=1000000, max_retry=10, only_if_original: bool = False):
    """
    Downloads the post
    """
    try:
        post_id = post_dict['id']
        logging.info(f"Downloading {post_id}")
        ext = post_dict['file_ext']
        download_target = post_dict.get("large_file_url", post_dict.get("file_url"))
        if only_if_original:
            # is_found = "variants" in post_dict["media_asset"]
            # print(f"Checking {post_id}, {str(is_found)}")
            #"variants": [{"type": "180x180", "url": "https://cdn.donmai.us/180x180/b0/4d/b04d27928d35f1d97a6d20edb1004202.jpg", "width": 135, "height": 180, "file_ext": "jpg"}, {"type": "360x360", "url": "https://cdn.donmai.us/360x360/b0/4d/b04d27928d35f1d97a6d20edb1004202.jpg", "width": 270, "height": 360, "file_ext": "jpg"}, {"type": "720x720", "url": "https://cdn.donmai.us/720x720/b0/4d/b04d27928d35f1d97a6d20edb1004202.webp", "width": 540, "height": 720, "file_ext": "webp"}, {"type": "sample", "url": "https://cdn.donmai.us/sample/b0/4d/sample-b04d27928d35f1d97a6d20edb1004202.jpg", "width": 850, "height": 1133, "file_ext": "jpg"}, {"type": "original", "url": "https://cdn.donmai.us/original/b0/4d/b04d27928d35f1d97a6d20edb1004202.jpg", "width": 1200, "height": 1600, "file_ext": "jpg"}]
            variants = post_dict["media_asset"].get("variants")
            if variants:
                # print(f"Found variants {variants}")
                # find the variant which matches with download_target
                target = None
                for variant in variants:
                    if variant.get("url") == download_target:
                        target = variant
                        # print(f"Found target {variant}")
                        target_width = variant.get("width")
                        target_height = variant.get("height")
                        break
                for variant in variants:
                    if variant.get("type") == "original":
                        if target:
                            if variant.get("width") < target_width or variant.get("height") < target_height:
                                console.print(f"[red]Error: {post_id} has smaller original variant {variant.get('width')}x{variant.get('height')}, expected {target_width}x{target_height}[/red]")
                                return
                        download_target = variant.get("url")
                        ext = variant.get("file_ext")
                        break
                else:
                    console.print(f"[red]Error: {post_id} has no original variant[/red]")
                    return
        save_path = save_location +f"{post_id % 100}/"+ f"{post_id}.{ext}"
        if not os.path.exists(save_location +f"{post_id % 100}/"):
            os.makedirs(save_location +f"{post_id % 100}/")
        # if url contains file extension, use that
        if download_target and "." in download_target:
            ext = download_target.split(".")[-1]
        # skip video files
        if ext in ["webm", "mp4", "mov", "avi", "zip"]:
            logging.info(f"Skipping {post_id} because it's a video")
            return
        if not download_target:
            logging.info(f"Error: {post_id} has no download target, dict: {post_dict}") # gold account?
            #print(f"Error: {post_id} has no download target, dict: {post_dict}") # gold account?
            return
        if os.path.exists(save_path):
            # Validate existing file
            img = None
            try:
                img = Image.open(save_path)
                img.load()
                img.close()
                return  # Already downloaded and valid
            except Exception as e:
                console.print(f"[yellow]Error: {post_id} has corrupted image {save_path}, {e}[/yellow]")
                os.remove(save_path)
            finally:
                if img:
                    img.close()
        for i in range(max_retry):
            try:
                filesize = proxyhandler.filesize(download_target)
            except Exception as e:
                if isinstance(e, KeyboardInterrupt):
                    raise e
                console.print(f"[yellow]Exception: {e} when getting filesize of {post_id}, retrying {i}/{max_retry}[/yellow]")
                filesize = None
            if filesize is not None:
                break
        if filesize is None:
            console.print(f"[red]Error: {post_id} has no filesize after {max_retry} retries[/red]")
            return

        if os.path.exists(save_path):
            # check file size
            if os.path.getsize(save_path) != filesize:
                console.print(f"[yellow]Error: {post_id} had different file size saved, expected {filesize}, got {os.path.getsize(save_path)}[/yellow]")
                os.remove(save_path)
            else:
                return  # Already downloaded with correct size
        if no_split:
            file_response = None
            for i in range(max_retry):
                try:
                    file_response = proxyhandler.get(download_target)
                    if file_response and file_response.status_code in (200, 206):
                        break
                    else:
                        console.print(f"[yellow]Error: {post_id}, {file_response}[/yellow]")
                except Exception as e:
                    if isinstance(e, KeyboardInterrupt):
                        raise e
                    console.print(f"[yellow]Exception: {e} when downloading {post_id}, retrying {i}/{max_retry}[/yellow]")
            
            if file_response is None or file_response.status_code not in (200, 206):
                console.print(f"[red]Error: {post_id} download failed after {max_retry} retries[/red]")
                return
            
            content = file_response.content
            filesize = file_response.headers.get('Content-Length')
            
            # Verify file size if available
            if filesize is not None:
                try:
                    if int(filesize) != len(content):
                        console.print(f"[yellow]Warning: {post_id} size mismatch, expected {filesize}, got {len(content)}[/yellow]")
                except ValueError:
                    pass
            
            # Save file
            with open(save_path, 'wb') as f:
                f.write(content)
        else:
            datas = [] # max 1MB per request
            if filesize is None:
                console.print(f"[red]Error: {post_id} has no filesize[/red]")
                return
            for i in range(0, filesize, split_size):
                datas.append((i, min(filesize, i + split_size)))
            # download
            current_filesize = os.path.getsize(save_path) if os.path.exists(save_path) else 0
            if current_filesize:
                console.print(f"[cyan]Resuming {post_id} from {current_filesize}, to {filesize}[/cyan]")
            with open(save_path, 'wb') as f:
                for data in datas:
                    if data[0] < current_filesize:
                        continue
                    for i in range(max_retry):
                        try:
                            file_response = proxyhandler.get_filepart(download_target, data[0], data[1] - 1)
                            if file_response and file_response.status_code in (200, 206):
                                break
                            else:
                                console.print(f"[yellow]Error: {post_id}, {file_response.status_code if file_response else None}[/yellow]")
                        except Exception as e:
                            if isinstance(e, KeyboardInterrupt):
                                raise e
                            console.print(f"[yellow]Exception: {e} when downloading {post_id} {data[0]}-{data[1]}, retrying {i}/{max_retry}[/yellow]")
                    if file_response is None or file_response.status_code != 200:
                        console.print(f"[red]Error: {post_id}, {file_response.status_code}[/red]")
                        return
                    # check file size
                    if int(file_response.headers.get('Content-Length')) != data[1] - data[0]:
                        console.print(f"[red]Error: {post_id} had different file size when downloading {data[0]}-{data[1]}, expected {data[1] - data[0]}, got {file_response.headers.get('Content-Length')}[/red]")
                        return
                    f.write(file_response.content)
            # compare file size
            if os.path.getsize(save_path) != filesize:
                console.print(f"[red]Error: {post_id} had different file size after downloading, expected {filesize}, got {os.path.getsize(save_path)}[/red]")
                os.remove(save_path)
                return
        # Successfully downloaded
        logging.info(f"Successfully downloaded {post_id}")
    except Exception as e:
        console.print(f"[red]Exception: {e} when downloading {post_id}[/red]")
        logging.error(f"Exception: {e} when downloading {post_id}")
        return

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Download posts from Danbooru JSONL files")
    parser.add_argument('--start_id', type=int, help='The start id', default=0)
    parser.add_argument('--end_id', type=int, help='The end id', default=9510199)
    parser.add_argument('--file_dir', type=str, help='The file directory containing JSONL files', required=True)
    parser.add_argument('--save_location', type=str, help='The save location', required=True)
    parser.add_argument('--proxy_list_file', type=str, help='The proxy list file', default='ips')
    parser.add_argument('--proxy_auth', type=str, help='The proxy auth', default='user:password_notdefault')
    parser.add_argument('--port', type=int, help='Proxy port', default=80)
    parser.add_argument('--no_split', action='store_true', help='Try downloading file at single chunk, unsafe')
    parser.add_argument('--split_size', type=int, help='The split size', default=1000000)  # about 1MB
    parser.add_argument('--max_retry', type=int, help='The max retry', default=10)
    parser.add_argument('--only_if_original', action='store_true', help='Only download if original')
    args = parser.parse_args()

    proxy_list_file = args.proxy_list_file
    save_dir = args.save_location
    last_id = args.end_id
    start_id = args.start_id
    only_if_original = args.only_if_original

    # Initialize proxy handler
    handler = ProxyHandler(
        proxy_list_file,
        port=args.port,
        wait_time=0.1,
        timeout=20,
        proxy_auth=args.proxy_auth,
    )
    handler.check()
    console.print(f"[green]✓ Loaded {len(handler)} proxies[/green]")

    # Validate paths
    if not os.path.exists(args.file_dir):
        console.print(f"[red]Error: {args.file_dir} does not exist[/red]")
        exit(1)
    if not os.path.exists(proxy_list_file):
        console.print(f"[red]Error: {proxy_list_file} does not exist[/red]")
        exit(1)

    # Display configuration
    config_table = Table(title="Download Configuration", show_header=False)
    config_table.add_column("Key", style="cyan")
    config_table.add_column("Value", style="white")
    config_table.add_row("ID Range", f"{start_id} → {last_id}")
    config_table.add_row("File Dir", args.file_dir)
    config_table.add_row("Save Location", save_dir)
    config_table.add_row("Proxies", str(len(handler)))
    config_table.add_row("Split Size", f"{args.split_size // 1000} KB")
    console.print(config_table)

    # First, collect all posts to get accurate count
    console.print("[cyan]Collecting posts from JSONL files...[/cyan]")
    all_posts = []
    for post_line in yield_posts(from_id=start_id, last_id=last_id, file_dir=args.file_dir):
        try:
            post = json.loads(post_line)
            all_posts.append(post)
        except Exception as e:
            if isinstance(e, KeyboardInterrupt):
                raise e
            console.print(f"[yellow]Error parsing JSON: {post_line[:50]}...[/yellow]")
            continue
    
    console.print(f"[green]✓ Found {len(all_posts)} posts to process[/green]")
    
    futures = []
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task_id = progress.add_task("Downloading posts", total=len(all_posts))
        
        with ThreadPoolExecutor(max_workers=len(handler) * 3) as executor:
            for post in all_posts:
                futures.append(executor.submit(
                    download_post,
                    post,
                    handler,
                    no_split=args.no_split,
                    save_location=save_dir,
                    split_size=args.split_size,
                    max_retry=args.max_retry,
                    only_if_original=only_if_original,
                ))
            
            completed = 0
            total = len(futures)
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    if isinstance(e, KeyboardInterrupt):
                        raise e
                    console.print(f"[red]Exception: {e}[/red]")
                finally:
                    completed += 1
                    progress.advance(task_id)
                    # Periodic log for external loggers
                    if completed % 100 == 0 or completed == total:
                        console.print(f"[dim]Progress: {completed}/{total} downloads completed[/dim]")
    
    console.print("[green]✓ Download complete![/green]")
