import os
import json
import logging
from tqdm import tqdm
from utils.proxyhandler import ProxyHandler
from concurrent.futures import ThreadPoolExecutor, as_completed

LOG_FILE = "download_post.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO)

def yield_posts(file_dir:str, from_id=0, last_id=7110548):
    """
    Yields the posts
    """
    # using listdir instead of glob because glob is slow
    files = []
    # walk through all files
    for root, _, filenames in os.walk(file_dir):
        for filename in filenames:
            if "_" not in filename:
                continue
            # 0_19.jsonl -> 0, 19
            starting_id, last_id = filename.split(".")[0].split("_")
            starting_id = int(starting_id)
            last_id = int(last_id)
            if starting_id > last_id:
                continue
            if last_id < from_id:
                continue
            files.append(os.path.join(root, filename))
    print(f"Total {len(files)} files")
    for file in files:
        with open(file, 'r', encoding='utf-8') as f:
            yield from f.readlines()

def download_post(post_dict, proxyhandler:ProxyHandler, pbar=None, no_split=False, save_location="G:/danbooru2023-c/", split_size=1000000, max_retry=10):
    """
    Downloads the post
    """
    try:
        post_id = post_dict['id']
        ext = post_dict['file_ext']
        download_target = post_dict.get("large_file_url", post_dict.get("file_url"))
        save_path = save_location +f"{post_id % 100} /"+ f"{post_id}.{ext}"
        if not os.path.exists(save_location +f"{post_id % 100} /"):
            os.makedirs(save_location +f"{post_id % 100} /")
        # if url contains file extension, use that
        if download_target and "." in download_target:
            ext = download_target.split(".")[-1]
        # skip video files
        if ext in ["webm", "mp4", "mov", "avi"]:
            logging.info(f"Skipping {post_id} because it's a video")
            return
        if not download_target:
            logging.info(f"Error: {post_id} has no download target, dict: {post_dict}") # gold account?
            #print(f"Error: {post_id} has no download target, dict: {post_dict}") # gold account?
            return
        for i in range(max_retry):
            try:
                filesize = proxyhandler.filesize(download_target)
            except Exception as e:
                if isinstance(e, KeyboardInterrupt):
                    raise e
                print(f"Exception: {e} when getting filesize of {post_id}, retrying {i}/{max_retry}")
                filesize = None
            if filesize is not None:
                break
        if filesize is None:
            print(f"Error: {post_id} has no filesize after {max_retry} retries")
            return

        if os.path.exists(save_path):
            # check file size
            if os.path.getsize(save_path) != filesize:
                print(f"Error: {post_id} had different file size saved, expected {filesize}, got {os.path.getsize(save_path)}")
                os.remove(save_path)
            else:
                if pbar is not None:
                    pbar.update(1)
                return
        if no_split:
            for i in range(max_retry):
                try:
                    file_response = proxyhandler.get(download_target)
                    if file_response and file_response.status_code == 200:
                        break
                    else:
                        print(f"Error: {post_id}, {file_response}")
                except Exception as e:
                    if isinstance(e, KeyboardInterrupt):
                        raise e
                    print(f"Exception: {e} when downloading {post_id}, retrying {i}/{max_retry}")
            filesize = file_response.headers.get('Content-Length')
            content = file_response.content
            # compare file size
            if int(filesize) != len(content):
                print(f"Error: {post_id} had different file size when downloading (no split), expected {filesize}, got {len(content)}")
                return
                # save file
            with open(save_path, 'wb') as f:
                f.write(content)
        else:
            datas = [] # max 1MB per request
            if filesize is None:
                print(f"Error: {post_id} has no filesize")
                return
            for i in range(0, filesize, split_size):
                datas.append((i, min(filesize, i + split_size)))
            # download
            current_filesize = os.path.getsize(save_path) if os.path.exists(save_path) else 0
            if current_filesize:
                print(f"Resuming {post_id} from {current_filesize}, to {filesize}")
            with open(save_path, 'wb') as f:
                for data in datas:
                    if data[0] < current_filesize:
                        continue
                    for i in range(max_retry):
                        try:
                            file_response = proxyhandler.get_filepart(download_target, data[0], data[1] - 1)
                            if file_response and file_response.status_code == 200:
                                break
                            else:
                                print(f"Error: {post_id}, {file_response.status_code if file_response else None}")
                        except Exception as e:
                            if isinstance(e, KeyboardInterrupt):
                                raise e
                            print(f"Exception: {e} when downloading {post_id} {data[0]}-{data[1]}, retrying {i}/{max_retry}")
                    if file_response is None or file_response.status_code != 200:
                        print(f"Error: {post_id}, {file_response.status_code}")
                        return
                    # check file size
                    if int(file_response.headers.get('Content-Length')) != data[1] - data[0]:
                        print(f"Error: {post_id} had different file size when downloading {data[0]}-{data[1]}, expected {data[1] - data[0]}, got {file_response.headers.get('Content-Length')}")
                        return
                    f.write(file_response.content)
            # compare file size
            if os.path.getsize(save_path) != filesize:
                print(f"Error: {post_id} had different file size after downloading, expected {filesize}, got {os.path.getsize(save_path)}")
                os.remove(save_path)
                return
        if pbar is not None:
            pbar.update(1)
    except Exception as e:
        print(f"Exception: {e} when downloading {post_id}")
        logging.error(f"Exception: {e} when downloading {post_id}")
        return

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Download posts")
    parser.add_argument('--start_id', type=int, help='The start id', default=0)
    parser.add_argument('--end_id', type=int, help='The end id', default=9510199)
    parser.add_argument('--file_dir', type=str, help='The file directory', default="D:/danbooru/post_gelbooru_0_6M")
    parser.add_argument('--save_location', type=str, help='The save location', default="D:/gelbooru2023_0-6M/")
    parser.add_argument('--proxy_list_file', type=str, help='The proxy list file', default="D:/danbooru/ips.txt")
    parser.add_argument('--proxy_auth', type=str, help='The proxy auth', default="user:password_notdefault")
    parser.add_argument('--no_split', action='store_true', help='Try downloading file at single chunk, unsafe')
    parser.add_argument('--split_size', type=int, help='The split size', default=1000000) # about 1MB
    parser.add_argument('--max_retry', type=int, help='The max retry', default=10)
    args = parser.parse_args()
    proxy_list_file = args.proxy_list_file
    save_dir = args.save_location
    last_id = args.end_id
    start_id = args.start_id
    handler = ProxyHandler(proxy_list_file, wait_time=0.1, timeouts=20,proxy_auth=args.proxy_auth)
    handler.check()
    assert os.path.exists(args.file_dir), f"{args.file_dir} does not exist"
    assert os.path.exists(proxy_list_file), f"{proxy_list_file} does not exist"
    futures = []
    with ThreadPoolExecutor(max_workers=80) as executor:
        pbar_download = tqdm(total=-start_id + last_id)
        for post in yield_posts(from_id=start_id, last_id=last_id, file_dir=args.file_dir):
            try:
                post = json.loads(post)
            except Exception as e:
                if isinstance(e, KeyboardInterrupt):
                    raise e
                print(f"Error: {post}")
                continue
            futures.append(executor.submit(download_post, post, handler, pbar=pbar_download, no_split=args.no_split, save_location=save_dir,split_size=args.split_size, max_retry=args.max_retry))
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                if isinstance(e, KeyboardInterrupt):
                    raise e
                print(f"Exception: {e}")
                continue
