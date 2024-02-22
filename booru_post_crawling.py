from typing import List
import os
from functools import cache
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from utils.proxyhandler import ProxyHandler

handler = None
PER_REQUEST_POSTS = 100
post_ids = set()

@cache
def split_query(start:int, end:int, mode:str="danbooru") -> List[str]:
    """
    Returns the list of queries to be made
    """
    assert mode in ["danbooru", "gelbooru"], f"Mode {mode} not supported"
    lists = []
    skipped = 0
    end = end + PER_REQUEST_POSTS if end % PER_REQUEST_POSTS != 0 else end
    for i in tqdm(range(start, end, PER_REQUEST_POSTS)):
        if i in post_ids:
            skipped += 1
            continue
        lists.append(get_query_bulk(i) if mode == "danbooru" else get_query_bulk_gelbooru(i))
    print(f"Skipped {skipped} queries")
    return lists

def get_query_bulk(index: int) -> str:
    """
    Returns the query link that contains the index
    """
    start_idx = index - index % PER_REQUEST_POSTS
    end_idx = start_idx + PER_REQUEST_POSTS - 1
    query = rf"https://danbooru.donmai.us/posts.json?tags=id%3A{start_idx}..{end_idx}&limit={PER_REQUEST_POSTS}"
    return query

def get_query_bulk_gelbooru(index: int) -> str:
    """
    Returns the query link that contains the index
    """
    start_idx = index - index % PER_REQUEST_POSTS
    end_idx = start_idx + PER_REQUEST_POSTS - 1
    query = rf"https://gelbooru.com/index.php?page=dapi&s=post&q=index&tags=id:%3E={start_idx}+id:%3C={end_idx}+score:%3E3&json=1&limit={PER_REQUEST_POSTS}"
    return query

def get_response(url):
    """
    Returns the response of the url
    """
    try:
        response = handler.get_response(url)
        return response
    except Exception as e:
        print(f"Exception: {e}")
        return None

def write_to_file(data, post_file='posts.jsonl', total_posts=0):
    """
    Writes the data to the file
    """
    skipped = 0
    if not os.path.exists(os.path.dirname(post_file)):
        os.makedirs(os.path.dirname(post_file), exist_ok=True)
    try:
        if os.path.exists(post_file):
            return
        with open(post_file, 'w', encoding="utf-8") as f:
            if "post" in data:
                data = data['post']
            if not isinstance(data, list):
                print(f"Error: {data}")
            total_posts += len(data)
            for post in data:
                if 'id' not in post:
                    print(f"Error: {post}")
                    continue
                if post['id'] in post_ids:
                    skipped += 1
                    continue
                #assert "file_url" in post or "large_file_url" in post, f"Post has no file url: {post['id']} : post {post}" # gold account?
                f.write(json.dumps(post))
                f.write('\n')
                post_ids.add(post['id'])
    except Exception as e:
        print(f"Exception: {e} while writing to file")
def get_posts(query, post_file='posts.jsonl', pbar=None):
    """
    Gets the posts from the query
    """
    if os.path.exists(post_file):
        pbar.update(1)
        return
    response = get_response(query)
    if response is not None:
        write_to_file(response, post_file=post_file)
    else:
        print(f"Error: {query}")
    pbar.update(1)

def get_posts_threaded(queries, post_save_dir='post', mode="danbooru"):
    """
    Gets the posts from the queries
    """
    def get_post_range(query, mode="danbooru"):
        """
        Returns the range of the query
        """
        if mode == "danbooru":
            return int(query.split("id%3A")[1].split("..")[0]), int(query.split("id%3A")[1].split("..")[1].split("&")[0])
        else:
            return int(query.split(r"id:%3E=")[1].split("+")[0]), int(query.split(r"id:%3C=")[1].split("+")[0])
    def get_filename_for_query(query, mode="danbooru"):
        """
        Returns the filename for the query
        """
        start, end = get_post_range(query, mode=mode)
        # create subdir by millions
        return f"{post_save_dir}/{start // 1000000}M/{start}_{end}.jsonl"
    query_post_pbar = tqdm(total=len(queries))
    with ThreadPoolExecutor(max_workers=len(handler.proxy_list) * 5) as executor:
        futures = [executor.submit(get_posts, query, post_file=get_filename_for_query(query, mode), pbar=query_post_pbar) for query in queries]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                if isinstance(e, KeyboardInterrupt):
                    raise e
                print(f"Exception: {e}")
                continue

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Downloads the posts from danbooru/gelbooru')
    parser.add_argument('--start_id', type=int, help='The start id')
    parser.add_argument('--end_id', type=int, help='The end id')
    parser.add_argument('--post_save_dir', type=str, help='The post save directory')
    parser.add_argument('--ips', type=str, help='The ips file', default="ips.txt")
    parser.add_argument('--proxy_auth', type=str, help='The proxy auth', default="user:password_notdefault")
    parser.add_argument('--wait_time', type=float, help='The wait time', default=1)
    parser.add_argument('--timeouts', type=int, help='The timeouts', default=15)
    parser.add_argument('--port', type=int, help='The port', default=80)
    parser.add_argument('--mode', type=str, help='The mode', default="danbooru")
    args = parser.parse_args()
    handler = ProxyHandler(args.ips, port=args.port, wait_time=args.wait_time, timeouts=args.timeouts, proxy_auth=args.proxy_auth)
    handler.check()
    print(f"Proxy Handler Checked, total {len(handler.proxy_list)} proxies")
    post_dir = args.post_save_dir
    query_list = split_query(args.start_id, args.end_id, mode=args.mode)
    get_posts_threaded(query_list, post_save_dir=post_dir, mode=args.mode)
    print("Done")
