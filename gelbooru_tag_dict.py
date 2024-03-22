from concurrent.futures import ThreadPoolExecutor
from utils.gelboorutags import GelbooruTag
from utils.proxyhandler import ProxyHandler
import os
import jsonlines
from tqdm import tqdm
import json
handler = ProxyHandler(r"C:\projects\Booru-Crawling\ips.txt", wait_time=1.1, timeouts=20,proxy_auth="user:password_notdefault")
handler.check()
tag_handler = GelbooruTag(handler=handler, exception_handle=0)
tag_handler.reorganize_and_reload()

# if file exists, skip
if os.path.exists(r"D:\danbooru\tagset.json"):
    print("Tag dict exists, skipping")
    # load instead
    with open(r"D:\danbooru\tagset.json", "r") as f:
        merged_bulk_string_set = json.load(f)
else:
    post_dir = r"D:\danbooru\post_gelbooru"
    merged_bulk_string_set = set()
    subdirs = os.listdir(post_dir)
    for subdir in subdirs:
        subdir = os.path.join(post_dir, subdir)
        if not os.path.isdir(subdir):
            continue
        post_files = os.listdir(subdir)
        for post_file in tqdm(post_files):
            post_file = os.path.join(subdir, post_file)
            if not post_file.endswith(".jsonl"):
                continue
            with jsonlines.open(post_file) as reader:
                for post in reader:
                    tags = post.get("tags")
                    if tags is None:
                        print(f"Error: {post['id']} has no tags")
                        continue
                    merged_bulk_string_set |= set(tags.split(" "))
        print(f"Total tags: {len(merged_bulk_string_set)}")
    with open(r"D:\danbooru\tagset.json", "w") as f:
        json.dump(list(merged_bulk_string_set), f)
print(f"Total tags: {len(merged_bulk_string_set)}")
merged_bulk_string_set = list(set(merged_bulk_string_set))
merged_bulk_string_list = []
for tags in tqdm(merged_bulk_string_set):
    if not tag_handler.tag_exists(tags):
        merged_bulk_string_list.append(tags)
print(f"Total tags: {len(merged_bulk_string_list)}")
merged_bulk_string_set = merged_bulk_string_list

# batch 20000 tags
batch_size = 99
pbar = tqdm(total=len(merged_bulk_string_set) // batch_size + 1, desc="Tags batch")

def get_types_and_update_pbar(selected_tags):
    bulk_string = " ".join(selected_tags)
    tag_handler.get_types(bulk_string, handler, max_retry=3)
    pbar.update(1)
    print(handler.get_average_time())

futures = []
with ThreadPoolExecutor(max_workers=15) as executor:
    for i in range(0, len(merged_bulk_string_set), batch_size):
        selected_tags = merged_bulk_string_set[i:i+batch_size]
        futures.append(executor.submit(get_types_and_update_pbar, selected_tags))
    for future in futures:
        future.result()
pbar.close()
