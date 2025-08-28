
import os
import json
from typing import List
import html
import logging
import datetime

from tqdm import tqdm
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor, as_completed

from utils.proxyhandler import ProxyHandler

LOG_FILE = "gelbooru.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s %(message)s')

tag_handler = None
MAX_FILE_SIZE = 30000000 # 30MB
def yield_posts(file_dir, from_id=0, end_id=7110548):
    """
    Yields the posts
    """
    files = []
    for root, dirs, filenames in os.walk(file_dir):
        for filename in filenames:
            if "_" not in filename:
                continue
            # 0_19.jsonl -> 0, 19
            start_id, end_id = filename.split(".")[0].split("_")
            start_id = int(start_id)
            end_id = int(end_id)
            if start_id > end_id:
                continue
            if end_id < from_id:
                continue
            files.append(os.path.join(root, filename))
    print(f"Total {len(files)} files")
    for file in files:
        with open(file, 'r', encoding='utf-8') as f:
            yield from f.readlines()

class GelbooruTag:
    """
    Tag dictionary
    """
    TAG_TYPE = {
        0: "general",
        1: "artist",
        3: "copyright",
        4: "character",
        5: "meta",
        6: "deprecated"
    }
    def __init__(self, file_name="gelbooru_tags.jsonl", handler:ProxyHandler=None):
        self.file_name = file_name
        self.tags = {}
        self.type_by_name = {}
        self.handler = handler
        self.load()
    def load(self):
        """
        Loads the tags
        """
        if not os.path.exists(self.file_name):
            return
        with open(self.file_name, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    tag = json.loads(line)
                except Exception as exce:
                    if isinstance(exce, KeyboardInterrupt):
                        raise exce
                    continue
                self.tags[tag['name']] = tag
                self.type_by_name[tag['name']] = tag['type']
                # add html escaped version
                escaped_tag_name = html.escape(tag['name']).replace("&#039;", "'")
                self.tags[escaped_tag_name] = tag
                self.type_by_name[escaped_tag_name] = tag['type']
    def save(self):
        """
        Saves the tags
        """
        with open(self.file_name, 'w', encoding='utf-8') as f:
            for tag in self.tags.values():
                f.write(json.dumps(tag) + "\n")
    def save_tag(self, tag):
        """
        Saves the tag
        """
        with open(self.file_name, 'a', encoding='utf-8') as f:
            f.write(json.dumps(tag) + "\n")
    def get_missing_tags(self, tags_string):
        """
        Returns the missing tags (not locally stored)
        """
        tags_string_list = tags_string.split(" ")
        tags = []
        for tag in tags_string_list:
            if self.get_tag(tag):
                continue
            tags.append(tag)
        return tags
    def reorganize(self):
        # writes down the tags into a new file
        with open(self.file_name + "_new", 'w', encoding='utf-8') as f:
            for tag_values in self.tags.values():
                f.write(json.dumps(tag_values) + "\n")
    def get_tag(self, tag_name):
        """
        Returns the tag
        """
        # if startswith backslash, remove it
        if tag_name.startswith("\\"):
            tag_name = tag_name[1:]
        #print(tag_name) #ninomae_ina'nis  -> ninomae_ina&#039;nis
        basic_escape = tag_name.replace("'", "&#039;")
        tag_name_urlsafe = html.unescape(tag_name).replace("'", "&#039;")
        lower_tag_name = tag_name.lower()
        upper_tag_name = tag_name.upper()
        #print(tag_name_urlsafe)
        if tag_name not in self.tags and tag_name_urlsafe not in self.tags and basic_escape not in self.tags and lower_tag_name not in self.tags and upper_tag_name not in self.tags:
            return None
        if basic_escape in self.tags:
            return self.tags[basic_escape]
        if lower_tag_name in self.tags:
            return self.tags[lower_tag_name]
        if upper_tag_name in self.tags:
            return self.tags[upper_tag_name]
        return self.tags[tag_name] if tag_name in self.tags else self.tags[tag_name_urlsafe]
    def _check_handler(self, handler:ProxyHandler):
        """
        Checks the handler
        """
        if handler is None:
            handler = self.handler
        if handler is None:
            logging.error("Error: Tag was not in dictionary, but cannot get tag because handler is None")
            raise Exception("Error: Tag was not in dictionary, but cannot get tag because handler is None")
        return handler
    def get_types(self, tags_string, handler:ProxyHandler=None, max_retry=10, verbose=False):
        """
        Returns the types
        """
        self.parse_tags(tags_string, handler, max_retry=max_retry)
        types = []
        for tag in tags_string.split(" "):
            # search self.type_by_name first
            if tag in self.type_by_name:
                types.append(self.type_by_name[tag])
                continue
            else:
                # first, search dictionary
                if (tag_result:=self.get_tag(tag)) is not None:
                    types.append(tag_result['type'])
                    # add to self.type_by_name
                    self.type_by_name[tag] = tag_result['type']
                    continue
                logging.error(f"Error: {tag} not found from dictionary")
                raise Exception(f"Error: {tag} not found from type_by_name")
        if not verbose:
            return types
        return [GelbooruTag.TAG_TYPE[t] for t in types]
    def structured_tags(self, tags_string, handler:ProxyHandler=None, max_retry=10):
        """
        Returns the tags and classes as a dictionary
        """
        tags_each = tags_string.split(" ")
        tag_types = self.get_types(tags_string, handler, max_retry=max_retry,verbose=True)
        tag_dict = {}
        for tag, tag_type in zip(tags_each, tag_types):
            if tag_type not in tag_dict:
                tag_dict[tag_type] = []
            tag_dict[tag_type].append(tag)
        return tag_dict
    def parse_tags(self, tags_string, handler:ProxyHandler=None, max_retry=10):
        """
        Returns the tags and classes
        """
        tags_string_list = tags_string.split(" ")
        tags = []
        # prepare _get_tags
        tag_query_prepared = []
        # split into 100 tags per request
        for i in range(0, len(tags_string_list), 100):
            tag_query_prepared.append(tags_string_list[i:i+100])
        # get tags
        for tag_query in tag_query_prepared:
            self._get_tags(tag_query, handler, max_retry=max_retry)
        # get tags
        for tag in tags_string_list:
            if (tag_result:=self.get_tag(tag)) is None:
                print(f"Error: {tag} not found")
                continue
            tags.append(tag_result)
        return tags
    def _get_tags(self, tag_names:List[str], handler:ProxyHandler=None, max_retry=10):
        """
        Returns the tag. The tag_names should not exceed 100 tags
        This may require internet connection.
        """
        if not tag_names:
            return
        missing_tags = []
        for tag_name in tag_names:
            if self.get_tag(tag_name):
                continue
            missing_tags.append(tag_name)
        if not missing_tags:
            return
        tag_name = " ".join(missing_tags)
        # unescape html
        tag_name = html.unescape(tag_name).replace("&#039;", "'")
        # url encode
        tag_name = quote(tag_name, safe='')
        self._check_handler(handler)
        for i in range(max_retry):
            try:
                response = handler.get_response(f"https://gelbooru.com/index.php?page=dapi&s=tag&q=index&json=1&names={tag_name}")
                if response is None:
                    continue
                tag = json.loads(response) if isinstance(response, str) else response
                if not tag:
                    print(f"Error: {tag_name} not found from response {response}")
                    continue
                if "tag" not in tag:
                    logging.error(f"Error: {tag_name} not found from response {response}")
                    print(f"Error: {tag_name} not found from response {response}")
                    continue
                # {"@attributes":{"limit":100,"offset":0,"count":4},"tag":[{"id":152532,"name":"1girl","count":6177827,"type":0,"ambiguous":0},{"id":138893,"name":"1boy","count":1481404,"type":0,"ambiguous":0},{"id":444,"name":"apron","count":174832,"type":0,"ambiguous":0},{"id":135309,"name":"blunt_bangs","count":233912,"type":0,"ambiguous":0}]}
                for tag in tag['tag']:
                    self.tags[tag['name']] = tag
                    self.type_by_name[tag['name']] = tag['type']
                    # add html escaped version
                    escaped_tag_name = html.escape(tag['name']).replace("&#039;", "'")
                    self.tags[escaped_tag_name] = tag
                    self.type_by_name[escaped_tag_name] = tag['type']
                    # lower case
                    lower_tag_name = tag['name'].lower()
                    self.tags[lower_tag_name] = tag
                    self.type_by_name[lower_tag_name] = tag['type']
                    self.save_tag(tag)
                return
            except Exception as e:
                logging.exception(f"Exception: {e} when getting tag {tag_name}, retrying {i}/{max_retry}")
                print(f"Exception: {e} when getting tag {tag_name}, retrying {i}/{max_retry}")
                pass
        print(f"Error: {tag_name} not found after {max_retry} retries")

def test_gelbooru_tag(handler):
    # test tag with 1boy 1girl apron blunt_bangs
    example_post = {"id": 9199506, "created_at": "Sun Nov 05 11:30:58 -0600 2023", "score": 23, "width": 2153, "height": 3303, "md5": "5baa221d4d53e229f44dbdeac5a09c2c", "directory": "5b/aa", "image": "5baa221d4d53e229f44dbdeac5a09c2c.jpg", "rating": "sensitive", "source": "https://twitter.com/kimi_tsuru/status/1721126761885532441", "change": 1699205459, "owner": "danbooru", "creator_id": 6498, "parent_id": 0, "sample": 1, "preview_height": 250, "preview_width": 162, "tags": "1girl absurdres azur_lane bikini blue_hair blush breasts cleavage cowboy_shot dido_(azur_lane) highres holding holding_tray huge_breasts kimi_tsuru long_hair purple_eyes simple_background solo swimsuit tray white_background white_bikini", "title": "", "has_notes": "false", "has_comments": "false", "file_url": "https://img3.gelbooru.com/images/5b/aa/5baa221d4d53e229f44dbdeac5a09c2c.jpg", "preview_url": "https://img3.gelbooru.com/thumbnails/5b/aa/thumbnail_5baa221d4d53e229f44dbdeac5a09c2c.jpg", "sample_url": "https://img3.gelbooru.com/samples/5b/aa/sample_5baa221d4d53e229f44dbdeac5a09c2c.jpg", "sample_height": 1304, "sample_width": 850, "status": "active", "post_locked": 0, "has_children": "false"}
    download_post(example_post, handler, no_split=True, save_location="G:/gelbooru2023/", max_retry=10, save_metadata=True, as_json=True)

censored_tags = []
if os.path.exists("censored_tags.txt"): # load censored tags
    with open("censored_tags.txt", 'r', encoding='utf-8') as f:
        for line in f:
            censored_tags.append(line.strip())

class GelbooruMetadata:
    def __init__(self, **kwargs) -> None:
        self.id = kwargs.get("id")
        # convert to YYYY-MM-DD HH:MM:SS format
        self.created_at = datetime.datetime.strptime(kwargs.get("created_at"), "%a %b %d %H:%M:%S %z %Y").strftime("%Y-%m-%d %H:%M:%S")
        self.score = kwargs.get("score")
        self.width = kwargs.get("width")
        self.height = kwargs.get("height")
        self.md5 = kwargs.get("md5")
        self.image_ext = kwargs.get("image").split(".")[-1]
        self.rating = kwargs.get("rating")
        self.source = kwargs.get("source", "")
        self.tags = kwargs.get("tags")
        self.title = kwargs.get("title", "")
        self.file_url = kwargs.get("file_url")
        self.has_children = kwargs.get("has_children", False)
        self.parent_id = kwargs.get("parent_id", 0)
    def get_dict(self):
        return dict(
            id=self.id,
            created_at=self.created_at,
            score=self.score,
            width=self.width,
            height=self.height,
            md5=self.md5,
            image_ext=self.image_ext,
            rating=self.rating,
            source=self.source,
            tags=self.tags,
            title=self.title,
            file_url=self.file_url,
            has_children=self.has_children,
            parent_id=self.parent_id,
        )
    def structured_dict(self, tag_handler:GelbooruTag, handler:ProxyHandler=None, max_retry=10):
        """
        Returns the structured dictionary
        """
        tags = tag_handler.structured_tags(self.tags, handler, max_retry=max_retry)
        return dict(
            id=self.id,
            created_at=self.created_at,
            score=self.score,
            width=self.width,
            height=self.height,
            md5=self.md5,
            image_ext=self.image_ext,
            rating=self.rating,
            source=self.source,
            title=self.title,
            file_url=self.file_url,
            has_children=self.has_children,
            parent_id=self.parent_id,
            tag_list_general=tags.get("general", []),
            tag_list_artist=tags.get("artist", []),
            tag_list_character=tags.get("character", []),
            tag_list_meta=tags.get("meta", []),
            tag_list_copyright=tags.get("copyright", []),
        )

def extract_and_parse_tags(post_dicts:List[str], tag_handler:GelbooruTag, proxyhandler:ProxyHandler, max_retry=10, batch_size=100, total_size=0):
    tagset = set()
    # collect batch
    batch = []
    pbar = tqdm(total=total_size)
    def wrap_parse_tag_for_pbar(tag):
        tag_handler.parse_tags(tag, proxyhandler, max_retry=max_retry)
        pbar.update(1)
        return 
    futures = []
    with ThreadPoolExecutor(max_workers=10) as executor:
        while (post_dict:=next(post_dicts, None)) is not None:
            if isinstance(post_dict, str):
                post_dict = json.loads(post_dict)
            if not any(tag in post_dict['tags'] for tag in censored_tags):
                continue
            tagset.update(tag_handler.get_missing_tags(post_dict['tags']))
            if len(tagset) >= batch_size:
                # search for tags
                print(f"Getting {len(tagset)} tags")
                #tag_handler.parse_tags(" ".join(tagset), proxyhandler, max_retry=max_retry)
                futures.append(executor.submit(wrap_parse_tag_for_pbar, " ".join(tagset)))
                batch.clear()
                tagset.clear()
    # wait for futures
    for future in as_completed(futures):
        try:
            future.result()
        except Exception as e:
            print(f"Exception: {e}")
    # search for tags
    print(f"Getting {len(tagset)} tags")
    tag_handler.parse_tags(" ".join(tagset), proxyhandler, max_retry=max_retry)
    batch.clear()
    tagset.clear()
    return

def download_meta(post_dict, proxyhandler:ProxyHandler, pbar=None, no_split=False, save_location="G:/gelbooru2023/", split_size=1000000, max_retry=10, as_json=False, save_metadata=False):
    # check if image exists
    # image_ext = post_dict['file_ext'] if 'file_ext' in post_dict else post_dict["image"].split(".")[-1]
    # post_path = save_location +f"{post_dict['id'] % 100}/"+ f"{post_dict['id']}.{image_ext}"
    # if not os.path.exists(post_path):
    #     logging.error(f"Error: {post_dict['id']} has no image")
    #     if pbar is not None:
    #         pbar.update(1)
    #     return
    post_id = post_dict['id']
    save_path = save_location +f"{post_id % 100}/"+ f"{post_id}.json" if as_json else save_location +f"{post_id % 100}/"+ f"{post_id}.txt"
    if not os.path.exists(save_location +f"{post_id % 100}/"):
        os.makedirs(save_location +f"{post_id % 100}/")
    if os.path.exists(save_path) and os.path.getsize(save_path) != 0:
        logging.info(f"Skipped {post_id} because metadata exists in {save_path}")
        pbar.update(1)
        return
    parsed_dict = GelbooruMetadata(**post_dict).structured_dict(tag_handler, proxyhandler, max_retry=max_retry)
    if not os.path.exists(save_path):
        with open(save_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(parsed_dict) if as_json else str(parsed_dict))
    pbar.update(1)
    return

def download_post(post_dict, proxyhandler:ProxyHandler, pbar=None, no_split=False, save_location="G:/gelbooru2023/", split_size=1000000, max_retry=10, save_metadata=False, as_json=False):
    post_id = post_dict['id']
    banned_tags = ["animated", "video", "3d", "photo_(medium)", "real_life"]
    if any(tag in post_dict['tags'] for tag in banned_tags):
        if pbar is not None:
            pbar.update(1)
        return # skip animated
    # we will skip danbooru-uploaded posts if not censored
    contains_censored_tag = any(tag in post_dict['tags'].split(" ") for tag in censored_tags)
    special_condition = contains_censored_tag
    if (not contains_censored_tag and (post_dict["owner"] == "danbooru" or post_dict.get("creator_id", 0) == 6498)): #backing danbooru posts, skip if not censored
        if pbar is not None:
            pbar.update(1)
        return
    ext = post_dict['file_ext'] if 'file_ext' in post_dict else post_dict["image"].split(".")[-1]
    download_target = post_dict.get("large_file_url", post_dict.get("file_url"))
    save_path = save_location +f"{post_id % 100}/"+ f"{post_id}.{ext}"
    if not os.path.exists(save_location +f"{post_id % 100}/"):
        os.makedirs(save_location +f"{post_id % 100}/")
    # if url contains file extension, use that
    if download_target and "." in download_target:
        ext = download_target.split(".")[-1]
    # skip video files
    if ext in ["webm", "mp4", "mov", "avi"]:
        pbar.update(1)
        return
    if not download_target:
        #print(f"Error: {post_id} has no download target, dict: {post_dict}") # gold account?
        pbar.update(1)
        return
    if save_metadata:
        save_meta_path = save_location +f"{post_id % 100}/"+ f"{post_id}.json" if as_json else save_location +f"{post_id % 100}/"+ f"{post_id}.txt"
        # if tag_list_general is not in post_dict, it is gelbooru post
        if "tag_list_general" not in post_dict:
            post_dict = GelbooruMetadata(**post_dict).structured_dict(tag_handler, proxyhandler, max_retry=max_retry)
        else:
            raise NotImplementedError("Not implemented for danbooru")
        if not os.path.exists(save_meta_path):
            with open(save_meta_path, 'w', encoding='utf-8') as f:
                f.write(json.dumps(post_dict) if as_json else str(post_dict))
    if os.path.exists(save_path) and os.path.getsize(save_path) != 0:
        #logging.info(f"Skipped {post_id}")
        pbar.update(1)
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
        pbar.update(1)
        return
    if filesize > MAX_FILE_SIZE and not special_condition:
        print(f"Error: {post_id} has filesize {filesize}, skipping")
        pbar.update(1)
        return
    if os.path.exists(save_path):
        # check file size
        if os.path.getsize(save_path) != filesize:
            print(f"Error: {post_id} had different file size saved, expected {filesize}, got {os.path.getsize(save_path)} when downloading")
            os.remove(save_path)
        else:
            if pbar is not None:
                pbar.update(1)
            return
    if no_split:
        file_response = None
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
                pass
        if not file_response or file_response.status_code != 200:
            status = file_response.status_code if file_response else None
            print(f"Error: {post_id}, {status}")
            if pbar is not None:
                pbar.update(1)
            return
        filesize = file_response.headers.get('Content-Length')
        content = file_response.content
        # compare file size
        if int(filesize) != len(content):
            print(f"Error: {post_id} had different file size when downloading (no split), expected {filesize}, got {len(content)}")
            if pbar is not None:
                pbar.update(1)
            return
            # save file
        with open(save_path, 'wb') as f:
            f.write(content)
    else:
        datas = [] # max 1MB per request
        if filesize is None:
            print(f"Error: {post_id} has no filesize")
            if pbar is not None:
                pbar.update(1)
            return
        for i in range(0, filesize, split_size):
            datas.append((i, min(filesize, i + split_size)))
        # download
        current_filesize = os.path.getsize(save_path) if os.path.exists(save_path) else 0
        if current_filesize:
            print(f"Resuming {post_id} from {current_filesize}, to {filesize}")
    
        for _i, data in enumerate(datas):
            if data[0] < current_filesize:
                continue
            if os.path.exists(save_path + f".{_i}"):
                if os.path.getsize(save_path + f".{_i}") == data[1] - data[0]:
                    continue
                else:
                    print(f"Error: {post_id} had different file size when downloading {data[0]}-{data[1]}, expected {data[1] - data[0]}, got {os.path.getsize(save_path + f'.{_i}')} when downloading")
                    os.remove(save_path + f".{_i}")
            file_response = None
            for i in range(max_retry):
                try:
                    file_response = proxyhandler.get_filepart(download_target, data[0], data[1] - 1)
                    if file_response and file_response.status_code == 200:
                        break
                    if not file_response:
                        print(f"Error: {post_id}, {file_response}")
                        continue
                    if int(file_response.headers.get('Content-Length')) != data[1] - data[0]:
                        print(f"Error: {post_id} had different file size when downloading {data[0]}-{data[1]}, expected {data[1] - data[0]}, got {file_response.headers.get('Content-Length')}, retrying {i}/{max_retry}")
                        continue
                    else:
                        print(f"Error: {post_id}, {file_response.status_code if file_response else None}")
                except Exception as e:
                    if isinstance(e, KeyboardInterrupt):
                        raise e
                    print(f"Exception: {e} when downloading {post_id} {data[0]}-{data[1]}, retrying {i}/{max_retry}")
            if not file_response or file_response.status_code != 200:
                status = file_response.status_code if file_response else None
                print(f"Error: {post_id}, {status}")
                if pbar is not None:
                    pbar.update(1)
                return
            # check file size
            if int(file_response.headers.get('Content-Length')) != data[1] - data[0]:
                print(f"Error: {post_id} had different file size when downloading {data[0]}-{data[1]}, expected {data[1] - data[0]}, got {file_response.headers.get('Content-Length')}")
                if pbar is not None:
                    pbar.update(1)
                return
            with open(save_path + f".{_i}", 'wb') as partfile:
                partfile.write(file_response.content)
            if os.path.getsize(save_path + f".{_i}") != data[1] - data[0]:
                print(f"Error: {post_id} had different file size when downloading {data[0]}-{data[1]}, expected {data[1] - data[0]}, got {os.path.getsize(save_path + f'.{_i}')}")
                os.remove(save_path + f".{_i}")
            # merge files
        with open(save_path, 'wb') as f:
            for _i in range(len(datas)):
                with open(save_path + f".{_i}", 'rb') as partfile:
                    f.write(partfile.read())
        # compare file size
        if os.path.getsize(save_path) != filesize:
            print(f"Error: {post_id} had different file size after downloading, expected {filesize}, got {os.path.getsize(save_path)}")
            os.remove(save_path)
            if pbar is not None:
                pbar.update(1)
            return
        else:
            # remove part files
            for _i in range(len(datas)):
                os.remove(save_path + f".{_i}")
    if pbar is not None:
        pbar.update(1)

def test_gelbooru_tag_search(handler):
    """
    Tests the tag search
    """
    tag = GelbooruTag()
    # test tags with url-unfriendly characters
    print(tag.structured_tags("head_on_another's_shoulder takagi-san type_95_(girls'_frontline) m.m", max_retry=10,
                              handler=handler))

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Downloads the posts from gelbooru')
    parser.add_argument('--start_id', type=int, help='The start id', default=0)
    parser.add_argument('--end_id', type=int, help='The end id', default=9794205)
    parser.add_argument('--file_dir', type=str, help='The file directory', default=r"D:\danbooru\post_gelbooru")
    parser.add_argument('--save_location', type=str, help='The save location', default=r"D:\danbooru\gelbooru-results")
    parser.add_argument('--proxy_list_file', type=str, help='The proxy list file', default=r"C:\projects\Booru-Crawling\ips.txt")
    parser.add_argument('--proxy_auth', type=str, help='The proxy auth', default="user:password_notdefault")
    parser.add_argument('--no_split', action='store_true', help='Try downloading file at single chunk, unsafe')
    parser.add_argument('--split_size', type=int, help='The split size', default=1000000) # about 1MB
    parser.add_argument('--max_retry', type=int, help='The max retry', default=10)
    parser.add_argument('--as_json', action='store_true', help='Save metadata as json')
    parser.add_argument('--save_metadata', action='store_true', help='Save metadata')
    args = parser.parse_args()
    tag_handler = GelbooruTag()
    tag_handler.reorganize()
    MAX_FILE_SIZE = args.split_size
    proxy_list_file = args.proxy_list_file
    save_location = args.save_location
    proxyhandler = ProxyHandler(proxy_list_file, wait_time=1.1, timeouts=20,proxy_auth=args.proxy_auth) # 4 requests per second, 20 proxy = 100 requests per second, 1MB per request = 100MB per second
    proxyhandler.check()
    test_gelbooru_tag_search(proxyhandler)
    # test
    futures = []
    file_dir = args.file_dir
    from_id = args.start_id
    end_id = args.end_id
    iterator = yield_posts(from_id=from_id, end_id=end_id, file_dir=file_dir)
    #extract_and_parse_tags(iterator, tag_handler, proxyhandler, max_retry=10, batch_size=100, total_size=9510199 - 8e6)
    with ThreadPoolExecutor(max_workers=80) as executor:
        pbar = tqdm(total=end_id - from_id)
        for post in yield_posts(from_id=from_id, end_id=end_id, file_dir=file_dir):
            try:
                post = json.loads(post)
            except:
                print(f"Error: {post}")
                continue
            #download_meta(post, proxyhandler, pbar=pbar, no_split=False, save_location=save_location,split_size=1000000, save_metadata=True, as_json=True)
            #download_post(post, proxyhandler, pbar=pbar, no_split=False, save_location=save_location,split_size=1000000)
            #futures += [executor.submit(download_post, post, proxyhandler, pbar=pbar, no_split=args.no_split, save_location=save_location,split_size=args.split_size, save_metadata=args.save_metadata, as_json=args.as_json)]
            futures += [executor.submit(download_meta, post, proxyhandler, pbar=pbar, no_split=False, save_location=save_location,split_size=1000000, save_metadata=True, as_json=True)] # this is for metadata
        for future in futures:
            try:
                future.result()
            except Exception as e:
                if isinstance(e, KeyboardInterrupt):
                    raise e
                print(f"Exception: {e}")
