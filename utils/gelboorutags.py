import os
from .proxyhandler import ProxyHandler
from typing import List
import json
import html
import logging
import datetime
from urllib.parse import quote

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
    def __init__(self, file_name="gelbooru_tags.jsonl", handler:ProxyHandler=None, exception_handle=None):
        """
        exception_handle -> tag type that will be used if tag is not found
        """
        self.file_name = file_name
        self.tags = {}
        self.type_by_name = {}
        self.handler = handler
        self.exception_handle = exception_handle # if tag not found, what to do
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
        Returns the types of given tags
        This can be used for bulk processing
        Use threaded version for faster processing (if you have proxy handler)
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
                if self.exception_handle is not None:
                    self.type_by_name[tag] = self.exception_handle
                    types.append(self.exception_handle)
                else:
                    raise Exception(f"Error: {tag} not found from type_by_name")
        if not verbose:
            return types
        return [GelbooruTag.TAG_TYPE[t] for t in types]
    def structured_tags(self, tags_string, handler:ProxyHandler=None, max_retry=10):
        """
        Returns the tags and classes as a dictionary
        This can be used for any string input (maybe merged too) for bulk processing
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
            avg_response_time = handler.get_average_time()
            if avg_response_time:
                print(f"Average response time: {avg_response_time}")
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
