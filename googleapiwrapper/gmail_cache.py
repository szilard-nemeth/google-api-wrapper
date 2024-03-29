from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Iterable, Set, Tuple

from pythoncommons.file_utils import FileUtils, FindResultType, JsonFileUtils
from pythoncommons.string_utils import auto_str, StringUtils

from googleapiwrapper.gmail_common import GmailRequestType
from googleapiwrapper.gmail_common import (
    THREADS_DIR_NAME,
    MESSAGE_DATA_FILENAME,
    MESSAGE_ID,
    MESSAGE_DATE,
    THREAD_JSON_FILENAME,
    MESSAGES_DIR_NAME,
)
from googleapiwrapper.gmail_domain import GenericObjectHelper as GH, ThreadField, MessageField
from googleapiwrapper.utils import CommonUtils

import logging

LOG = logging.getLogger(__name__)


class ItemCacheState(Enum):
    NOT_CACHED = "not cached"
    CACHE_LEVEL_NOT_DETERMINED = "cache level not yet determined"
    FULLY_CACHED = "fully cached"
    PARTIALLY_CACHED = "partially cached"


@dataclass
class CachedItem:
    id: str
    data: Any = field(repr=False)
    cache_state: ItemCacheState


@dataclass
class CacheMeta:
    type: str
    type_plural: str

    def __post_init__(self):
        self.type_capitalized = self.type.title()


@dataclass
class CacheMetrics:
    items_written: int
    bytes_written: int
    items_read: int
    bytes_read: int

    def _combine(self, other):
        items_written = self.items_written + other.items_written
        bytes_written = self.bytes_written + other.bytes_written
        items_read = self.items_read + other.items_read
        bytes_read = self.bytes_read + other.bytes_read
        return CacheMetrics(items_written, bytes_written, items_read, bytes_read)

    @staticmethod
    def create_for_read(items_read, bytes_read):
        return CacheMetrics(0, 0, items_read, bytes_read)

    @staticmethod
    def create_for_write(items_written, bytes_written):
        return CacheMetrics(items_written, bytes_written, 0, 0)

    @staticmethod
    def create_empty():
        return CacheMetrics(0, 0, 0, 0)

    @staticmethod
    def combine(*actions):
        if not actions:
            raise ValueError("Expected at least one instance of " + CacheMetrics.__class__.__name__)
        result = actions[0]
        for i in range(1, len(actions)):
            result = result._combine(actions[i])
        return result

    def __str__(self):
        return (
            self.__class__.__name__
            + " { "
            + "items written: "
            + str(self.items_written)
            + ", bytes written (dynamic): "
            + StringUtils.format_bytes_as_str(self.bytes_written)
            + ", items read: "
            + str(self.items_read)
            + ", bytes read (dynamic): "
            + StringUtils.format_bytes_as_str(self.bytes_read)
            + " }"
        )


class CacheActionsPerformed:
    def __init__(self):
        self._latest: Dict["GmailRequestType", CacheMetrics] = defaultdict(CacheMetrics.create_empty)
        self._sum: Dict["GmailRequestType", CacheMetrics] = defaultdict(CacheMetrics.create_empty)

    def add(self, request_type: "GmailRequestType", metrics: CacheMetrics):
        self._latest[request_type] = metrics
        self._sum[request_type] = self._sum[request_type].combine(metrics)
        self.log(request_type)

    def log(self, request_type):
        LOG.trace("Added metrics for %s: %s", request_type, self._latest[request_type])
        for req_type, metrics in self._sum.items():
            LOG.trace("Sum metrics for %s: %s", req_type, metrics)

    def print_all(self):
        for req_type, metrics in self._sum.items():
            LOG.info("Sum metrics for %s: %s", req_type, metrics)


@auto_str
class CacheResultItems:
    def __init__(self, item_ids: List[str], cache_type: str, cache_type_plural: str = None):
        if not cache_type_plural:
            cache_type_plural = f"{cache_type}s"
        self._meta = CacheMeta(cache_type, cache_type_plural)
        self.item_ids = item_ids
        self.not_cached_items: Dict[str, CachedItem] = {}
        self.partially_cached_items: Dict[str, CachedItem] = {}
        self.not_yet_determined_items: Dict[str, CachedItem] = {}
        self.fully_cached_items: Dict[str, CachedItem] = {}

    def add_not_cached(self, item_ids: Iterable[str]):
        self.not_cached_items.update(self._create_list(item_ids, ItemCacheState.NOT_CACHED))
        return self

    def add_partially_cached(self, item_ids: Iterable[str]):
        self.partially_cached_items.update(self._create_list(item_ids, ItemCacheState.PARTIALLY_CACHED))
        return self

    def add_not_yet_determined(self, item_ids: Iterable[str]):
        self.not_yet_determined_items.update(self._create_list(item_ids, ItemCacheState.CACHE_LEVEL_NOT_DETERMINED))
        return self

    def add_fully_cached(self, item_ids_with_data: Dict[str, Any]):
        for item_id, data in item_ids_with_data.items():
            self.fully_cached_items[item_id] = CachedItem(item_id, data, ItemCacheState.FULLY_CACHED)
        return self

    @property
    def not_cached_ids(self):
        return self.not_cached_items.keys()

    @property
    def partially_cached_ids(self):
        return self.partially_cached_items.keys()

    @property
    def not_yet_determined_ids(self):
        return self.not_yet_determined_items.keys()

    @property
    def fully_cached_ids(self):
        return self.fully_cached_items.keys()

    @staticmethod
    def _create_list(item_ids: Iterable[str], state: ItemCacheState) -> Dict[str, CachedItem]:
        return {id: CachedItem(id, None, state) for id in item_ids}

    def are_all_fully_cached(self) -> bool:
        return sum([len(self.not_cached_ids), len(self.partially_cached_ids), len(self.not_yet_determined_ids)]) == 0

    def get_no_of_any_cached_all(self) -> int:
        """
        :return: Any cached item. This also counts all cached items not related to the specified item_ids during init.
        """
        return sum([len(self.fully_cached_ids), len(self.partially_cached_ids), len(self.not_yet_determined_ids)])

    def get_no_of_any_cached_for_items(self) -> int:
        """
        :return: Any cached item. This only counts cached items related to the specified item_ids during init.
        """
        return sum(
            [
                len(set(self.fully_cached_ids).intersection(self.item_ids)),
                len(set(self.partially_cached_items).intersection(self.item_ids)),
                len(set(self.not_yet_determined_ids).intersection(self.item_ids)),
            ]
        )

    def is_fully_cached(self, item_id: str) -> bool:
        return item_id in self.fully_cached_ids

    def get_data_for_item(self, item_id: str):
        if item_id not in self.fully_cached_items:
            raise ValueError(f"Can't get data from a non-fully cached item. Item ID: {item_id}")
        return self.fully_cached_items[item_id].data

    def get_status_dict(self, ids=False, lengths=True):
        def _dict_val(coll):
            if ids:
                return coll
            elif lengths:
                return len(coll)
            return self.not_yet_determined_ids if ids else len(self.not_yet_determined_ids)

        return {
            ItemCacheState.FULLY_CACHED.value: _dict_val(self.fully_cached_ids),
            ItemCacheState.PARTIALLY_CACHED.value: _dict_val(self.partially_cached_ids),
            ItemCacheState.NOT_CACHED.value: _dict_val(self.not_cached_ids),
            ItemCacheState.CACHE_LEVEL_NOT_DETERMINED.value: _dict_val(self.not_yet_determined_ids),
        }

    def mark_partially_cached(self, item_id):
        item_state: ItemCacheState = self._get_item_cache_status(item_id)
        self.partially_cached_items[item_id] = CachedItem(item_id, None, ItemCacheState.PARTIALLY_CACHED)
        self._remove_from_previous_collection(item_id, item_state)

    def mark_fully_cached(self, item_id, item_data):
        item_state: ItemCacheState = self._get_item_cache_status(item_id)
        self.fully_cached_items[item_id] = CachedItem(item_id, item_data, ItemCacheState.FULLY_CACHED)
        self._remove_from_previous_collection(item_id, item_state)

    def _get_item_cache_status(self, item_id):
        not_yet_determined = True if item_id in self.not_yet_determined_ids else False
        not_cached = True if item_id in self.not_cached_ids else False

        if not any([not_cached, not_yet_determined]):
            raise ValueError(
                f"{self._meta.type_capitalized} with ID '{item_id}' should be in the collection of {self._meta.type_plural} "
                f"with not yet determined or not cached state but it wasn't in any of these. "
                f"This could be a programming error!"
            )
        if all([not_cached, not_yet_determined]):
            raise ValueError(
                f"{self._meta.type_capitalized} with ID '{item_id}' is in the collection of {self._meta.type_plural} "
                f"with not yet determined AND not cached state but should be only one of these. "
                f"This is a programming error!"
            )
        if not_cached:
            return ItemCacheState.NOT_CACHED
        else:
            return ItemCacheState.CACHE_LEVEL_NOT_DETERMINED

    def _remove_from_previous_collection(self, item_id, item_state):
        if item_state == ItemCacheState.NOT_CACHED:
            del self.not_cached_items[item_id]
        elif item_state == ItemCacheState.CACHE_LEVEL_NOT_DETERMINED:
            del self.not_yet_determined_items[item_id]

    @property
    def cache_type(self):
        return self._meta.type

    @property
    def cache_type_plural(self):
        return self._meta.type_plural

    @property
    def cache_type_capitalized(self):
        return self._meta.type_capitalized


class CachingStrategy(ABC):
    """
    The CachingStrategy interface declares operations common to all supported versions
    of the caching algorithm.

    The Context uses this interface to call the algorithm defined by Concrete
    Strategies.
    """

    def __init__(self, output_basedir: str, project_name: str, user_email: str):
        if not output_basedir:
            raise ValueError("Must define output basedir!")

        if not project_name or not user_email:
            raise ValueError(f"Both project name and user email should be set. Current values:{locals()})")
        self.output_basedir = output_basedir
        self.project_name = project_name
        self.user_email = user_email
        self.fill_cache()

    @abstractmethod
    def fill_cache(self):
        pass

    @abstractmethod
    def process_threads(self, thread_response: Dict[str, Any]):
        pass

    @abstractmethod
    def get_cache_state_for_threads(self, thread_ids: List[str], expect_one_message_per_thread: bool):
        pass

    @abstractmethod
    def actualize_cache_state(self, cache_state: CacheResultItems, thread_id: str, message_ids: List[str]):
        pass

    @abstractmethod
    def process_attachment_for_message(
        self, thread_id: str, message_id: str, attachment_id: str, attachment_response: Dict[str, Any]
    ):
        pass

    @abstractmethod
    def get_cache_state_for_message(self, thread_id: str, message_id: str, attachment_id: str):
        pass

    @abstractmethod
    def get_cached_threads(self):
        pass

    @abstractmethod
    def print_actions_performed(self):
        pass


class FileSystemEmailThreadCacheStrategy(CachingStrategy):
    def __init__(self, output_basedir: str, project_name: str, user_email: str):
        # Cache-related properties
        self.cached_thread_ids: List[str] = []
        # Key: Message ID
        self.cached_message_attachments: Set[Tuple[str, str, str]] = set()  # Tuple: (threadID, messageID, attachmentID)
        # Main key: Thread id
        # Inner-dict key: message id, value: message date
        self.thread_to_message_data: Dict[str, Dict[str, str]] = {}
        self.unknown_message_per_thread: Dict[str, Set[str]] = {}

        # Other properties
        user_email_converted = CommonUtils.convert_email_address_to_dirname(user_email)
        self.project_acct_basedir = FileUtils.join_path(output_basedir, user_email_converted)
        self.threads_dir = FileUtils.ensure_dir_created(
            FileUtils.join_path(self.project_acct_basedir, THREADS_DIR_NAME)
        )
        self._cache_actions_performed = CacheActionsPerformed()
        super().__init__(output_basedir, project_name, user_email)

    def get_cached_threads(self):
        return self.thread_to_message_data.keys()

    def fill_cache(self):
        found_thread_dirnames: List[str] = FileUtils.find_files(
            self.threads_dir, find_type=FindResultType.DIRS, regex=".*", single_level=True
        )
        self.cached_thread_ids.extend(found_thread_dirnames)
        for thread_id in self.cached_thread_ids:
            metrics = self._load_message_data_from_file(thread_id)
            self._cache_actions_performed.add(GmailRequestType.MESSAGES, metrics)

        LOG.trace(f"Loaded message data: {self.thread_to_message_data}")

    def _load_message_data_from_file(self, thread_id) -> CacheMetrics:
        """
        Load all message IDs for all threads but loading all payloads into memory would be costly
        so they are skipped here
        Example message data: [{'message_date': '1620084692000', 'message_id': '1793492a16dc62b5'}]
        """
        message_data_file = FileUtils.join_path(self.threads_dir, thread_id, MESSAGE_DATA_FILENAME)
        list_of_message_data, metrics = self._load_data_from_file(message_data_file)
        self.thread_to_message_data[thread_id] = {
            msg_data[MESSAGE_ID]: msg_data[MESSAGE_DATE] for msg_data in list_of_message_data
        }
        return metrics

    def process_threads(self, thread_response: Dict[str, Any]):
        # TODO only write to file if required, i.e. thread is not fully cached. Also, make this configurable
        thread_id: str = GH.get_field(thread_response, ThreadField.ID)
        thread_dir, metrics_1 = self._write_thread_data_to_file(thread_id, thread_response)
        metrics_2 = self._write_message_data_to_file(thread_dir, thread_response)
        metrics = CacheMetrics.combine(metrics_1, metrics_2)
        self._cache_actions_performed.add(GmailRequestType.THREADS_GET, metrics)

    def process_attachment_for_message(
        self, thread_id: str, message_id: str, attachment_id: str, attachment_response: Dict[str, Any]
    ):
        msg_attachment_filename = self._get_attachment_filename(
            thread_id, message_id, attachment_id, create_messages_dir=True
        )
        metrics = self._write_to_file(msg_attachment_filename, attachment_response)
        self._cache_actions_performed.add(GmailRequestType.ATTACHMENTS, metrics)

    def _write_thread_data_to_file(self, thread_id: str, thread_response) -> Tuple[str, CacheMetrics]:
        current_thread_dir = FileUtils.ensure_dir_created(FileUtils.join_path(self.threads_dir, thread_id))
        raw_thread_json_file = FileUtils.join_path(current_thread_dir, THREAD_JSON_FILENAME)
        metrics = self._write_to_file(raw_thread_json_file, thread_response)
        return current_thread_dir, metrics

    def _write_message_data_to_file(self, thread_dir: str, thread_response) -> CacheMetrics:
        message_data_dicts: List[Dict[str, str]] = self._convert_thread_response_to_message_data_dicts(thread_response)
        message_data_file = FileUtils.join_path(thread_dir, MESSAGE_DATA_FILENAME)
        return self._write_to_file(message_data_file, message_data_dicts)

    @staticmethod
    def _load_data_from_file(file) -> Tuple[List[Dict[str, str]], CacheMetrics]:
        data, bytes_read = JsonFileUtils.load_data_from_json_file(file)
        return data, CacheMetrics.create_for_read(1, bytes_read)

    @staticmethod
    def _write_to_file(file, data) -> CacheMetrics:
        bytes_written = JsonFileUtils.write_data_to_file_as_json(file, data, pretty=True)
        return CacheMetrics.create_for_write(1, bytes_written)

    def get_cache_state_for_threads(self, thread_ids: List[str], expect_one_message_per_thread: bool):
        unknown_thread_ids: Set[str] = set(thread_ids).difference(set(self.cached_thread_ids))
        if expect_one_message_per_thread:
            # Only query threads that are not in cache, on other words unknown.
            # All known thread IDs are stored in self.thread_ids.
            # When only one message / thread is expected, consider all known threads as fully cached.

            fully_cached = {}
            for t_id in self.cached_thread_ids:
                try:
                    fully_cached[t_id], metrics = self._get_thread_from_file_system(t_id)
                    self._cache_actions_performed.add(GmailRequestType.THREADS_LIST, metrics)
                except Exception as e:
                    LOG.error("Error while processing thread.", e)
                    LOG.warning("Cannot open file for thread: %s. Adding it to not cached threads.", t_id)
                    unknown_thread_ids.add(t_id)

            return (
                CacheResultItems(thread_ids, cache_type="thread")
                .add_not_cached(unknown_thread_ids)
                .add_fully_cached(fully_cached)
            )

        # If we expect more than 1 message per thread, even known threads may have new messages.
        # In this case, treat all known thread IDs as not yet determined, as messages should be queried again for them.
        known_thread_ids: Set[str] = set(thread_ids).difference(unknown_thread_ids)
        return (
            CacheResultItems(thread_ids, cache_type="thread")
            .add_not_yet_determined(known_thread_ids)
            .add_not_cached(unknown_thread_ids)
        )

    def get_cache_state_for_message(self, thread_id: str, message_id: str, attachment_id: str):
        """
        Caution: This loads all message data into memory including message payloads + attachments so the resulted
        CacheResultItems should not be kept in memory for a long time as it would just accumulate without any particular reason.
        !!Use it with care!!
        :param thread_id:
        :param message_id:
        :param attachment_id:
        :return:
        """
        attachment_data = None
        cache_key = (thread_id, message_id, attachment_id)
        if cache_key not in self.cached_message_attachments:
            # Try to load from Filesystem
            msg_attachment_filename = self._get_attachment_filename(
                thread_id, message_id, attachment_id, create_messages_dir=False
            )
            if FileUtils.does_file_exist(msg_attachment_filename):
                attachment_data = self._load_data_from_file(msg_attachment_filename)
                self.cached_message_attachments.add(cache_key)

        # if not attachment_data:
        #     raise ValueError("BUG! Attachment data is not set!")
        # TODO bug here, attachment_data is None --> Load from disk or memory?
        cached = {thread_id: attachment_data} if cache_key in self.cached_message_attachments else {}
        not_cached = [] if len(cached) > 0 else [thread_id]
        return (
            CacheResultItems([message_id], cache_type="message attachment")
            .add_not_cached(not_cached)
            .add_fully_cached(cached)
        )

    def _get_attachment_filename(self, thread_id, message_id, attachment_id, create_messages_dir=True):
        thread_dir = self._get_thread_dir(thread_id)
        messages_dir: str = self._get_messages_dir(thread_dir, create=create_messages_dir)
        return FileUtils.join_path(messages_dir, self._get_message_attachment_filename(message_id, attachment_id))

    def _get_thread_dir(self, thread_id):
        thread_dir: str = FileUtils.join_path(self.threads_dir, thread_id)
        if not FileUtils.does_path_exist(thread_dir):
            raise ValueError(
                f"Thread dir does not exist for thread with ID: {thread_dir}. "
                f"This should not happen at this point of the execution."
            )
        return thread_dir

    @staticmethod
    def _get_messages_dir(thread_dir, create=True):
        messages_dir = FileUtils.join_path(thread_dir, MESSAGES_DIR_NAME)
        if create:
            FileUtils.ensure_dir_created(messages_dir)
        return messages_dir

    def _get_thread_from_file_system(self, thread_id: str) -> Tuple[Any, CacheMetrics]:
        """
        Caution: This loads all thread data into memory including message payloads.
        !!Use it with care!!
        :param thread_id:
        :return:
        """
        if thread_id not in self.cached_thread_ids:
            raise ValueError(f"Thread with ID '{thread_id}' is not in cache. This should not happen at this point.")
        thread_json_file = FileUtils.join_path(self.threads_dir, thread_id, THREAD_JSON_FILENAME)
        return self._load_data_from_file(thread_json_file)

    def actualize_cache_state(self, cache_state: CacheResultItems, thread_id: str, message_ids: List[str]):
        message_ids_for_thread: List[str] = (
            self.thread_to_message_data[thread_id].keys() if thread_id in self.thread_to_message_data else []
        )
        self.unknown_message_per_thread[thread_id] = set(message_ids).difference(message_ids_for_thread)
        if self.unknown_message_per_thread[thread_id]:
            cache_state.mark_partially_cached(thread_id)
        else:
            # Thread is fully cached with all messages
            data, metrics = self._get_thread_from_file_system(thread_id)
            self._cache_actions_performed.add(GmailRequestType.MESSAGES, metrics)
            cache_state.mark_fully_cached(thread_id, data)

    @staticmethod
    def _convert_thread_response_to_message_data_dicts(thread_response):
        messages_response: List[Dict[str, Any]] = GH.get_field(thread_response, ThreadField.MESSAGES)
        message_data_dicts: List[Dict[str, str]] = []
        for msg in messages_response:
            message_data_dicts.append(
                {MESSAGE_ID: GH.get_field(msg, MessageField.ID), MESSAGE_DATE: GH.get_field(msg, MessageField.DATE)}
            )
        return message_data_dicts

    @staticmethod
    def _get_message_attachment_filename(message_id, attachment_id):
        short_attachment_id = StringUtils.md5_hash(attachment_id)
        return f"message_{message_id}_attachment_{short_attachment_id}.txt"

    def print_actions_performed(self):
        self._cache_actions_performed.print_all()


class NoCacheStrategy(CachingStrategy):
    def get_cached_threads(self):
        LOG.debug(f"Invoked get_cached_threads of {type(self).__name__}")
        return []

    def actualize_cache_state(self, cache_state: CacheResultItems, thread_id: str, message_ids: List[str]):
        LOG.debug(f"Invoked actualize_cache_state of {type(self).__name__}")

    def get_cache_state_for_threads(self, thread_ids: List[str], expect_one_message_per_thread):
        return CacheResultItems(thread_ids, cache_type="thread").add_not_cached(thread_ids)

    def get_cache_state_for_message(self, thread_id: str, message_id: str, attachment_id: str):
        return CacheResultItems([thread_id], cache_type="message").add_not_cached([message_id])

    def fill_cache(self):
        LOG.debug(f"Invoked fill_cache of {type(self).__name__}")

    def process_threads(self, thread_response: Dict[str, Any]):
        LOG.debug(f"Invoked process_threads of {type(self).__name__} with an email thread")

    def process_attachment_for_message(
        self, thread_id: str, message_id: str, attachment_id: str, attachment_response: Dict[str, Any]
    ):
        LOG.debug(f"Invoked process_attachment_for_message of {type(self).__name__} with an email attachment")

    def print_actions_performed(self):
        LOG.info("No cache actions were performed!")


class CachingStrategyType(Enum):
    NO_CACHE = NoCacheStrategy
    FILESYSTEM_CACHE_STRATEGY = FileSystemEmailThreadCacheStrategy
