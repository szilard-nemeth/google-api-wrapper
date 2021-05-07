import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Any, Set, Iterable

from pythoncommons.file_utils import JsonFileUtils, FileUtils, FindResultType

from googleapiwrapper.gmail_domain import Thread, GenericObjectHelper as GH, ThreadField, MessageField
from googleapiwrapper.utils import CommonUtils

MESSAGE_DATE = "message_date"
MESSAGE_ID = "message_id"
THREAD_JSON_FILENAME = "thread.json"
MESSAGE_DATA_FILENAME = "message_data"
THREADS_DIR_NAME = "threads"
LOG = logging.getLogger(__name__)


class ItemCacheState(Enum):
    NOT_CACHED = "not cached"
    CACHE_LEVEL_NOT_DETERMINED = "cache level not yet determined"
    FULLY_CACHED = "fully cached"
    PARTIALLY_CACHED = "partially cached"


@dataclass
class CachedItem:
    id: str
    thread_data: Any
    cache_state: ItemCacheState


class CacheResultItems:
    def __init__(self, thread_ids: List[str]):
        self.item_ids = thread_ids
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
        return sum([
            len(set(self.fully_cached_ids).intersection(self.item_ids)),
            len(set(self.partially_cached_items).intersection(self.item_ids)),
            len(set(self.not_yet_determined_ids).intersection(self.item_ids)),
            ])

    def is_fully_cached(self, item_id: str) -> bool:
        return item_id in self.fully_cached_ids

    def get_data_for_item(self, item_id: str):
        if item_id not in self.fully_cached_items:
            raise ValueError(f"Can't get data from a non-fully cached item. Item ID: {item_id}")
        return self.fully_cached_items[item_id].thread_data

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
            ItemCacheState.CACHE_LEVEL_NOT_DETERMINED.value: _dict_val(self.not_yet_determined_ids)
        }

    def mark_partially_cached(self, thread_id):
        item_state: ItemCacheState = self._get_item_cache_status(thread_id)
        self.partially_cached_items[thread_id] = CachedItem(thread_id, None, ItemCacheState.PARTIALLY_CACHED)
        self._remove_from_previous_collection(thread_id, item_state)

    def mark_fully_cached(self, thread_id, thread_data):
        item_state: ItemCacheState = self._get_item_cache_status(thread_id)
        self.fully_cached_items[thread_id] = CachedItem(thread_id, thread_data, ItemCacheState.FULLY_CACHED)
        self._remove_from_previous_collection(thread_id, item_state)

    def _get_item_cache_status(self, thread_id):
        not_yet_determined = True if thread_id in self.not_yet_determined_ids else False
        not_cached = True if thread_id in self.not_cached_ids else False

        if not any([not_cached, not_yet_determined]):
            raise ValueError(f"Thread with id '{thread_id}' should be in the collection of threads "
                             f"with not yet determined or not cached state but it wasn't in any of these. "
                             f"This could be a programming error!")
        if all([not_cached, not_yet_determined]):
            raise ValueError(f"Thread with id '{thread_id}' is in the collection of threads "
                             f"with not yet determined AND not cached state but should be only one of these. "
                             f"This is a programming error!")
        if not_cached:
            return ItemCacheState.NOT_CACHED
        else:
            return ItemCacheState.CACHE_LEVEL_NOT_DETERMINED

    def _remove_from_previous_collection(self, thread_id, item_state):
        if item_state == ItemCacheState.NOT_CACHED:
            del self.not_cached_items[thread_id]
        elif item_state == ItemCacheState.CACHE_LEVEL_NOT_DETERMINED:
            del self.not_yet_determined_items[thread_id]


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
    def process_threads(self, thread_response: Dict[str, Any], thread: Thread):
        pass

    @abstractmethod
    def get_cache_state(self, thread_ids: List[str], expect_one_message_per_thread: bool):
        pass

    @abstractmethod
    def actualize_cache_state(self, cache_state: CacheResultItems, thread_id: str, message_ids: List[str]):
        pass


class FileSystemEmailThreadCacheStrategy(CachingStrategy):
    def __init__(self, output_basedir: str, project_name: str, user_email: str):
        # Cache-related properties
        self.cached_thread_ids: List[str] = []
        # Main key: Thread id
        # Inner-dict key: message id, value: message date
        self.thread_to_message_data: Dict[str, Dict[str, str]] = {}
        self.unknown_message_per_thread: Dict[str, Set[str]] = {}

        # Other properties
        user_email_converted = CommonUtils.convert_email_address_to_dirname(user_email)
        self.project_acct_basedir = FileUtils.join_path(output_basedir, user_email_converted)
        self.threads_dir = FileUtils.join_path(self.project_acct_basedir, THREADS_DIR_NAME)
        super().__init__(output_basedir, project_name, user_email)

    def fill_cache(self):
        found_thread_dirnames: List[str] = FileUtils.find_files(self.threads_dir, find_type=FindResultType.DIRS, regex=".*")
        self.cached_thread_ids.extend(found_thread_dirnames)
        for thread_id in self.cached_thread_ids:
            self._load_message_data_from_file(thread_id)
        LOG.debug(f"Loaded message data: {self.thread_to_message_data}")

    def _load_message_data_from_file(self, thread_id):
        """
        Load all message IDs for all threads but loading all payloads into memory would be costly
        so they are skipped here
        """
        message_data_file = FileUtils.join_path(self.threads_dir, thread_id, MESSAGE_DATA_FILENAME)
        # Example: [{'message_date': '1620084692000', 'message_id': '1793492a16dc62b5'}]
        list_of_message_data: List[Dict[str, str]] = JsonFileUtils.load_data_from_json_file(message_data_file)
        self.thread_to_message_data[thread_id] = {msg_data[MESSAGE_ID]: msg_data[MESSAGE_DATE] for msg_data in
                                                  list_of_message_data}

    def process_threads(self, thread_response: Dict[str, Any], thread: Thread):
        # TODO only write to file if required, i.e. thread is not fully cached.
        #  Also, make this configurable
        thread_id: str = GH.get_field(thread_response, ThreadField.ID)
        threads_dir = FileUtils.ensure_dir_created(FileUtils.join_path(self.project_acct_basedir, THREADS_DIR_NAME))
        current_thread_dir = FileUtils.ensure_dir_created(FileUtils.join_path(threads_dir, thread_id))
        raw_thread_json_file = FileUtils.join_path(current_thread_dir, THREAD_JSON_FILENAME)
        JsonFileUtils.write_data_to_file_as_json(raw_thread_json_file, thread_response, pretty=True)

        message_data_dicts: List[Dict[str, str]] = self._convert_thread_response_to_message_data_dicts(thread_response)
        message_data_file = FileUtils.join_path(current_thread_dir, MESSAGE_DATA_FILENAME)
        JsonFileUtils.write_data_to_file_as_json(message_data_file, message_data_dicts, pretty=True)

    def get_cache_state(self, thread_ids: List[str], expect_one_message_per_thread: bool):
        unknown_thread_ids: Set[str] = set(thread_ids).difference(set(self.cached_thread_ids))
        if expect_one_message_per_thread:
            # Only query threads that are not in cache, on other words unknown.
            # All known thread IDs are stored in self.thread_ids.
            # When only one message / thread is expected, consider all known threads as fully cached.
            return CacheResultItems(thread_ids)\
                .add_not_cached(unknown_thread_ids)\
                .add_fully_cached({t_id: self._get_thread_from_file_system(t_id) for t_id in self.cached_thread_ids})

        # If we expect more than 1 message per thread, even known threads may have new messages.
        # In this case, treat all known thread IDs as not yet determined, as messages should be queried again for them.
        known_thread_ids: Set[str] = set(thread_ids).difference(unknown_thread_ids)
        return CacheResultItems(thread_ids)\
            .add_not_yet_determined(known_thread_ids)\
            .add_not_cached(unknown_thread_ids)

    def _get_thread_from_file_system(self, thread_id: str):
        """
        Caution: This loads all thread data into memory including message payloads.
        Use it with care!
        :param thread_id:
        :return:
        """
        if thread_id not in self.cached_thread_ids:
            raise ValueError(f"Thread id '{thread_id}' is not in cache. This should not happen at this point.")
        thread_json_file = FileUtils.join_path(self.threads_dir, thread_id, THREAD_JSON_FILENAME)
        return JsonFileUtils.load_data_from_json_file(thread_json_file)

    def actualize_cache_state(self, cache_state: CacheResultItems, thread_id: str, message_ids: List[str]):
        message_ids_for_thread: List[str] = self.thread_to_message_data[thread_id].keys() \
            if thread_id in self.thread_to_message_data else []
        self.unknown_message_per_thread[thread_id] = set(message_ids).difference(message_ids_for_thread)
        if self.unknown_message_per_thread[thread_id]:
            cache_state.mark_partially_cached(thread_id)
            return
        # Thread is fully cached with all messages
        cache_state.mark_fully_cached(thread_id, self._get_thread_from_file_system(thread_id))

    @staticmethod
    def _convert_thread_response_to_message_data_dicts(thread_response):
        messages_response: List[Dict[str, Any]] = GH.get_field(thread_response, ThreadField.MESSAGES)
        message_data_dicts: List[Dict[str, str]] = []
        for msg in messages_response:
            message_data_dicts.append({
                MESSAGE_ID: GH.get_field(msg, MessageField.ID),
                MESSAGE_DATE: GH.get_field(msg, MessageField.DATE)
            })
        return message_data_dicts


class NoCacheStrategy(CachingStrategy):
    def actualize_cache_state(self, cache_state: CacheResultItems, thread_id: str, message_ids: List[str]):
        LOG.debug(f"Invoked actualize_cache_state of {type(self).__name__}")

    def get_cache_state(self, thread_ids: List[str], expect_one_message_per_thread):
        return CacheResultItems(thread_ids).add_not_cached(thread_ids)

    def fill_cache(self):
        LOG.debug(f"Invoked fill_cache of {type(self).__name__}")

    def process_threads(self, thread_response: Dict[str, Any], thread: Thread):
        LOG.debug(f"Invoked handle_threads of {type(self).__name__} with an email thread")


class ApiFetchingContext:
    def __init__(self, strategy: CachingStrategy) -> None:
        # TODO log debug cache strategy type
        self._caching_strategy = strategy

    @property
    def caching_strategy(self) -> CachingStrategy:
        return self._caching_strategy

    @caching_strategy.setter
    def caching_strategy(self, strategy: CachingStrategy) -> None:
        self._caching_strategy = strategy

    def process_thread(self, thread_response: Dict[str, Any], thread_obj: Thread):
        self._caching_strategy.process_threads(thread_response, thread_obj)

    def get_cache_state_for_threads(self, thread_ids: List[str], expect_one_message_per_thread: bool) -> CacheResultItems:
        return self._caching_strategy.get_cache_state(thread_ids, expect_one_message_per_thread)

    def process_messages(self, cache_state: CacheResultItems, thread_id: str, message_ids: List[str]):
        self._caching_strategy.actualize_cache_state(cache_state, thread_id, message_ids)


class CachingStrategyType(Enum):
    NO_CACHE = NoCacheStrategy
    FILESYSTEM_CACHE_STRATEGY = FileSystemEmailThreadCacheStrategy
