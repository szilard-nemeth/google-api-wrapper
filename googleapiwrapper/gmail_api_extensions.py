import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Dict, Any, Set

from pythoncommons.file_utils import JsonFileUtils, FileUtils, FindResultType

from googleapiwrapper.gmail_domain import Thread, GenericObjectHelper as GH, ThreadField, MessageField
from googleapiwrapper.utils import CommonUtils

MESSAGE_DATE = "message_date"
MESSAGE_ID = "message_id"
THREAD_JSON_FILENAME = "thread.json"
MESSAGE_DATA_FILENAME = "message_data"
THREADS_DIR_NAME = "threads"
LOG = logging.getLogger(__name__)


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
    def handle_threads(self, thread_response: Dict[str, Any], thread: Thread):
        pass

    @abstractmethod
    def get_thread_ids_to_query_from_api(self, thread_ids, expect_one_message_per_thread=False):
        pass

    @abstractmethod
    def is_thread_fully_cached(self, thread_id: str, message_ids: List[str]):
        pass

    @abstractmethod
    def get_unknown_messages_for_thread(self, thread_id: str):
        pass

    @abstractmethod
    def get_thread_from_cache(self, thread_id):
        pass


class FileSystemEmailThreadCacheStrategy(CachingStrategy):
    def __init__(self, output_basedir: str, project_name: str, user_email: str):
        self.thread_ids: List[str] = []
        # Key: Thread id
        # Inner-dict key: message id
        # Inner-dict value: message date
        self.thread_to_message_data: Dict[str, Dict[str, str]] = {}
        self.unknown_message_per_thread: Dict[str, Set[str]] = {}
        user_email_converted = CommonUtils.convert_email_address_to_dirname(user_email)
        self.project_acct_basedir = FileUtils.join_path(output_basedir, user_email_converted)
        self.threads_dir = FileUtils.join_path(self.project_acct_basedir, THREADS_DIR_NAME)
        super().__init__(output_basedir, project_name, user_email)

    def fill_cache(self):
        found_thread_dirnames = FileUtils.find_files(self.threads_dir, find_type=FindResultType.DIRS, regex=".*")
        self.thread_ids.extend(found_thread_dirnames)

        # Load all message IDs for all threads but loading all payloads into memory would be costly
        # so they are skipped here
        for thread_id in self.thread_ids:
            message_data_file = FileUtils.join_path(self.threads_dir, thread_id, MESSAGE_DATA_FILENAME)
            # Example: [{'message_date': '1620084692000', 'message_id': '1793492a16dc62b5'}]
            list_of_message_data: List[Dict[str, str]] = JsonFileUtils.load_data_from_json_file(message_data_file)
            self.thread_to_message_data[thread_id] = {msg_data[MESSAGE_ID]: msg_data[MESSAGE_DATE] for msg_data in list_of_message_data}
        LOG.debug(f"Loaded message data: {self.thread_to_message_data}")

    def get_thread_from_cache(self, thread_id):
        if thread_id not in self.thread_ids:
            raise ValueError(f"Thread id '{thread_id}' is not in cache. This should not happen at this point.")
        thread_json_file = FileUtils.join_path(self.threads_dir, thread_id, THREAD_JSON_FILENAME)
        return JsonFileUtils.load_data_from_json_file(thread_json_file)

    def handle_threads(self, thread_response: Dict[str, Any], thread: Thread):
        thread_id: str = GH.get_field(thread_response, ThreadField.ID)
        threads_dir = FileUtils.ensure_dir_created(FileUtils.join_path(self.project_acct_basedir, THREADS_DIR_NAME))
        current_thread_dir = FileUtils.ensure_dir_created(FileUtils.join_path(threads_dir, thread_id))
        raw_thread_json_file = FileUtils.join_path(current_thread_dir, THREAD_JSON_FILENAME)
        JsonFileUtils.write_data_to_file_as_json(raw_thread_json_file, thread_response, pretty=True)

        message_data_dicts: List[Dict[str, str]] = self._convert_thread_response_to_message_data_dicts(thread_response)
        message_data_file = FileUtils.join_path(current_thread_dir, MESSAGE_DATA_FILENAME)
        JsonFileUtils.write_data_to_file_as_json(message_data_file, message_data_dicts, pretty=True)

    def is_thread_fully_cached(self, thread_id: str, message_ids: List[str]):
        unknown_message_ids: Set[str] = set(message_ids).difference(self._get_message_ids_for_thread(thread_id))
        self.unknown_message_per_thread[thread_id] = unknown_message_ids
        if not unknown_message_ids:
            return True
        return False

    def get_unknown_messages_for_thread(self, thread_id: str):
        return self.unknown_message_per_thread[thread_id]

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

    def _get_message_ids_for_thread(self, thread_id: str):
        if thread_id not in self.thread_to_message_data:
            return []
        return self.thread_to_message_data[thread_id].keys()

    def get_thread_ids_to_query_from_api(self, thread_ids, expect_one_message_per_thread=False):
        if expect_one_message_per_thread:
            # Only query threads that are not in cache
            return set(thread_ids).difference(set(self.thread_ids))
        # If we expect more than 1 message per thread, even known threads may have new messages.
        # Return all thread IDs in this case, as messages should be queried again for these.
        return thread_ids


class NoCacheStrategy(CachingStrategy):
    def get_unknown_messages_for_thread(self, thread_id: str):
        raise ValueError("This method does not make sense for this implementation.")

    def get_thread_from_cache(self, thread_id):
        return None

    def get_thread_ids_to_query_from_api(self, thread_ids, expect_one_message_per_thread=False):
        # All threads are unknown if we are using no cache strategy
        return thread_ids

    def fill_cache(self):
        LOG.debug(f"Invoked fill_cache of {type(self).__name__}")

    def handle_threads(self, thread_response: Dict[str, Any], thread: Thread):
        LOG.debug(f"Invoked handle_threads of {type(self).__name__} with an email thread")

    def is_thread_fully_cached(self, thread_id: str, message_ids: List[str]):
        return False


class ApiFetchingContext:
    def __init__(self, strategy: CachingStrategy) -> None:
        self._caching_strategy = strategy

    @property
    def caching_strategy(self) -> CachingStrategy:
        return self._caching_strategy

    @caching_strategy.setter
    def caching_strategy(self, strategy: CachingStrategy) -> None:
        self._caching_strategy = strategy

    def process_thread(self, thread_response: Dict[str, Any], thread_obj: Thread):
        self._caching_strategy.handle_threads(thread_response, thread_obj)

    def get_thread_ids_to_query_from_api(self, thread_ids: List[str],
                                         expect_one_message_per_thread=False):
        return self._caching_strategy.get_thread_ids_to_query_from_api(thread_ids,
                                                                expect_one_message_per_thread=expect_one_message_per_thread)

    def is_thread_fully_cached(self, thread_id: str, message_ids: List[str]) -> bool:
        fully_cached: bool = self._caching_strategy.is_thread_fully_cached(thread_id, message_ids)
        return fully_cached

    def get_unknown_message_ids_for_thread(self, thread_id: str) -> List[str]:
        unknown_message_ids: List[str] = self._caching_strategy.get_unknown_messages_for_thread(thread_id)
        return unknown_message_ids

    def get_thread_from_cache(self, thread_id):
        return self._caching_strategy.get_thread_from_cache(thread_id)


class CachingStrategyType(Enum):
    NO_CACHE = NoCacheStrategy
    FILESYSTEM_CACHE_STRATEGY = FileSystemEmailThreadCacheStrategy
