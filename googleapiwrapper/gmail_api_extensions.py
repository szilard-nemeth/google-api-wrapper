import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Dict, Any

from pythoncommons.file_utils import JsonFileUtils, FileUtils, FindResultType

from googleapiwrapper.gmail_domain import Thread, GenericObjectHelper as GH, ThreadField, MessageField

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
        user_email_converted = user_email.replace('@', '_').replace('.', '_')
        self.project_acct_basedir = FileUtils.join_path(self.output_basedir, project_name, user_email_converted)
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


class FileSystemEmailThreadCacheStrategy(CachingStrategy):
    def __init__(self, output_basedir: str, project_name: str, user_email: str):
        self.thread_ids: List[str] = []
        self.message_data_dicts: List[Dict[str, str]] = []
        super().__init__(output_basedir, project_name, user_email)

    def fill_cache(self):
        threads_dir = FileUtils.join_path(self.project_acct_basedir, THREADS_DIR_NAME)
        found_thread_dirnames = FileUtils.find_files(threads_dir, find_type=FindResultType.DIRS, regex=".*")
        self.thread_ids.extend(found_thread_dirnames)
        for thread_id in self.thread_ids:
            message_data_file = FileUtils.join_path(threads_dir, thread_id, MESSAGE_DATA_FILENAME)
            self.message_data_dicts.extend(JsonFileUtils.load_data_from_json_file(message_data_file))
        LOG.debug(f"Loaded message data: {self.message_data_dicts}")

    def handle_threads(self, thread_response: Dict[str, Any], thread: Thread):
        thread_id: str = GH.get_field(thread_response, ThreadField.ID)
        threads_dir = FileUtils.ensure_dir_created(FileUtils.join_path(self.project_acct_basedir, THREADS_DIR_NAME))
        current_thread_dir = FileUtils.ensure_dir_created(FileUtils.join_path(threads_dir, thread_id))
        raw_thread_json_file = FileUtils.join_path(current_thread_dir, THREAD_JSON_FILENAME)
        JsonFileUtils.write_data_to_file_as_json(raw_thread_json_file, thread_response, pretty=True)

        messages_response: List[Dict[str, Any]] = GH.get_field(thread_response, ThreadField.MESSAGES)

        message_data_dicts: List[Dict[str, str]] = []
        for msg in messages_response:
            message_data_dicts.append({
                "message_id": GH.get_field(msg, MessageField.ID),
                "message_date": GH.get_field(msg, MessageField.DATE)
            })
        message_data_file = FileUtils.join_path(current_thread_dir, MESSAGE_DATA_FILENAME)
        JsonFileUtils.write_data_to_file_as_json(message_data_file, message_data_dicts, pretty=True)

    def get_thread_ids_to_query_from_api(self, thread_ids, expect_one_message_per_thread=False):
        if expect_one_message_per_thread:
            return set(thread_ids).difference(set(self.thread_ids))
        return thread_ids


class NoCacheStrategy(CachingStrategy):
    def fill_cache(self):
        LOG.debug(f"Invoked fill_cache of {type(self).__name__}")

    def handle_threads(self, thread_response: Dict[str, Any], thread: Thread):
        LOG.debug(f"Invoked handle_threads of {type(self).__name__} with an email thread")


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


class CachingStrategyType(Enum):
    NO_CACHE = NoCacheStrategy
    FILESYSTEM_CACHE_STRATEGY = FileSystemEmailThreadCacheStrategy
