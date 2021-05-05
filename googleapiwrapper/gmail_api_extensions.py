from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Dict, Any

from pythoncommons.file_utils import JsonFileUtils, FileUtils

from googleapiwrapper.gmail_domain import Thread, GenericObjectHelper as GH, ThreadField, MessageField


class CachingStrategy(ABC):
    """
    The CachingStrategy interface declares operations common to all supported versions
    of the caching algorithm.

    The Context uses this interface to call the algorithm defined by Concrete
    Strategies.
    """

    def __init__(self, output_basedir: str):
        if not output_basedir:
            raise ValueError("Must define output basedir!")
        self.output_basedir = output_basedir

    @abstractmethod
    def handle_threads(self, thread_response: Dict[str, Any], thread: Thread):
        pass


class RawEmailThreadCachingStrategy(CachingStrategy):
    def handle_threads(self, thread_response: Dict[str, Any], thread: Thread):
        thread_id: str = GH.get_field(thread_response, ThreadField.ID)
        threads_dir = FileUtils.ensure_dir_created(FileUtils.join_path(self.output_basedir, "threads"))
        current_thread_dir = FileUtils.ensure_dir_created(FileUtils.join_path(threads_dir, thread_id))
        JsonFileUtils.write_data_to_file_as_json(FileUtils.join_path(current_thread_dir, "thread_raw.json"), thread_response, pretty=True)


class SomeOtherStrategy(CachingStrategy):
    def handle_threads(self, thread_response: Dict[str, Any], thread: Thread):
        # TODO implement
        pass


class NoCacheStrategy(CachingStrategy):
    def handle_threads(self, thread_response: Dict[str, Any], thread: Thread):
        # TODO implement
        pass


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


class CachingStrategyType(Enum):
    NO_CACHE = NoCacheStrategy
    RAW_MAIL_THREADS = RawEmailThreadCachingStrategy
    OTHER: SomeOtherStrategy
