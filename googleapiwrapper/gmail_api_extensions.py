import logging
from typing import List, Dict, Any

from googleapiwrapper.gmail_cache import CachingStrategy, CacheResultItems

LOG = logging.getLogger(__name__)


class ApiFetchingContext:
    def __init__(self, strategy: CachingStrategy) -> None:
        # TODO log debug cache strategy type
        self._caching_strategy = strategy

    @property
    def caching_strategy(self) -> "CachingStrategy":
        return self._caching_strategy

    @caching_strategy.setter
    def caching_strategy(self, strategy: CachingStrategy) -> None:
        self._caching_strategy = strategy

    def process_thread(self, thread_response: Dict[str, Any]):
        self._caching_strategy.process_threads(thread_response)

    def process_messages(self, cache_state: CacheResultItems, thread_id: str, message_ids: List[str]):
        self._caching_strategy.actualize_cache_state(cache_state, thread_id, message_ids)

    def process_attachment_for_message(
        self, thread_id: str, message_id: str, attachment_id, attachment_response: Dict[str, Any]
    ):
        self._caching_strategy.process_attachment_for_message(thread_id, message_id, attachment_id, attachment_response)

    def get_cache_state_for_threads(self, thread_ids: List[str], expect_one_message_per_thread: bool):
        return self._caching_strategy.get_cache_state_for_threads(thread_ids, expect_one_message_per_thread)

    def get_cache_state_for_message(self, thread_id: str, message_id: str, attachment_id: str) -> CacheResultItems:
        return self._caching_strategy.get_cache_state_for_message(thread_id, message_id, attachment_id)

    def get_cached_threads(self) -> List[str]:
        return self._caching_strategy.get_cached_threads()

    def print_cache_actions(self):
        self._caching_strategy.print_actions_performed()
