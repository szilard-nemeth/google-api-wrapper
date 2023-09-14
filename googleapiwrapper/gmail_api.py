import logging
import sys
import datetime
from collections import defaultdict
from dataclasses import dataclass
from typing import List, Dict, Any

from googleapiclient.discovery import build
from pythoncommons.date_utils import timeit

from googleapiwrapper.gmail_api_extensions import ApiFetchingContext
from googleapiwrapper.gmail_cache import CacheResultItems, CachingStrategyType
from googleapiwrapper.gmail_common import GmailRequestType
from googleapiwrapper.gmail_domain import (
    Message,
    MessagePartDescriptor,
    MessagePart,
    GmailMessageBodyPart,
    ThreadsResponseField,
    MessagePartField,
    MessagePartBodyField,
    HeaderField,
    MessagePartBody,
    Header,
    ThreadField,
    GetAttachmentParam,
    MessageField,
    Thread,
    ListQueryParam,
    GmailThreads,
    GenericObjectHelper as GH,
    ThreadQueryFormat,
    ThreadQueryParam,
)
from googleapiwrapper.google_auth import GoogleApiAuthorizer, AuthedSession
from pythoncommons.string_utils import auto_str

CONV_CONTEXT_PREFIX = "[API Conversion context] "
LOG = logging.getLogger(__name__)


class GmailRequestLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f"[GMAIL REQUEST] {msg}", kwargs


REQ_LOG = GmailRequestLoggerAdapter(LOG, {})


class Progress:
    def __init__(self, limit: int = None):
        self.req_counts: Dict[GmailRequestType, int] = defaultdict(int)
        self.all_items_count: Dict[GmailRequestType, int] = defaultdict(int)
        self.processed_items: Dict[GmailRequestType, int] = defaultdict(int)
        self.new_items_with_last_request: Dict[GmailRequestType, int] = defaultdict(int)
        self.current_item_id: Dict[GmailRequestType, str] = defaultdict(str)
        self.limit = limit

    def print_stats(self):
        LOG.info("=" * 50 + "    STATISTICS    " + "=" * 50)
        LOG.info("# of requests, by type: %s", self.req_counts)
        LOG.info("All items count, by type: %s", self.all_items_count)
        LOG.info("Processed items, by type: %s", self.processed_items)
        LOG.info("=" * 50 + "    END OF STATISTICS    " + "=" * 50)

    def _print_status(self, req_type: GmailRequestType):
        LOG.info(
            f"[# of requests: {self.req_counts[req_type]}] "
            f"Received {self.new_items_with_last_request[req_type]} more {req_type.value}"
        )

    def incr_requests(self, req_type: GmailRequestType):
        self.req_counts[req_type] += 1

    def register_new_items(self, req_type: GmailRequestType, number_of_new_items: int, print_status=True):
        self.all_items_count[req_type] += number_of_new_items
        self.new_items_with_last_request[req_type] = number_of_new_items
        if print_status:
            self._print_status(req_type)

    def incr_processed_items(self, req_type: GmailRequestType, item_id: str):
        self.current_item_id[req_type] = item_id
        self.processed_items[req_type] += 1

    def is_limit_reached(self, req_type: GmailRequestType):
        if self.limit:
            return self.processed_items[req_type] > self.limit
        return False

    def print_processing_items(self, req_type: GmailRequestType, print_item_id=True):
        msg = f"Processing {req_type.value}: {self.processed_items[req_type]} / {self.all_items_count[req_type]}."
        if print_item_id:
            msg += f" [Item ID: {self.current_item_id[req_type]}]"
        LOG.debug(msg)


# TODO Move this object as a dependency of ApiFetchingContext
@auto_str
class ApiConversionContext:
    def __init__(
        self,
        limit: int = None,
        show_empty_body_errors=True,
        query: str = None,
        format: ThreadQueryFormat = None,
        sanity_check: bool = False,
        expect_one_message_per_thread: bool = False,
    ):
        self.query = query
        self.sanity_check = sanity_check
        self.format = format
        self.expect_one_message_per_thread = expect_one_message_per_thread
        self.progress = Progress(limit=limit)
        self.show_empty_body_errors = show_empty_body_errors
        self.decode_errors: List[MessagePartDescriptor] = []
        self.empty_bodies: List[MessagePartDescriptor] = []

        # Set later
        self.threads: GmailThreads or None = None
        self.current_message: Message or None = None
        self.current_message_part: MessagePart or None = None

    def register_current_message(self, message: Message):
        self.current_message: Message = message

    def register_current_message_part(self, message_part: MessagePart):
        self.current_message_part = message_part

    def report_decode_error(self, thread_id: str, gmail_msg_body_part: GmailMessageBodyPart):
        self._log_error(
            f"Decoding error for thread with ID '{thread_id}'.\n"
            f"Details:\n{self._get_current_message_details(gmail_msg_body_part)}"
        )
        self.decode_errors.append(
            MessagePartDescriptor(self.current_message, self.current_message_part, gmail_msg_body_part)
        )

    def report_empty_body(self, thread_id: str, gmail_msg_body_part: GmailMessageBodyPart):
        details = self._get_current_message_details(
            gmail_msg_body_part, short_message_part=True, short_gmail_message_body_part=True, log_message=False
        )
        if self.show_empty_body_errors:
            self._log_error(f"Empty message for thread with ID '{thread_id}'.\n" f"Details:\n{details}")

        self.empty_bodies.append(
            MessagePartDescriptor(self.current_message, self.current_message_part, gmail_msg_body_part)
        )

    @staticmethod
    def _log_error(msg: str):
        LOG.error(CONV_CONTEXT_PREFIX + " " + msg)

    def _get_current_message_details(
        self,
        gmail_msg_body_part: GmailMessageBodyPart,
        short_message_part=True,
        short_gmail_message_body_part=True,
        log_message=True,
    ):
        message_str = self.current_message.short_str() if log_message else "<ommitted>"
        message_part_str = self.current_message_part.short_str() if short_message_part else self.current_message_part
        gmail_msg_body_part_str = (
            gmail_msg_body_part.short_str() if short_gmail_message_body_part else gmail_msg_body_part
        )
        return (
            f"Message: {message_str},\n"
            f"MessagePart: {message_part_str},\n"
            f"gmail_msg_body_part: {gmail_msg_body_part_str}"
        )

    def handle_encoding_errors(self):
        # TODO error log all messages that had missing body + attachment request
        self.decode_errors.clear()

    def handle_empty_bodies(self, func):
        # TODO error log all
        for descriptor in self.empty_bodies:
            func(descriptor)
        self.empty_bodies.clear()

    def perform_sanity_check(self, thread_id: str):
        for desc in self.empty_bodies:
            d_message_id: str = desc.message.id
            d_thread_id: str = desc.message.thread_id
            LOG.debug("Found message id in descriptor: %s", d_message_id)
            LOG.debug("Found thread id in descriptor: %s", d_thread_id)
            if thread_id != d_thread_id:
                thread_ids = set([d.message.thread_id for d in self.empty_bodies])
                raise ValueError(
                    "Mismatch in thread ids. "
                    "Current thread id: {}, found thread_ids: {}".format(thread_id, thread_ids)
                )


CONVERSION_CONTEXT: ApiConversionContext = None
module = sys.modules[__name__]


@dataclass
class ThreadQueryResults:
    threads: GmailThreads

    def __post_init__(self):
        messages = self.threads.messages
        self.no_of_threads = len(self.threads.threads)
        self.no_of_messages = len(messages)
        # TODO Add subject property to GmailThread
        self.subjects_and_ids = [(t.messages[0].subject, t.api_id) for t in self.threads.threads]
        self.unique_subjects = set([tup[0] for tup in self.subjects_and_ids])

    def __str__(self):
        return (
            f"{{ Number of threads: {self.no_of_threads}\n"
            f"Number of messages: {self.no_of_messages}\n"
            f"Subjects and ids: {self.subjects_and_ids}\n"
            f"Unique subjects: {self.unique_subjects}"
        )


class GmailWrapper:
    USERID_ME = "me"
    DEFAULT_API_FIELDS = {ListQueryParam.USER_ID.value: USERID_ME}
    DEFAULT_PAGE_SIZE = 100

    def __init__(
        self,
        authorizer: GoogleApiAuthorizer,
        api_version: str = None,
        cache_strategy_type: CachingStrategyType = CachingStrategyType.FILESYSTEM_CACHE_STRATEGY,
        output_basedir: str = None,
    ):
        self.authed_session: AuthedSession = authorizer.authorize()
        cache_strategy_obj = cache_strategy_type.value(
            output_basedir, self.authed_session.project_name, self.authed_session.user_email
        )
        self.api_fetching_ctx: ApiFetchingContext = ApiFetchingContext(cache_strategy_obj)
        if not api_version:
            api_version = authorizer.service_type.default_api_version
        self.service = build(
            authorizer.service_type.service_name, api_version, credentials=self.authed_session.authed_creds
        )
        self.users_svc = self.service.users()
        self.messages_svc = self.users_svc.messages()
        self.threads_svc = self.users_svc.threads()
        self.attachments_svc = self.messages_svc.attachments()

    @timeit
    def query_threads(
        self,
        query: str = None,
        limit: int = None,
        sanity_check=True,
        expect_one_message_per_thread=False,
        show_empty_body_errors=True,
        format: ThreadQueryFormat = ThreadQueryFormat.FULL,
        offline: bool = False,
    ) -> ThreadQueryResults:
        query_conf: str = (
            f"Query: {query}, Limit: {limit}, Expect one message per thread: {expect_one_message_per_thread}"
        )
        LOG.info(f"Querying gmail threads. Config: {query_conf}")
        module.CONVERSION_CONTEXT = ApiConversionContext(
            query=query,
            limit=limit,
            format=format,
            show_empty_body_errors=show_empty_body_errors,
            sanity_check=sanity_check,
            expect_one_message_per_thread=expect_one_message_per_thread,
        )
        ctx = CONVERSION_CONTEXT
        kwargs = self._get_new_kwargs()
        if query:
            kwargs[ListQueryParam.QUERY.value] = query
        if limit and limit < GmailWrapper.DEFAULT_PAGE_SIZE:
            kwargs[ListQueryParam.MAX_RESULTS.value] = limit

        if not offline:
            self._fetch_threads(ctx, kwargs, self._threads_response_handler)
        else:
            rt = GmailRequestType.THREADS_LIST
            ctx.threads = GmailThreads()
            thread_ids = self.api_fetching_ctx.get_cached_threads()
            ctx.progress.register_new_items(rt, len(thread_ids), print_status=True)
            self._process_threads(ctx, rt, thread_ids)
        ctx.handle_encoding_errors()
        LOG.info(f"Finished querying gmail threads. Config: {query_conf}")
        ctx.progress.print_stats()
        return ThreadQueryResults(ctx.threads)

    @staticmethod
    def _log_cache_state_details(cache_state: CacheResultItems, item_ids: List[str]):
        ct_plural: str = cache_state.cache_type_plural
        no_of_items: int = len(item_ids)
        LOG.info(
            f"Found cached {ct_plural} {cache_state.get_no_of_any_cached_for_items()} / {no_of_items}. "
            f"Breakdown of cache state: \n{cache_state.get_status_dict()}"
        )
        LOG.trace(f"API fetching context returned cache state for {len(item_ids)} {ct_plural}: {cache_state}")

    def _fetch_thread_data_minimal(self, thread_id, ctx: ApiConversionContext) -> List[str]:
        # Try to query in minimal format first, hoping that some messages are already in cache
        thread_resp_minimal: Dict[str, Any] = self._fetch_thread_data(thread_id, ctx, format=ThreadQueryFormat.MINIMAL)
        messages_response: List[Dict[str, Any]] = GH.get_field(thread_resp_minimal, ThreadField.MESSAGES)
        message_ids: List[str] = [GH.get_field(msg, MessageField.ID) for msg in messages_response]
        return message_ids

    def _request_thread_or_load_from_cache(
        self, thread_id: str, cache_state: CacheResultItems, ctx: ApiConversionContext
    ):
        loaded_from_cache = False
        accepted_thread_query_formats = (ThreadQueryFormat.FULL, ThreadQueryFormat.RAW)
        if ctx.format not in accepted_thread_query_formats:
            raise ValueError(
                "Expecting Gmail query format to be in: {}. Actual value: {}".format(
                    accepted_thread_query_formats, ctx.format
                )
            )

        if not cache_state.is_fully_cached(thread_id):
            message_ids: List[str] = self._fetch_thread_data_minimal(thread_id, ctx)
            self.api_fetching_ctx.process_messages(cache_state, thread_id, message_ids)

        # Check if thread is now considered as fully cached, given the provided message IDs above
        if cache_state.is_fully_cached(thread_id):
            thread_resp_full = self._get_item_from_cache(cache_state, thread_id)
            loaded_from_cache = True
        else:
            # Not all messages for this thread are in cache.
            # In this case, we need to retrieve the thread again, now with specified format
            thread_resp_full: Dict[str, Any] = self._fetch_thread_data(thread_id, ctx, format=ctx.format)
        return thread_resp_full, loaded_from_cache

    @staticmethod
    def _get_item_from_cache(cache_state: CacheResultItems, item_id):
        ct = cache_state.cache_type
        ctc = cache_state.cache_type_capitalized
        thread_id_str = f"{ctc} ID: {item_id}"
        LOG.debug(
            f"{ctc} found in cache (fully cached / all messages were found in cache), won't make further API requests for it. {thread_id_str}"
        )
        thread_resp_full = cache_state.get_data_for_item(item_id)
        if not thread_resp_full:
            raise ValueError(f"{ctc} data is None for {ct} ID '{item_id}'. Please check logs.")
        return thread_resp_full

    def _convert_to_thread_object(self, ctx, sanity_check: bool, thread_id: str, thread_resp_full):
        messages_response: List[Dict[str, Any]] = GH.get_field(thread_resp_full, ThreadField.MESSAGES)
        messages: List[Message] = [self.parse_api_message(message) for message in messages_response]

        if sanity_check:
            ctx.perform_sanity_check(thread_id)

        arbitrary_msg_subject: str = messages[0].subject
        thread_obj: Thread = Thread(thread_id, arbitrary_msg_subject, messages)
        if sanity_check:
            self._sanity_check(thread_obj)
        return thread_obj

    def request_attachment_or_load_from_cache(self, descriptor: MessagePartDescriptor, ctx: ApiConversionContext):
        # Fix MessagePartBody object that has attachmentId only
        # Quoting from API doc for Field 'attachmentId':
        # When present, contains the ID of an external attachment that can be retrieved in a
        # separate messages.attachments.get request.
        # When not present, the entire content of the message part body is contained in the data field.
        message_id: str = descriptor.message.id
        thread_id: str = descriptor.message.thread_id
        attachment_id = descriptor.message_part.body.attachment_id
        if not message_id or not attachment_id:
            if ctx.show_empty_body_errors:
                LOG.error(
                    "Both message_id and attachment_id has to be set in order to load message attachment from cache "
                    f"or to query attachment details from API.\nObject was: {descriptor}"
                )
            return

        cache_state: CacheResultItems = self.api_fetching_ctx.get_cache_state_for_message(
            thread_id, message_id, attachment_id
        )
        self._log_cache_state_details(cache_state, [message_id])
        if cache_state.is_fully_cached(message_id):
            attachment_response = self._get_item_from_cache(cache_state, thread_id)
        else:
            attachment_response: Dict[str, Any] = self._fetch_attachment(ctx, thread_id, message_id, attachment_id)
            self.api_fetching_ctx.process_attachment_for_message(
                thread_id, message_id, attachment_id, attachment_response
            )

        # Fix the GmailMessageBodyPart object's body_data property with the contents of the attachment.
        # TODO consider storing FS instead of whole file contents in memory?
        descriptor.gmail_msg_body_part.body_data = attachment_response

    def parse_api_message(self, message: Dict):
        message_part = GH.get_field(message, MessageField.PAYLOAD)
        message_id: str = GH.get_field(message, MessageField.ID)
        message_part_obj: MessagePart = self.parse_message_part(message_part, message_id)
        return Message(
            message_id,
            GH.get_field(message, MessageField.THREAD_ID),
            datetime.datetime.fromtimestamp(int(GH.get_field(message, MessageField.DATE)) / 1000),
            GH.get_field(message, MessageField.SNIPPET),
            message_part_obj,
        )

    def parse_message_part(self, message_part, message_id: str) -> MessagePart:
        message_parts = GH.get_field(message_part, MessagePartField.PARTS, [])
        headers = self._parse_headers(message_part)
        message_part_obj: MessagePart = MessagePart(
            GH.get_field(message_part, MessagePartField.PART_ID),
            GH.get_field(message_part, MessagePartField.MIME_TYPE),
            headers,
            self._parse_message_part_body_obj(GH.get_field(message_part, MessagePartField.BODY)),
            [self.parse_message_part(part, message_id) for part in message_parts],
        )
        return message_part_obj

    @staticmethod
    def _parse_headers(message_part):
        headers_list: List[Dict[str, str]] = GH.get_field(message_part, MessagePartField.HEADERS)
        headers: List[Header] = []
        if not headers_list:
            LOG.warning("Headers is empty for message part: %s", message_part)
            return headers

        for header_dict in headers_list:
            headers.append(
                Header(GH.get_field(header_dict, HeaderField.NAME), GH.get_field(header_dict, HeaderField.VALUE))
            )
        return headers

    @staticmethod
    def _parse_message_part_body_obj(messagepart_body: Dict[str, Any]):
        message_part_body_obj = MessagePartBody(
            GH.get_field(messagepart_body, MessagePartBodyField.DATA),
            GH.get_field(messagepart_body, MessagePartBodyField.SIZE),
            GH.get_field(messagepart_body, MessagePartBodyField.ATTACHMENT_ID),
        )
        return message_part_body_obj

    def _fetch_thread_data(
        self, thread_id: str, ctx: ApiConversionContext, format: ThreadQueryFormat = ThreadQueryFormat.MINIMAL
    ):
        kwargs = self._get_new_kwargs()
        kwargs[ThreadField.ID.value] = thread_id
        kwargs[ThreadQueryParam.FORMAT.value] = format.value
        # TODO print email subject
        REQ_LOG.info(f"Requesting gmail thread with ID '{thread_id}', format: {format.value}")
        tdata = self.threads_svc.get(**kwargs).execute()
        ctx.progress.incr_requests(GmailRequestType.THREADS_GET)
        return tdata

    def _fetch_attachment(
        self, ctx: ApiConversionContext, thread_id: str, message_id: str, attachment_id: str
    ) -> Dict[str, Any]:
        kwargs = self._get_new_kwargs()
        kwargs[GetAttachmentParam.MESSAGE_ID.value] = message_id
        kwargs[GetAttachmentParam.ATTACHMENT_ID.value] = attachment_id
        REQ_LOG.info(
            f"Requesting gmail attachment for message with ID '{message_id}', Thread ID '{thread_id}', Attachment ID '{attachment_id}'"
        )
        response = self.attachments_svc.get(**kwargs).execute()
        ctx.progress.incr_requests(GmailRequestType.ATTACHMENTS)
        return response

    @staticmethod
    def _get_new_kwargs():
        kwargs = {}
        kwargs.update(GmailWrapper.DEFAULT_API_FIELDS)
        return kwargs

    def _sanity_check(self, thread: Thread):
        # TODO implement checking if all messages have the same subject
        pass

    def _fetch_threads(self, ctx: ApiConversionContext, kwargs, response_handler_func):
        request = self.threads_svc.list(**kwargs)
        ctx.threads = GmailThreads()
        while request is not None:
            REQ_LOG.info("Requesting gmail threads")
            response: Dict[str, Any] = request.execute()
            ctx.progress.incr_requests(GmailRequestType.THREADS_LIST)
            response_handler_func(ctx, response)
            request = self.threads_svc.list_next(request, response)

    def _threads_response_handler(self, ctx: ApiConversionContext, response):
        progress = ctx.progress
        if response:
            rt = GmailRequestType.THREADS_LIST
            list_of_threads: List[Dict[str, str]] = response.get(ThreadsResponseField.THREADS.value, [])
            progress.register_new_items(rt, len(list_of_threads), print_status=True)
            thread_ids: List[str] = [GH.get_field(t, ThreadField.ID) for t in list_of_threads]
            self._process_threads(ctx, rt, thread_ids)

    def _process_threads(self, ctx: ApiConversionContext, rt: GmailRequestType, thread_ids: List[str]):
        progress = ctx.progress

        cache_state: CacheResultItems = self.api_fetching_ctx.get_cache_state_for_threads(
            thread_ids, ctx.expect_one_message_per_thread
        )
        self._log_cache_state_details(cache_state, thread_ids)
        for idx, thread_id in enumerate(thread_ids):
            # TODO consider limiting only real sent requests, not processed items!
            progress.incr_processed_items(rt, thread_id)
            if progress.is_limit_reached(rt):
                LOG.warning(f"Reached request limit of {progress.limit}, stop processing more items.")
                return ThreadQueryResults(ctx.threads)
            progress.print_processing_items(rt)
            thread_resp_full, loaded_from_cache = self._request_thread_or_load_from_cache(thread_id, cache_state, ctx)
            if not loaded_from_cache:
                self.api_fetching_ctx.process_thread(thread_resp_full)
            thread_obj: Thread = self._convert_to_thread_object(ctx, ctx.sanity_check, thread_id, thread_resp_full)
            ctx.threads.add(thread_obj)  # This action will internally create GmailMessage and rest of the stuff
            ctx.handle_empty_bodies(lambda desc: self.request_attachment_or_load_from_cache(desc, ctx))

        self.api_fetching_ctx.print_cache_actions()
