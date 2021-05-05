import binascii
import logging
import sys
from dataclasses import dataclass, field, InitVar
import datetime
from enum import Enum
from typing import List, Dict, Any

from googleapiclient.discovery import build
from googleapiwrapper.google_auth import GoogleApiAuthorizer, AuthedSession
from pythoncommons.string_utils import auto_str

from googleapiwrapper.utils import Decoder

LOG = logging.getLogger(__name__)


class HeaderField(Enum):
    NAME = "name"
    VALUE = "value"


class ListQueryParam(Enum):
    QUERY = "q"
    USER_ID = "userId"
    MAX_RESULTS = "maxResults"


class GetAttachmentParam(Enum):
    USER_ID = "userId"
    MESSAGE_ID = "messageId"
    ATTACHMENT_ID = "id"


class ThreadsResponseField(Enum):
    THREADS = "threads"


class MessagePartBodyField(Enum):
    SIZE = "size"
    DATA = "data"
    ATTACHMENT_ID = "attachmentId"


class MessagePartField(Enum):
    PART_ID = "partId"
    MIME_TYPE = "mimeType"
    HEADERS = "headers"
    BODY = "body"
    PARTS = "parts"


class MessageField(Enum):
    ID = "id"
    THREAD_ID = "threadId"
    SNIPPET = "snippet"
    DATE = "internalDate"
    PAYLOAD = "payload"


class ThreadField(Enum):
    ID = "id"
    MESSAGES = "messages"
    SNIPPET = "snippet"


class ApiItemType(Enum):
    THREAD = "thread"
    MESSAGE = "message"


class MimeType(Enum):
    TEXT_PLAIN = "text/plain"


@dataclass
class MessagePartBody:
    data: str
    size: str
    attachmentId: str


@dataclass
class Header:
    name: str
    value: str


@dataclass
class MessagePart:
    id: str
    mimeType: str  # TODO rename
    headers: List[Header]
    body: MessagePartBody
    parts: List[Any]  # Cannot refer to MessagePart :(


@dataclass
class Message:
    id: str
    threadId: str
    date: datetime.datetime
    snippet: str
    payload: MessagePart
    subject: str = field(init=False)
    message_parts: List[MessagePart] = field(init=False)

    def __post_init__(self):
        self.subject = self._get_subject_from_headers()
        self.message_parts: List[MessagePart] = self._get_all_msg_parts_recursive(self.payload)

    def _get_subject_from_headers(self):
        for header in self.payload.headers:
            if header.name == 'Subject':
                return header.value
        return None

    def _get_all_msg_parts_recursive(self, msg_part: MessagePart):
        lst: List[MessagePart] = []
        for part in msg_part.parts:
            lst += self._get_all_msg_parts_recursive(part)
        lst.append(msg_part)
        return lst


@dataclass
class Thread:
    id: str
    subject: str
    messages: List[Message]


# CUSTOM CLASSES
@dataclass
class GmailMessageBodyPart:
    body_data: str
    mime_type: str

    def __init__(self, body_data, mime_type):
        self.body = body_data
        self.mime_type = mime_type


@dataclass
class GmailMessage:
    msg_id: str
    thread_id: str
    subject: str
    date: datetime.datetime
    message_parts: InitVar[List[MessagePart]]

    def __post_init__(self, message_parts):
        self.message_body_parts: List[GmailMessageBodyPart] = self._convert_message_parts(message_parts)

    @staticmethod
    def from_message(message: Message, thread_id: str):
        CONVERSION_CONTEXT.register_current_message(message)
        # message.message_parts already contains all MessageParts (recursively collected)
        return GmailMessage(message.id, thread_id, message.subject, message.date, message.message_parts)

    def _convert_message_parts(self, message_parts: List[MessagePart]) -> List[GmailMessageBodyPart]:
        result: List[GmailMessageBodyPart] = []
        for message_part in message_parts:
            CONVERSION_CONTEXT.register_current_message_part(message_part)
            mime_type: str = message_part.mimeType
            body, successful, empty = self._decode_base64_encoded_body(message_part)
            gmail_msg_body_part: GmailMessageBodyPart = GmailMessageBodyPart(body, mime_type)
            result.append(gmail_msg_body_part)
            if not successful:
                CONVERSION_CONTEXT.report_decode_error(gmail_msg_body_part)
            if empty:
                CONVERSION_CONTEXT.report_empty_body(gmail_msg_body_part)
        return result

    def _decode_base64_encoded_body(self, message_part: MessagePart):
        encoded_body_data = message_part.body.data
        successful = True
        empty = False
        try:
            if encoded_body_data:
                decoded_body_data = Decoder.decode_base64(encoded_body_data)
            else:
                decoded_body_data = ""
                empty = True
        except binascii.Error:
            LOG.exception(f"Failed to parse base64 encoded data for message with id: {self.msg_id}."
                          f"Storing original body data to object and storing original API object as well.")
            decoded_body_data = encoded_body_data
            successful = False
        return decoded_body_data, successful, empty

    def get_all_plain_text_parts(self) -> List[GmailMessageBodyPart]:
        return self.get_all_parts_with_type(MimeType.TEXT_PLAIN)

    def get_all_parts_with_type(self, mime_type: MimeType) -> List[GmailMessageBodyPart]:
        return self._filter_by_mime_type(mime_type, self.message_body_parts)

    @staticmethod
    def _filter_by_mime_type(mime_type: MimeType, message_parts: List[GmailMessageBodyPart]) -> List[
        GmailMessageBodyPart]:
        return list(filter(lambda x: x.mime_type == mime_type.value, message_parts))


@dataclass
class MessagePartDescriptor:
    message: Message
    message_part: MessagePart
    gmail_msg_body_part: GmailMessageBodyPart


@dataclass
class GmailThread:
    def __init__(self, api_id, messages: List[GmailMessage]):
        self.api_id = api_id
        self.messages: List[GmailMessage] = messages


@dataclass
class GmailThreads:
    threads: List[GmailThread] = field(default_factory=list)

    def add(self, thread: Thread):
        gmail_thread = GmailThread(thread.id, [GmailMessage.from_message(m, thread.id) for m in thread.messages])
        self.threads.append(gmail_thread)

    @property
    def messages(self) -> List[GmailMessage]:
        return [msg for t in self.threads for msg in t.messages]


class Progress:
    def __init__(self, item_type: ApiItemType, limit: int = None):
        self.requests_count = 0
        self.all_items_count = 0
        self.processed_items = 0
        self.new_items_with_last_request = -1
        self.item_type = item_type
        self.limit = limit

    def _print_status(self):
        LOG.info(f"[Request #: {self.requests_count}] "
                 f"Received {self.new_items_with_last_request} more {self.item_type.value}s")

    def incr_requests(self):
        self.requests_count += 1

    def register_new_items(self, number_of_new_items: int, print_status=True):
        self.all_items_count += number_of_new_items
        self.new_items_with_last_request = number_of_new_items
        if print_status:
            self._print_status()

    def incr_processed_items(self):
        self.processed_items += 1

    def is_limit_reached(self):
        if self.limit:
            return self.processed_items > self.limit
        return False

    def print_processing_items(self):
        LOG.debug(f"Processing {self.item_type.value}s: {self.processed_items} / {self.all_items_count}")


@auto_str
class ApiConversionContext:
    def __init__(self, item_type: ApiItemType, limit: int = None):
        self.progress = Progress(item_type, limit=limit)
        self.decode_errors: List[MessagePartDescriptor] = []
        self.empty_bodies: List[MessagePartDescriptor] = []

        # Set later
        self.current_message: Message or None = None
        self.current_message_part: MessagePart or None = None

    def register_current_message(self, message: Message):
        self.current_message: Message = message

    def register_current_message_part(self, message_part: MessagePart):
        self.current_message_part = message_part

    def report_decode_error(self, gmail_msg_body_part: GmailMessageBodyPart):
        # TODO error log
        self.decode_errors.append(MessagePartDescriptor(self.current_message,
                                                        self.current_message_part, gmail_msg_body_part))

    def report_empty_body(self, gmail_msg_body_part: GmailMessageBodyPart):
        # TODO error log
        self.empty_bodies.append(MessagePartDescriptor(self.current_message,
                                                       self.current_message_part, gmail_msg_body_part))

    def handle_encoding_errors(self):
        # TODO error log all
        self.decode_errors.clear()

    def handle_empty_bodies(self, func):
        # TODO error log all
        for descriptor in self.empty_bodies:
            func(descriptor)
        self.empty_bodies.clear()


CONVERSION_CONTEXT: ApiConversionContext = None
this = sys.modules[__name__]


class GmailWrapper:
    USERID_ME = 'me'
    DEFAULT_API_FIELDS = {ListQueryParam.USER_ID.value: USERID_ME}
    DEFAULT_PAGE_SIZE = 100

    def __init__(self, authorizer: GoogleApiAuthorizer, api_version: str = None):
        self.authed_session: AuthedSession = authorizer.authorize()
        if not api_version:
            api_version = authorizer.service_type.default_api_version
        self.service = build(authorizer.service_type.service_name, api_version,
                             credentials=self.authed_session.authed_creds)
        self.users_svc = self.service.users()
        self.messages_svc = self.users_svc.messages()
        self.threads_svc = self.users_svc.threads()
        self.attachments_svc = self.messages_svc.attachments()

    def query_threads_with_paging(self, query: str = None, limit: int = None,
                                  sanity_check=True) -> GmailThreads:
        this.CONVERSION_CONTEXT = ApiConversionContext(ApiItemType.THREAD, limit=limit)
        ctx = CONVERSION_CONTEXT
        kwargs = self._get_new_kwargs()
        if query:
            kwargs[ListQueryParam.QUERY.value] = query
        if limit and limit < GmailWrapper.DEFAULT_PAGE_SIZE:
            kwargs[ListQueryParam.MAX_RESULTS.value] = limit
        request = self.threads_svc.list(**kwargs)

        threads = GmailThreads()

        while request is not None:
            response: Dict[str, Any] = request.execute()
            if response:
                ctx.progress.incr_requests()
                list_of_threads: List[Dict[str, str]] = response.get(ThreadsResponseField.THREADS.value, [])
                ctx.progress.register_new_items(len(list_of_threads), print_status=True)

                for idx, thread in enumerate(list_of_threads):
                    ctx.progress.incr_processed_items()
                    if ctx.progress.is_limit_reached():
                        LOG.warning("Reached limit, stop processing more items.")
                        return threads
                    ctx.progress.print_processing_items()

                    thread_response = self._query_thread_data(thread)
                    messages_response: List[Dict[str, Any]] = self._get_field(thread_response, ThreadField.MESSAGES)
                    messages: List[Message] = [self.parse_api_message(message) for message in messages_response]
                    ctx.handle_empty_bodies(lambda desc: self._query_attachment_of_descriptor(desc))
                    # Create Thread object and that will create GmailMessage and rest of the stuff
                    thread_obj: Thread = Thread(self._get_field(thread_response, ThreadField.ID),
                                        messages[0].subject, messages)
                    threads.add(thread_obj)
                    if sanity_check:
                        self._sanity_check(thread_obj)
            request = self.threads_svc.list_next(request, response)

        # TODO error log all messages that had missing body + attachment request
        ctx.handle_encoding_errors()
        return threads

    def _query_attachment_of_descriptor(self, descriptor: MessagePartDescriptor):
        # Fix MessagePartBody object that has attachmentId only
        # Quoting from API doc for Field 'attachmentId':
        # When present, contains the ID of an external attachment that can be retrieved in a
        # separate messages.attachments.get request.
        # When not present, the entire content of the message part body is contained in the data field.
        message_id = descriptor.message.id
        attachment_id = descriptor.message_part.body.attachmentId
        if not message_id or not attachment_id:
            LOG.error("Both message_id and attachment_id has to be set in order to query attachment details from API."
                      f"Object was: {descriptor}")
            return
        attachment_response = self._query_attachment(message_id, attachment_id)
        # TODO Implement attachment handling

    def parse_api_message(self, message: Dict):
        message_part = self._get_field(message, MessageField.PAYLOAD)
        message_id: str = self._get_field(message, MessageField.ID)
        message_part_obj: MessagePart = self.parse_message_part(message_part, message_id)
        return Message(
            message_id,
            self._get_field(message, MessageField.THREAD_ID),
            datetime.datetime.fromtimestamp(int(self._get_field(message, MessageField.DATE)) / 1000),
            self._get_field(message, MessageField.SNIPPET),
            message_part_obj
        )

    def parse_message_part(self, message_part, message_id: str) -> MessagePart:
        message_parts = self._get_field(message_part, MessagePartField.PARTS, [])
        headers = self._parse_headers(message_part)
        message_part_obj: MessagePart = MessagePart(
            self._get_field(message_part, MessagePartField.PART_ID),
            self._get_field(message_part, MessagePartField.MIME_TYPE),
            headers,
            self._parse_message_part_body_obj(self._get_field(message_part, MessagePartField.BODY)),
            [self.parse_message_part(part, message_id) for part in message_parts],
        )
        return message_part_obj

    def _parse_headers(self, message_part):
        headers_list: List[Dict[str, str]] = self._get_field(message_part, MessagePartField.HEADERS)
        headers: List[Header] = []
        for header_dict in headers_list:
            headers.append(Header(self._get_field(header_dict, HeaderField.NAME),
                                  self._get_field(header_dict, HeaderField.VALUE)))
        return headers

    def _parse_message_part_body_obj(self, messagepart_body):
        message_part_body_obj = MessagePartBody(self._get_field(messagepart_body, MessagePartBodyField.DATA),
                                                self._get_field(messagepart_body, MessagePartBodyField.SIZE),
                                                self._get_field(messagepart_body, MessagePartBodyField.ATTACHMENT_ID))
        return message_part_body_obj

    def _query_thread_data(self, thread):
        kwargs = self._get_new_kwargs()
        kwargs[ThreadField.ID.value] = self._get_field(thread, ThreadField.ID)
        tdata = self.threads_svc.get(**kwargs).execute()
        return tdata

    def _query_attachment(self, message_id: str, attachment_id: str):
        kwargs = self._get_new_kwargs()
        kwargs[GetAttachmentParam.MESSAGE_ID.value] = message_id
        kwargs[GetAttachmentParam.ATTACHMENT_ID.value] = attachment_id
        attachment_data = self.attachments_svc.get(**kwargs).execute()
        return attachment_data

    @staticmethod
    def _get_field(gmail_api_obj: Dict, field, default_val=None):
        if isinstance(field, Enum):
            if field.value in gmail_api_obj:
                ret = gmail_api_obj[field.value]
            else:
                ret = default_val
            if not ret:
                ret = default_val
            return ret

    @staticmethod
    def _get_new_kwargs():
        kwargs = {}
        kwargs.update(GmailWrapper.DEFAULT_API_FIELDS)
        return kwargs

    def _sanity_check(self, thread: Thread):
        # TODO implement checking if all messages have the same subject
        pass
