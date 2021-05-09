import binascii
import logging
import sys
from dataclasses import dataclass, field, InitVar
import datetime
from enum import Enum
from typing import List, Dict, Any

from googleapiwrapper.utils import Decoder
LOG = logging.getLogger(__name__)


class ThreadQueryFormat(Enum):
    FULL = "full"
    METADATA = "metadata"
    MINIMAL = "minimal"


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


class ThreadQueryParam(Enum):
    FORMAT = "format"


class ApiItemType(Enum):
    THREAD = "thread"
    MESSAGE = "message"


class MimeType(Enum):
    TEXT_PLAIN = "text/plain"


@dataclass
class MessagePartBody:
    data: str
    size: str
    attachment_id: str

    def short_str(self):
        return f"{{ size: {self.size}, attachment_id: {self.attachment_id} }}"


@dataclass
class Header:
    name: str
    value: str


@dataclass
class MessagePart:
    id: str
    mime_type: str
    headers: List[Header]
    body: MessagePartBody
    parts: List[Any]  # Cannot refer to MessagePart :(

    def short_str(self):
        return f"{{ ID: {self.id}, " \
               f"mime_type: {self.mime_type}, " \
               f"headers: {self.headers}, " \
               f"body (short): {self.body.short_str()}, " \
               f"parts (short): {[part.short_str() for part in self.parts]} }}"


@dataclass
class Message:
    id: str
    thread_id: str
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

    def short_str(self):
        return f"{{ ID: {self.id}, snippet: {self.snippet}, subject: {self.subject} }}"


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
        self.body_data = body_data
        self.mime_type = mime_type

    def short_str(self):
        return f"{{ mime_type: {self.mime_type} }}"


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
    def _get_conversion_context():
        module = sys.modules["googleapiwrapper.gmail_api"]
        return module.__getattribute__("CONVERSION_CONTEXT")

    @staticmethod
    def from_message(message: Message, thread_id: str):
        GmailMessage._get_conversion_context().register_current_message(message)
        # message.message_parts already contains all MessageParts (recursively collected)
        return GmailMessage(message.id, thread_id, message.subject, message.date, message.message_parts)

    def _convert_message_parts(self, message_parts: List[MessagePart]) -> List[GmailMessageBodyPart]:
        result: List[GmailMessageBodyPart] = []
        CONVERSION_CONTEXT = GmailMessage._get_conversion_context()
        for message_part in message_parts:
            CONVERSION_CONTEXT.register_current_message_part(message_part)
            mime_type: str = message_part.mime_type
            body, decoding_successful, empty = self._decode_base64_encoded_body(message_part)
            gmail_msg_body_part: GmailMessageBodyPart = GmailMessageBodyPart(body, mime_type)
            result.append(gmail_msg_body_part)
            if not decoding_successful:
                CONVERSION_CONTEXT.report_decode_error(self.thread_id, gmail_msg_body_part)
            if empty:
                CONVERSION_CONTEXT.report_empty_body(self.thread_id, gmail_msg_body_part)
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
            LOG.exception(f"Failed to parse base64 encoded data for message with ID: {self.msg_id}."
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

    def __str__(self):
        return f"{{ mesage: {self.message.short_str()}, " \
               f"message_part: {self.message_part.short_str()}, " \
               f"gmail_msg_body_part: {self.gmail_msg_body_part} " \
               f"}}"


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


class GenericObjectHelper:
    @staticmethod
    def get_field(gmail_api_obj: Dict, field, default_val=None):
        if isinstance(field, Enum):
            if field.value in gmail_api_obj:
                ret = gmail_api_obj[field.value]
            else:
                ret = default_val
            if not ret:
                ret = default_val
            return ret
