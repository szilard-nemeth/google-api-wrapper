import enum

MESSAGE_DATE = "message_date"
MESSAGE_ID = "message_id"
THREAD_JSON_FILENAME = "thread.json"
MESSAGE_DATA_FILENAME = "message_data"
THREADS_DIR_NAME = "threads"
MESSAGES_DIR_NAME = "messages"


class GmailRequestType(enum.Enum):
    THREADS_GET = "threads_get"
    THREADS_LIST = "threads_list"
    MESSAGES = "messages"
    USERS = "users"
    ATTACHMENTS = "attachments"
