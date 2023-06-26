import base64
import email
import logging

LOG = logging.getLogger(__name__)


class CommonUtils:
    @staticmethod
    def convert_email_address_to_dirname(user_email: str):
        return user_email.replace("@", "_").replace(".", "_")


class Decoder:
    @staticmethod
    def decode_base64(encoded):
        decoded_data = base64.b64decode(encoded)
        return str(decoded_data)

    @staticmethod
    def decode_base64_urlsafe(encoded):
        # https://www.daimto.com/how-to-read-gmail-message-body-with-python/
        decoded = base64.urlsafe_b64decode(encoded)
        mime_msg = email.message_from_bytes(decoded)

        # Find full message body
        message_main_type = mime_msg.get_content_maintype()
        if message_main_type == "multipart":
            for part in mime_msg.get_payload():
                if part.get_content_maintype() == "text":
                    return part.get_payload()
        elif message_main_type == "text":
            return mime_msg.get_payload()
