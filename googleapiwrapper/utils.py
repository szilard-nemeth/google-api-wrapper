import base64
import logging

LOG = logging.getLogger(__name__)

class CommonUtils:
    @staticmethod
    def convert_email_address_to_dirname(user_email: str):
        return user_email.replace('@', '_').replace('.', '_')


class Decoder:
    @staticmethod
    def decode_base64(encoded):
        decoded_data = base64.b64decode(encoded)
        return str(decoded_data)