import logging
import os
import pickle
import os.path
from typing import List

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from pythoncommons.file_utils import FileUtils

from googleapiwrapper.common import ServiceType
LOG = logging.getLogger(__name__)


class GoogleApiAuthorizer:
    CREDENTIALS_FILENAME = 'credentials.json'
    TOKEN_FILENAME = 'token.pickle'
    DEFAULT_SCOPES = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    # If modifying these scopes, delete the file token.pickle.
    DEFAULT_WEBSERVER_PORT = 49555

    def __init__(self,
                 service_type: ServiceType,
                 scopes: List[str] = None,
                 server_port: int = DEFAULT_WEBSERVER_PORT,
                 token_filename: str = TOKEN_FILENAME,
                 credentials_filename: str = CREDENTIALS_FILENAME,
                 token_file_path: str = None,
                 credentials_file_path: str = None):
        self.service_type = service_type
        self._set_scopes(scopes)
        self.server_port = server_port
        self.token_full_path = token_filename
        self.token_full_path = self._get_file_full_path(token_filename,
                                                        provided_path=token_file_path,
                                                        should_exist=False,
                                                        file_type="token")
        self.credentials_full_path = self._get_file_full_path(credentials_filename,
                                                              provided_path=credentials_file_path,
                                                              should_exist=True,
                                                              file_type="credentials")
        LOG.info(f"Configuration of {self.__name__}:\n"
                 f"Token file path (read/write): {self.token_full_path}"
                 f"Credentials file path (read-only): {self.credentials_full_path}")

    @staticmethod
    def _get_file_full_path(filename: str,
                            provided_path=None,
                            should_exist=True,
                            file_type=""):
        # output dir takes precedence
        if provided_path:
            if not should_exist:
                return provided_path
            if FileUtils.does_file_exist(provided_path):
                return provided_path
        fallback_path = FileUtils.join_path(os.getcwd(), filename)

        if provided_path:
            LOG.warning(f"Provided {file_type} file path does not exist: {provided_path}. "
                        f"Falling back to path: {fallback_path}")
        return fallback_path

    def _set_scopes(self, scopes):
        self.scopes = scopes
        if self.scopes is None:
            self.scopes = self.service_type.default_scopes

        # https://stackoverflow.com/a/51643134/1106893
        os.environ["OAUTHLIB_RELAX_TOKEN_SCOPE"] = "1"
        self.scopes.extend(self.DEFAULT_SCOPES)

    def authorize(self):
        creds = self._load_token()
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            creds = self._handle_login(creds)
        return creds

    def _load_token(self):
        """
        The file token.pickle stores the user's access and refresh tokens, and is
        created automatically when the authorization flow completes for the first
        time.
        """
        creds = None
        if os.path.exists(self.token_full_path):
            with open(self.token_full_path, 'rb') as token:
                creds = pickle.load(token)
        return creds

    def _handle_login(self, creds):
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(self.credentials_full_path, self.scopes)
            self.authed_creds = flow.run_local_server(port=self.server_port)

            # TODO Save credentials tied to profile (email address)
            session = flow.authorized_session()
            profile_info = session.get('https://www.googleapis.com/userinfo/v2/me').json()
            print(profile_info)
        # Save the credentials for the next run
        self._write_token()
        return self.authed_creds

    def _write_token(self):
        with open(self.token_full_path, 'wb') as token:
            pickle.dump(self.authed_creds, token)
