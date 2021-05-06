import logging
import os
import pickle
import os.path
from dataclasses import dataclass
from typing import List

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from pythoncommons.file_utils import FileUtils

from googleapiwrapper.common import ServiceType
LOG = logging.getLogger(__name__)


@dataclass
class AuthedSession:
    authed_creds: Credentials
    user_email: str
    user_name: str
    project_name: str


class GoogleApiAuthorizer:
    CREDENTIALS_FILENAME = 'credentials.json'
    TOKEN_FILENAME = 'token.pickle'
    DEFAULT_SCOPES = ["https://www.googleapis.com/auth/userinfo.profile", "https://www.googleapis.com/auth/userinfo.email"]
    # TODO If modifying these scopes, delete the file token.pickle.
    DEFAULT_WEBSERVER_PORT = 49555

    def __init__(self,
                 service_type: ServiceType,
                 project: str = None,  # TODO Make this mandatory later
                 scopes: List[str] = None,
                 server_port: int = DEFAULT_WEBSERVER_PORT,
                 token_filename: str = TOKEN_FILENAME,
                 credentials_filename: str = CREDENTIALS_FILENAME,
                 token_file_path: str = None,
                 credentials_file_path: str = None):
        self.service_type = service_type
        self.project = project if project else "unknown"
        self._set_scopes(scopes)
        self.server_port = server_port
        self.token_full_path = self._get_file_full_path(token_filename,
                                                        provided_path=token_file_path,
                                                        should_exist=False,
                                                        file_type="token")
        self.credentials_full_path = self._get_file_full_path(credentials_filename,
                                                              provided_path=credentials_file_path,
                                                              should_exist=True,
                                                              file_type="credentials")
        LOG.info(f"Configuration of {type(self).__name__}:\n"
                 f"Project: {self.project}\n"
                 f"Scopes: {self.scopes}\n"
                 f"Server port: {self.server_port}\n"
                 f"Token file path (read/write): {self.token_full_path}\n"
                 f"Credentials file path (read-only): {self.credentials_full_path}\n")

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

    def authorize(self) -> AuthedSession:
        authed_session: AuthedSession = self._load_token()
        # If there are no (valid) credentials available, let the user log in.
        if not authed_session or not authed_session.authed_creds or not authed_session.authed_creds.valid:
            authed_session = self._handle_login(authed_session)
        return authed_session

    def _load_token(self) -> AuthedSession:
        """
        The file token.pickle stores the user's access and refresh tokens, and is
        created automatically when the authorization flow completes for the first
        time.
        """
        authed_session: AuthedSession or None = None
        if os.path.exists(self.token_full_path):
            with open(self.token_full_path, 'rb') as token:
                authed_session = pickle.load(token)
        return authed_session

    def _handle_login(self, authed_session: AuthedSession) -> AuthedSession:
        if authed_session:
            creds = authed_session.authed_creds
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(self.credentials_full_path, self.scopes)
            authed_creds: Credentials = flow.run_local_server(port=self.server_port, prompt='consent')

            session = flow.authorized_session()
            profile_info = session.get('https://www.googleapis.com/userinfo/v2/me').json()
            authed_session = AuthedSession(authed_creds, profile_info["email"], profile_info["name"], self.project)
        # Save the credentials for the next run
        self._write_token(authed_session)
        return authed_session

    def _write_token(self, authed_session: AuthedSession):
        with open(self.token_full_path, 'wb') as token:
            pickle.dump(authed_session, token)
