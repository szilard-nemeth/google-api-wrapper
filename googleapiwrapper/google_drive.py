import logging
import os
from enum import Enum
from typing import List, Tuple, Dict

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from pythoncommons.string_utils import auto_str, StringUtils

from googleapiwrapper.google_auth import GoogleApiAuthorizer

LOG = logging.getLogger(__name__)


class DuplicateFileHandlingMode(Enum):
    REMOVE_AND_CREATE = "REMOVE_AND_CREATE"
    ADD_NEW_REVISION = "ADD_NEW_REVISION"


class DriveApiScope(Enum):
    # https://developers.google.com/drive/api/v2/about-auth
    DRIVE_PER_FILE_ACCESS = "https://www.googleapis.com/auth/drive.file"


class DriveApiMimeType(Enum):
    FILE = "application/vnd.google-apps.file"
    FOLDER = "application/vnd.google-apps.folder"


class NormalMimeType(Enum):
    APPLICATION_OCTET_STREAM = "application/octet-stream"


class DriveApiMimeTypes:
    # https://stackoverflow.com/questions/4212861/what-is-a-correct-mime-type-for-docx-pptx-etc
    # https://stackoverflow.com/questions/11894772/google-drive-mime-types-listing
    MIME_MAPPINGS = {
        "application/vnd.openxmlformats-officedocument.presentationml.presentation": "MS Presentation (pptx)",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "MS Word document (docx)",
        "application/vnd.ms-powerpoint": "MS Presentation (ppt)",

        "application/pdf": "PDF document",
        "application/x-apple-diskimage": "Apple disk image",
        "application/zip": "Zip file",
        "text/plain": "Plain text file",
        "application/msword": "MS Word document (doc)",
        
        "image/jpeg": "JPEG image",
        "image/gif": "GIF image",
        "video/mp4": "Video (mp4)",

        "application/vnd.google-apps.spreadsheet": "Google sheet",
        DriveApiMimeType.FOLDER: "Google drive folder",
        DriveApiMimeType.FILE: "Google drive file",
        "application/vnd.google-apps.document": "Google doc",
        "application/vnd.google-apps.form": "Google form",
        "application/vnd.google-apps.presentation": "Google presentation",
        "application/vnd.google-apps.map": "Google map",
    }


class FileField:
    F_OWNER = "owners"
    SHARING_USER = "sharingUser"
    SHARED_WITH_ME_TIME = "sharedWithMeTime"
    MODIFIED_TIME = "modifiedTime"
    CREATED_TIME = "createdTime"
    LINK = "webViewLink"
    MIMETYPE = "mimeType"
    NAME = "name"
    ID = "id"

    _ALL_FIELDS_WITH_DISPLAY_NAME = [(ID, "ID"),
                                     (NAME, "Name"),
                                     (MIMETYPE, "Type"),
                                     (LINK, "Link"),
                                     (CREATED_TIME, "Created date"),
                                     (MODIFIED_TIME, "Last modified time"),
                                     (SHARED_WITH_ME_TIME, "Shared with me date"),
                                     (F_OWNER, "Owner")]

    PRINTABLE_FIELD_DISPLAY_NAMES = ["Name", "Link", "Shared with me date", "Owner", "Type"]
    # FIELDS_TO_PRINT = [tup[0] for tup in FIELDS_TO_PRINT]

    BASIC_FIELDS_COMMA_SEPARATED = ", ".join([ID, NAME, F_OWNER])
    GOOGLE_API_FIELDS = [tup[0] for tup in _ALL_FIELDS_WITH_DISPLAY_NAME]
    GOOGLE_API_FIELDS_COMMA_SEPARATED = ", ".join(GOOGLE_API_FIELDS)
    FIELD_DISPLAY_NAMES = [tup[1] for tup in _ALL_FIELDS_WITH_DISPLAY_NAME]


class GenericUserField:
    UNKNOWN_USER = 'unknown'
    EMAIL_ADDRESS = 'emailAddress'
    DISPLAY_NAME = 'displayName'


class GenericApiField:
    PAGING_NEXT_PAGE_TOKEN = "nextPageToken"


@auto_str
class DriveApiUser(dict):
    def __init__(self, owner_dict):
        super(DriveApiUser, self).__init__()
        # convenience variables
        email_field = GenericUserField.EMAIL_ADDRESS
        display_name_field = GenericUserField.DISPLAY_NAME
        unknown_user = GenericUserField.UNKNOWN_USER

        email = owner_dict[email_field] if email_field in owner_dict else unknown_user
        name = owner_dict[display_name_field] if display_name_field in owner_dict else unknown_user
        self.email = email
        self.name = StringUtils.replace_special_chars(name)

    def __repr__(self):
        return self.__str__()


@auto_str
class DriveApiFile(dict):
    def __init__(self, id, name, link, created_date, modified_date, shared_with_me_date, mime_type, owners,
                 sharing_user: DriveApiUser):
        super(DriveApiFile, self).__init__()
        self.id = id
        self.name = StringUtils.replace_special_chars(name)
        self.link = link
        self.created_date = created_date
        self.modified_date = modified_date
        self.shared_with_me_date = shared_with_me_date
        self.mime_type = mime_type
        self.owners = owners

        sharing_user.name = StringUtils.replace_special_chars(sharing_user.name)
        self.sharing_user = sharing_user

    def __repr__(self):
        return self.__str__()


class DriveApiWrapper:
    DEFAULT_API_VERSION = 'v3'
    DEFAULT_ORDER_BY = "sharedWithMeTime desc"
    QUERY_SHARED_WITH_ME = "sharedWithMe"
    DEFAULT_PAGE_SIZE = 100

    def __init__(self, authorizer: GoogleApiAuthorizer, api_version: str = None):
        self.authed_session = authorizer.authorize()
        if not api_version:
            api_version = authorizer.service_type.default_api_version
        self.service = build(authorizer.service_type.service_name, api_version,
                             credentials=self.authed_session.authed_creds)
        self.files_service = self.service.files()

    @staticmethod
    def get_field_names_with_pagination(fields, resource_type='files'):
        # File fields are documented here: https://developers.google.com/drive/api/v3/reference/files#resource
        fields_str = "{res}({fields})".format(res=resource_type, fields=fields)
        return "{}, {}".format(GenericApiField.PAGING_NEXT_PAGE_TOKEN, fields_str)

    def print_shared_files(self, page_size=DEFAULT_PAGE_SIZE, fields=None, order_by=DEFAULT_ORDER_BY):
        files = self.get_shared_files(page_size=page_size, fields=fields, order_by=order_by)
        for file in files:
            LOG.info(u'{0} ({1})'.format(file[FileField.NAME], file[FileField.ID]))

    def get_shared_files(self, page_size=DEFAULT_PAGE_SIZE, fields: List[str] = None, order_by: str = DEFAULT_ORDER_BY):
        fields_str = self._get_field_names(fields)
        return self.list_files_with_paging(self.QUERY_SHARED_WITH_ME, page_size, fields_str, order_by)

    @staticmethod
    def _get_field_names(fields):
        if not fields:
            fields = DriveApiWrapper._get_default_fields()
        fields_str = DriveApiWrapper.get_field_names_with_pagination(fields)
        return fields_str

    @staticmethod
    def _get_default_fields():
        return FileField.GOOGLE_API_FIELDS_COMMA_SEPARATED

    def get_files(self,
                  filename: str,
                  mime_type=DriveApiMimeType.FILE,
                  page_size=DEFAULT_PAGE_SIZE,
                  fields: List[str] = None,
                  parent: str = None,
                  just_not_trashed: bool = False,
                  order_by: str = DEFAULT_ORDER_BY) -> List[DriveApiFile]:
        fields_str = self._get_field_names(fields)
        query: str = f"mimeType = '{mime_type.value}' and name = '{filename}'"
        if parent:
            query += f" and '{parent}' in parents"
        if just_not_trashed:
            query += " and trashed != true"
        return self.list_files_with_paging(query, page_size, fields_str, order_by)

    def list_files_with_paging(self, query, page_size, fields, order_by) -> List[DriveApiFile]:
        result_files = []
        LOG.info("Listing files with query: %s. Page size: %s, Fields: %s, order by: %s",
                 query, page_size, fields, order_by)
        request = self.files_service.list(q=query, pageSize=page_size, fields=fields, orderBy=order_by)
        while request is not None:
            files_doc = request.execute()
            if files_doc:
                api_file_results = files_doc.get('files', [])
                drive_api_files: List[DriveApiFile] = [DriveApiWrapper._convert_to_drive_file_object(i) for i in api_file_results]
                result_files.extend(drive_api_files)
            else:
                LOG.warning('No files found.')
            request = self.files_service.list_next(request, files_doc)

        return result_files

    def upload_file(self, path_to_local_file: str, drive_path: str,
                    dupe_file_handling_mode: DuplicateFileHandlingMode = DuplicateFileHandlingMode.ADD_NEW_REVISION):
        invalid = False
        components: List[str] = []
        dirnames: List[str] = []
        if not drive_path:
            invalid = True
        if not invalid:
            # Assumption: Last part of path is the filename, not dir
            components = drive_path.split(os.sep)
            dirnames = [c for c in components[:-1] if c]
            invalid = True if len(dirnames) == 0 else False
        if invalid:
            raise ValueError("Invalid Google Drive path. "
                             "Expecting a normal file path with at least one component separated by '{}'!"
                             "Given file path: {}".format(os.sep, drive_path))

        filename = components[-1]
        file_metadata = {"name": filename}
        if len(dirnames) > 0:
            dirs_as_path = os.sep.join(dirnames)
            folder_structure: List[Tuple[str, str]] = self.create_folder_structure(dirs_as_path)
            last_folder = folder_structure[-1]
            parent_folder_id = last_folder[0]
            parent_folder_name = last_folder[1]
            file_metadata["parents"] = [parent_folder_id]
        else:
            # One component pathname is just a filename, assuming parent = "root"
            parent_folder_id = None
            parent_folder_name = "root"

        existing_files: List[DriveApiFile] = self.get_files(filename,
                                                            mime_type=NormalMimeType.APPLICATION_OCTET_STREAM,
                                                            parent=parent_folder_id,
                                                            just_not_trashed=True)
        if not existing_files:
            self._upload_and_create_new_file(file_metadata, path_to_local_file)
            return

        LOG.info("Found %d files with name '%s' under parent folder: %s.")
        if len(existing_files) > 1:
            LOG.warning("Falling back to duplicate file handling mode: REMOVE_AND_CREATE",
                        len(existing_files), parent_folder_name)
            dupe_file_handling_mode = DuplicateFileHandlingMode.REMOVE_AND_CREATE

        if dupe_file_handling_mode == DuplicateFileHandlingMode.REMOVE_AND_CREATE:
            self._remove_file_if_exists(existing_files, filename, parent_folder_id)
            self._upload_and_create_new_file(file_metadata, path_to_local_file)
        elif dupe_file_handling_mode == DuplicateFileHandlingMode.ADD_NEW_REVISION:
            media_file = MediaFileUpload(path_to_local_file, mimetype=NormalMimeType.APPLICATION_OCTET_STREAM.value)
            file = self.files_service.update(fileId=existing_files[0].id,
                                             media_body=media_file,
                                             fields='id').execute()

    def _upload_and_create_new_file(self, file_metadata, path_to_file):
        media_file = MediaFileUpload(path_to_file, mimetype=NormalMimeType.APPLICATION_OCTET_STREAM.value)
        file = self.files_service.create(body=file_metadata,
                                         media_body=media_file,
                                         fields='id').execute()
        LOG.info("File ID: %s", file.get('id'))

    def _remove_file_if_exists(self, existing_files, name_of_file, parent_folder_id):
        if len(existing_files) > 0:
            for file in existing_files:
                # File exists, remove it as one Google drive folder can have multiple files with the same name!
                request = self.files_service.delete(fileId=file.id)
                response = request.execute()
                print(response)

    def create_folder_structure(self, path: str) -> List[Tuple[str, str]]:
        folders = path.split(os.sep)
        folders = [f for f in folders if f]
        structure: List[Tuple[str, str]] = []
        for folder_name in folders:
            if len(structure) > 0:
                parent_id = structure[-1][0]
            else:
                parent_id = None
            folder_id: str = self.create_folder(folder_name, parent_id=parent_id)
            structure.append((folder_id, folder_name))
        return structure

    def create_folder(self, name: str, parent_id) -> str:
        folders: List[DriveApiFile] = self.get_files(name, mime_type=DriveApiMimeType.FOLDER)
        if not folders:
            file_metadata = {
                'name': name,
                'mimeType': DriveApiMimeType.FOLDER.value
            }
            if parent_id:
                file_metadata['parents'] = [parent_id]
            new_folder = self.files_service.create(body=file_metadata, fields='id').execute()
            LOG.info('Folder ID: %s', new_folder.get('id'))
            return new_folder.get('id')
        elif len(folders) == 1:
            return folders[0].id
        else:
            raise ValueError("Expected to find one folder with name '{}', "
                             "but found multiple. Results: {}".format(name, folders))

    @classmethod
    def convert_mime_type(cls, mime_type):
        if mime_type in DriveApiMimeTypes.MIME_MAPPINGS:
            return DriveApiMimeTypes.MIME_MAPPINGS[mime_type]
        else:
            LOG.warning("MIME type not found among possible values: %s. Using MIME type value as is", mime_type)
            return mime_type

    @classmethod
    def _convert_to_drive_file_object(cls, item) -> DriveApiFile:
        list_of_owners_dicts = item['owners']
        owners = [DriveApiUser(owner_dict) for owner_dict in list_of_owners_dicts]

        unknown_user = {GenericUserField.EMAIL_ADDRESS: GenericUserField.UNKNOWN_USER,
                        GenericUserField.DISPLAY_NAME: GenericUserField.UNKNOWN_USER}
        sharing_user_dict = item[FileField.SHARING_USER] if FileField.SHARING_USER in item else unknown_user
        sharing_user = DriveApiUser(sharing_user_dict)

        return DriveApiFile(DriveApiWrapper._safe_get(item, FileField.ID),
                            DriveApiWrapper._safe_get(item, FileField.NAME),
                            DriveApiWrapper._safe_get(item, FileField.LINK),
                            DriveApiWrapper._safe_get(item, FileField.CREATED_TIME),
                            DriveApiWrapper._safe_get(item, FileField.MODIFIED_TIME),
                            DriveApiWrapper._safe_get(item, FileField.SHARED_WITH_ME_TIME),
                            DriveApiWrapper._safe_get(item, FileField.MIMETYPE),
                            owners,
                            sharing_user)

    @staticmethod
    def _safe_get(d: Dict[str, str], key:str):
        if key not in d:
            return None
        return d[key]
