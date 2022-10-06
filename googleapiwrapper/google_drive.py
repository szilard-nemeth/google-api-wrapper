import logging
import os
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict, Any, Tuple

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from pythoncommons.file_utils import FileUtils
from pythoncommons.object_utils import ObjUtils
from pythoncommons.string_utils import auto_str, StringUtils

from googleapiwrapper.google_auth import GoogleApiAuthorizer

LOG = logging.getLogger(__name__)
OP_SETTINGS_ARG_NAME = "op_settings"


def capture_single_operation_settings(func):
    def wrapper(*args, **kwargs):
        arg0 = args[0]
        self = None
        if isinstance(arg0, DriveApiWrapper):
            self = arg0

        op_settings = None
        if OP_SETTINGS_ARG_NAME in kwargs:
            op_settings = kwargs[OP_SETTINGS_ARG_NAME]

        # Try to find operation settings by type if name does not match
        for kwarg in kwargs:
            if isinstance(kwarg, DriveApiWrapperSingleOperationSettings):
                LOG.warning(
                    "Found single operation settings by type. Was expecting it by running with name '%s'",
                    OP_SETTINGS_ARG_NAME,
                )
                op_settings = kwarg

        if self and op_settings:
            LOG.info("Setting single operation settings to self: %s", op_settings)
            self.current_op_settings = op_settings

        # Evaluate final settings --> All methods decorated should work just with self.final_settings alone
        if self:
            self.final_settings = self._evaluate_to_final_settings()

        ret = func(*args, **kwargs)

        # Post-call operations
        self.current_op_settings = None

        return ret

    return wrapper


class DriveApiOperationType(Enum):
    QUERY = "QUERY"
    UPLOAD_OR_CREATE = "UPLOAD_OR_CREATE"


class DriveApiScope(Enum):
    # https://developers.google.com/drive/api/v2/about-auth
    DRIVE_PER_FILE_ACCESS = "https://www.googleapis.com/auth/drive.file"


class DriveApiMimeType(Enum):
    FILE = "application/vnd.google-apps.file"
    FOLDER = "application/vnd.google-apps.folder"


class NormalMimeType(Enum):
    APPLICATION_OCTET_STREAM = "application/octet-stream"
    APPLICATION_JSON = "application/json"


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

    EXT_TO_MIME_MAPPINGS = {"json": NormalMimeType.APPLICATION_JSON}

    @classmethod
    def get_mime_type_by_filename(cls, filename):
        ext = FileUtils.get_file_extension(filename)
        if ext in cls.EXT_TO_MIME_MAPPINGS:
            return cls.EXT_TO_MIME_MAPPINGS[ext]
        # Fallback to default MIME type
        return NormalMimeType.APPLICATION_OCTET_STREAM


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
    PARENTS = "parents"

    _ALL_FIELDS_WITH_DISPLAY_NAME = [
        (ID, "ID"),
        (NAME, "Name"),
        (MIMETYPE, "Type"),
        (LINK, "Link"),
        (CREATED_TIME, "Created date"),
        (MODIFIED_TIME, "Last modified time"),
        (SHARED_WITH_ME_TIME, "Shared with me date"),
        (F_OWNER, "Owner"),
        (PARENTS, "Parents"),
    ]

    PRINTABLE_FIELD_DISPLAY_NAMES = [
        "Name",
        "Link",
        "Shared with me date",
        "Owner",
        "Type",
    ]
    # FIELDS_TO_PRINT = [tup[0] for tup in FIELDS_TO_PRINT]

    BASIC_FIELDS_COMMA_SEPARATED = ", ".join([ID, NAME, F_OWNER])
    GOOGLE_API_FIELDS = [tup[0] for tup in _ALL_FIELDS_WITH_DISPLAY_NAME]
    GOOGLE_API_FIELDS_COMMA_SEPARATED = ", ".join(GOOGLE_API_FIELDS)
    FIELD_DISPLAY_NAMES = [tup[1] for tup in _ALL_FIELDS_WITH_DISPLAY_NAME]


class GenericUserField:
    UNKNOWN_USER = "unknown"
    EMAIL_ADDRESS = "emailAddress"
    DISPLAY_NAME = "displayName"


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
class DriveApiFile:
    def __init__(
        self,
        id,
        name,
        link,
        created_date,
        modified_date,
        shared_with_me_date,
        mime_type,
        owners,
        sharing_user: DriveApiUser or None,
        parents,
        parent: Any = None,
    ):
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
        self.parents = parents
        self._parent = parent

    @staticmethod
    def create_root_api_file():
        return DriveApiFile("N/A", "/", "N/A", "N/A", "N/A", "N/A", DriveApiMimeType.FOLDER, "N/A", None, None)

    def __repr__(self):
        return self.__str__()


class DuplicateFileWriteResolutionMode(Enum):
    REMOVE_AND_CREATE = "REMOVE_AND_CREATE"
    ADD_NEW_REVISION = "ADD_NEW_REVISION"
    FAIL_FAST = "FAIL_FAST"


class SearchResultHandlingMode(Enum):
    SINGLE_FILE_PER_SEARCH_RESULT = "SINGLE_FILE_PER_SEARCH_RESULT"
    ALLOW_MULTIPLE_SEARCH_RESULTS_FOR_FILE = "ALLOW_MULTIPLE_SEARCH_RESULTS"


class FileFindMode(Enum):
    ALL = "ALL"
    JUST_UNTRASHED = "JUST_UNTRASHED"


@dataclass
class DriveApiWrapperSessionSettings:
    # TODO Make this more simple: Classes DriveApiWrapperSingleOperationSettings & _DriveApiWrapperFinalSettings are having the same set of fields --> error prone!
    file_find_mode: FileFindMode
    duplicate_file_handling_mode: DuplicateFileWriteResolutionMode = DuplicateFileWriteResolutionMode.ADD_NEW_REVISION
    search_result_handling_mode: SearchResultHandlingMode or None = (
        SearchResultHandlingMode.SINGLE_FILE_PER_SEARCH_RESULT
    )
    # TODO unused
    enable_path_cache: bool = True
    # TODO Add option: Just find files/dris created by Google Api Wrapper


@dataclass
class DriveApiWrapperSingleOperationSettings:
    file_find_mode: FileFindMode or None
    duplicate_file_handling_mode: DuplicateFileWriteResolutionMode or None = (
        DuplicateFileWriteResolutionMode.ADD_NEW_REVISION
    )
    search_result_handling_mode: SearchResultHandlingMode or None = (
        SearchResultHandlingMode.SINGLE_FILE_PER_SEARCH_RESULT
    )


@dataclass
class _DriveApiWrapperFinalSettings:
    file_find_mode: FileFindMode
    duplicate_file_handling_mode: DuplicateFileWriteResolutionMode
    search_result_handling_mode: SearchResultHandlingMode


class DriveFileStructure:
    def __init__(self, initial_path: str):
        if not initial_path:
            raise ValueError("Empty path")

        comps = initial_path.split(os.sep)
        if len(comps) == 1:
            # One component pathname is just a filename, assuming parent = "root"
            parent = DriveApiFile.create_root_api_file()
            self.add_file(parent)

        path_folders = initial_path.split(os.sep)
        self.path_folders = self._sanitize(path_folders)
        self.drive_api_files: List[DriveApiFile] = []
        self._index = 0
        self._count = len(self.path_folders)

    @staticmethod
    def _sanitize(path_folders):
        return [f for f in path_folders if f]

    def __len__(self):
        return len(self.drive_api_files)

    def __iter__(self):
        return self

    def __next__(self):
        if self._index == self._count:
            raise StopIteration
        result = self.path_folders[self._index]
        self._index += 1
        return result

    def get_last_file_or_dir(self) -> DriveApiFile or None:
        if len(self.drive_api_files) > 0:
            return self.drive_api_files[-1]
        return None

    def has_any_file(self):
        return len(self.drive_api_files) > 0

    def add_file(self, drive_api_file: DriveApiFile):
        self.drive_api_files.append(drive_api_file)


class DriveApiWrapper:
    DEFAULT_API_VERSION = "v3"
    DEFAULT_ORDER_BY = "sharedWithMeTime desc"
    QUERY_SHARED_WITH_ME = "sharedWithMe"
    DEFAULT_PAGE_SIZE = 100
    DRIVE_API_FILE_CACHE: Dict[str, DriveApiFile] = {}
    DRIVE_API_FILE_CACHE_BY_ID: Dict[str, DriveApiFile] = {}

    def __init__(
        self,
        authorizer: GoogleApiAuthorizer,
        api_version: str = None,
        session_settings: DriveApiWrapperSessionSettings = None,
    ):
        self.authed_session = authorizer.authorize()
        if not api_version:
            api_version = authorizer.service_type.default_api_version
        self.service = self._build_service(api_version, authorizer)
        self.files_service = self.service.files()
        self.session_settings: DriveApiWrapperSessionSettings or None = session_settings
        self.current_op_settings: DriveApiWrapperSingleOperationSettings or None = None
        self.final_settings: _DriveApiWrapperFinalSettings or None = None

    def _build_service(self, api_version, authorizer):
        return build(
            authorizer.service_type.service_name,
            api_version,
            credentials=self.authed_session.authed_creds,
        )

    def _evaluate_to_final_settings(self) -> _DriveApiWrapperFinalSettings:
        LOG.debug("Current session settings: %s", self.session_settings)
        LOG.debug("Current single operation settings: %s", self.current_op_settings)
        file_find_mode = self._get_setting_property("file_find_mode")
        duplicate_file_handling_mode = self._get_setting_property("duplicate_file_handling_mode")
        search_result_handling_mode = self._get_setting_property("search_result_handling_mode")
        return _DriveApiWrapperFinalSettings(file_find_mode, duplicate_file_handling_mode, search_result_handling_mode)

    def _get_setting_property(self, prop):
        current_obj = self.session_settings
        if current_obj:
            if not hasattr(current_obj, prop):
                raise ValueError(
                    "Expected to have property called '{}' on session settings object: {}. "
                    "Current properties: {}".format(prop, current_obj, ObjUtils.get_properties(current_obj))
                )
        current_obj = self.current_op_settings
        if current_obj:
            if not hasattr(current_obj, prop):
                raise ValueError(
                    "Expected to have property called '{}' on current single operation settings object: {}. "
                    "Current properties: {}".format(prop, current_obj, ObjUtils.get_properties(current_obj))
                )

        current_op_value = None
        if self.current_op_settings:
            current_op_value = getattr(self.current_op_settings, prop)

        session_value = getattr(self.session_settings, prop)
        LOG.debug(
            "Value of property '%s'. " "Current single operation value: %s, " "Session value: %s",
            prop,
            current_op_value,
            session_value,
        )
        if current_op_value:
            return current_op_value
        return session_value

    def _add_dir_to_cache(self, dir_name, drive_file: DriveApiFile):
        # TODO consider adding not only dir names as key but with dir name + google drive path?
        if not drive_file:
            raise ValueError("Cannot add None object to cache! Dir name was: {}".format(dir_name))
        LOG.debug("Adding dir '%s' to cache, drive file: '%s'", dir_name, drive_file)
        self.DRIVE_API_FILE_CACHE[dir_name] = drive_file
        self.DRIVE_API_FILE_CACHE_BY_ID[drive_file.id] = drive_file

    def _load_dir_from_cache(self, dir_name) -> DriveApiFile or None:
        LOG.debug("Loading dir '%s' from cache", dir_name)
        if dir_name in self.DRIVE_API_FILE_CACHE:
            return self.DRIVE_API_FILE_CACHE[dir_name]
        return None

    def _load_dir_from_cache_by_id(self, id: str) -> DriveApiFile or None:
        LOG.debug("Loading dir from cache by id: %s", id)
        if id in self.DRIVE_API_FILE_CACHE_BY_ID:
            return self.DRIVE_API_FILE_CACHE_BY_ID[id]
        return None

    @staticmethod
    def get_field_names_with_pagination(fields, resource_type="files"):
        fields_str = DriveApiWrapper.get_field_names(fields, resource_type=resource_type, include_resource=True)
        return "{}, {}".format(GenericApiField.PAGING_NEXT_PAGE_TOKEN, fields_str)

    @staticmethod
    def get_field_names(fields, resource_type="files", include_resource=False):
        # File fields are documented here: https://developers.google.com/drive/api/v3/reference/files#resource
        if include_resource:
            return f"{resource_type}({fields})"
        return f"{fields}"

    def print_shared_files(self, page_size=DEFAULT_PAGE_SIZE, fields=None, order_by=DEFAULT_ORDER_BY):
        files = self.get_shared_files(page_size=page_size, fields=fields, order_by=order_by)
        for file in files:
            LOG.info("{0} ({1})".format(file[FileField.NAME], file[FileField.ID]))

    def get_shared_files(
        self,
        page_size=DEFAULT_PAGE_SIZE,
        fields: List[str] = None,
        order_by: str = DEFAULT_ORDER_BY,
    ):
        fields_str = self._get_field_names(fields, operation_type=DriveApiOperationType.QUERY)
        return self.list_files_with_paging(self.QUERY_SHARED_WITH_ME, page_size, fields_str, order_by)

    @staticmethod
    def _get_field_names(fields, operation_type):
        if not fields:
            fields = DriveApiWrapper._get_default_fields()
        if operation_type == DriveApiOperationType.QUERY:
            return DriveApiWrapper.get_field_names_with_pagination(fields)
        elif operation_type == DriveApiOperationType.UPLOAD_OR_CREATE:
            return DriveApiWrapper.get_field_names(fields)

    @staticmethod
    def _get_default_fields():
        return FileField.GOOGLE_API_FIELDS_COMMA_SEPARATED

    def get_file(self, drive_path):
        return self._get_file_internal(drive_path)

    def get_files(self, expression, owner=None):
        mimetype = DriveApiMimeTypes.get_mime_type_by_filename(expression)
        return self._get_files(expression, mimetype=mimetype, resolve_parents=True)

    def _get_file_by_id(self, id: str):
        return self.files_service.get(fileId=id).execute()

    @capture_single_operation_settings
    def _get_files(
        self,
        filename: str,
        mimetype: Enum = DriveApiMimeType.FILE,
        page_size=DEFAULT_PAGE_SIZE,
        fields: List[str] = None,
        parent: DriveApiFile = None,
        order_by: str = DEFAULT_ORDER_BY,
        resolve_parents: bool = False,
    ) -> List[DriveApiFile]:
        fields_str = self._get_field_names(fields, operation_type=DriveApiOperationType.QUERY)
        query: str = f"mimeType = '{mimetype.value}'"

        if "*" in filename:
            filename = filename.replace("*", "")
            query += f" and name contains '{filename}'"
        else:
            query += f" and name = '{filename}'"

        if parent:
            query += f" and '{parent.id}' in parents"
        if self.final_settings.file_find_mode == FileFindMode.JUST_UNTRASHED:
            query += " and trashed != true"
        return self.list_files_with_paging(query, page_size, fields_str, order_by, resolve_parents=resolve_parents)

    def _get_file_internal(self, drive_path) -> List[DriveApiFile]:
        dirnames, filename = self._validate_upload_file_candidate(drive_path)
        structure: DriveFileStructure = self._verify_all_dirs_exist(dirnames)
        if len(structure) != len(dirnames):
            return []

        existing_files: List[DriveApiFile] = self._get_files(
            filename,
            mimetype=DriveApiMimeTypes.get_mime_type_by_filename(filename),
            parent=structure.get_last_file_or_dir(),
        )
        return existing_files

    def list_files_with_paging(
        self, query, page_size, fields, order_by, resolve_parents: bool = False
    ) -> List[DriveApiFile]:
        result_files = []
        LOG.info(
            "Listing files with query: %s. Page size: %s, Fields: %s, order by: %s",
            query,
            page_size,
            fields,
            order_by,
        )
        request = self.files_service.list(q=query, pageSize=page_size, fields=fields, orderBy=order_by)
        while request is not None:
            files_doc = request.execute()
            if files_doc:
                api_file_results = files_doc.get("files", [])
                drive_api_files: List[DriveApiFile] = [
                    DriveApiWrapper._convert_to_drive_file_object(i) for i in api_file_results
                ]
                if resolve_parents:
                    for drive_api_file in drive_api_files:
                        if drive_api_file.parents:
                            LOG.debug("Resolving parent of DriveApiFile: %s", drive_api_file)
                            parent_id = drive_api_file.parents[0]
                            if parent_id in self.DRIVE_API_FILE_CACHE_BY_ID:
                                drive_api_file._parent = self._load_dir_from_cache_by_id(parent_id)
                                continue
                            get_result = self._get_file_by_id(parent_id)
                            parent_api_file = DriveApiWrapper._convert_to_drive_file_object(get_result)
                            drive_api_file._parent = parent_api_file
                            self._add_dir_to_cache(parent_api_file.name, parent_api_file)
                result_files.extend(drive_api_files)
            else:
                LOG.warning("No files found.")
            request = self.files_service.list_next(request, files_doc)

        return result_files

    @capture_single_operation_settings
    def does_file_exist(
        self,
        drive_path: str,
        op_settings: DriveApiWrapperSingleOperationSettings = None,
    ):
        existing_files = self._get_file_internal(drive_path)
        expect_single_result = (
            op_settings.search_result_handling_mode == SearchResultHandlingMode.SINGLE_FILE_PER_SEARCH_RESULT
        )
        if expect_single_result and len(existing_files) > 1:
            raise ValueError(
                "Was expecting just one result file for path: '{}'. Results: {}".format(drive_path, existing_files)
            )
        return True if existing_files else False

    @capture_single_operation_settings
    def upload_file(
        self,
        path_to_local_file: str,
        drive_path: str,
        fields: List[str] = None,
        op_settings: DriveApiWrapperSingleOperationSettings = None,
    ) -> DriveApiFile:
        fields_str = self._get_field_names(fields, operation_type=DriveApiOperationType.UPLOAD_OR_CREATE)
        dirnames, filename = self._validate_upload_file_candidate(drive_path)
        structure: DriveFileStructure = self._prepare_dirs(dirnames)
        parent_drive_file = structure.get_last_file_or_dir()
        parent_id = parent_drive_file.id

        existing_files: List[DriveApiFile] = self._get_files(
            filename,
            mimetype=DriveApiMimeTypes.get_mime_type_by_filename(filename),
            parent=parent_drive_file,
            fields=fields,
        )
        if not existing_files:
            file_metadata = {"name": filename, "parents": [parent_id]}
            return self._upload_and_create_new_file(filename, file_metadata, path_to_local_file, fields=fields_str)

        LOG.info(
            "Found %d files with name '%s' under parent folder: %s.",
            len(existing_files),
            filename,
            parent_drive_file.name,
        )

        dupe_file_handling_mode = self.final_settings.duplicate_file_handling_mode
        if len(existing_files) > 0 and dupe_file_handling_mode == DuplicateFileWriteResolutionMode.FAIL_FAST:
            raise ValueError(
                "Found files when not expected, see logs above! "
                "Duplicate file handling mode is set to: {}".format(dupe_file_handling_mode)
            )

        if len(existing_files) > 1:
            LOG.warning(
                "Falling back to duplicate file handling mode: REMOVE_AND_CREATE",
                len(existing_files),
                parent_drive_file.name,
            )
            dupe_file_handling_mode = DuplicateFileWriteResolutionMode.REMOVE_AND_CREATE

        if dupe_file_handling_mode == DuplicateFileWriteResolutionMode.REMOVE_AND_CREATE:
            file_metadata = {"name": filename, "parents": [parent_id]}
            self._remove_file_if_exists(existing_files, filename, parent_drive_file.id)
            return self._upload_and_create_new_file(filename, file_metadata, path_to_local_file, fields=fields_str)
        elif dupe_file_handling_mode == DuplicateFileWriteResolutionMode.ADD_NEW_REVISION:
            media_file = MediaFileUpload(
                path_to_local_file,
                mimetype=DriveApiMimeTypes.get_mime_type_by_filename(filename).value,
            )
            update_result = self.files_service.update(
                fileId=existing_files[0].id, media_body=media_file, fields=fields_str
            ).execute()
            return self._convert_to_drive_file_object(update_result)

    def _prepare_dirs(
        self,
        dirnames,
        op_settings: DriveApiWrapperSingleOperationSettings = None,
    ) -> DriveFileStructure:
        structure: DriveFileStructure = self._create_folder_structure(os.sep.join(dirnames))
        if len(structure) != len(dirnames):
            raise ValueError(
                "Unexpected error happened. Created folder structure is different than requested dirs!"
                "Folder structure: {}"
                "Requested dirs: {}".format(structure, dirnames)
            )
        return structure

    def _verify_all_dirs_exist(self, dirnames):
        structure: DriveFileStructure = DriveFileStructure(os.sep.join(dirnames))
        for folder_name in structure:
            if structure.has_any_file():
                parent_drive_file = structure.get_last_file_or_dir()
            else:
                parent_drive_file = None
            cached_dir: DriveApiFile = self._load_dir_from_cache(folder_name)
            if cached_dir:
                structure.add_file(cached_dir)
                continue
            found_folders: List[DriveApiFile] = self._get_files(
                folder_name, mimetype=DriveApiMimeType.FOLDER, parent=parent_drive_file
            )
            if not found_folders:
                return structure
            if len(found_folders) > 2:
                raise ValueError(
                    "Expected to find exactly one folder with name '{}', "
                    "but found multiple. Results: {}".format(folder_name, found_folders)
                )
            current_drive_file = found_folders[0]
            structure.add_file(current_drive_file)
            self._add_dir_to_cache(folder_name, current_drive_file)
        return structure

    # TODO Move to DriveFileStructure ?
    @staticmethod
    def _validate_upload_file_candidate(drive_path):
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
            raise ValueError(
                "Invalid Google Drive path. "
                "Expecting a normal file path with at least one component separated by '{}'!"
                "Given file path: {}".format(os.sep, drive_path)
            )
        return dirnames, components[-1]

    def _upload_and_create_new_file(self, filename, file_metadata, path_to_file, fields: str) -> DriveApiFile:
        media_file = MediaFileUpload(path_to_file, mimetype=DriveApiMimeTypes.get_mime_type_by_filename(filename).value)
        file = self.files_service.create(body=file_metadata, media_body=media_file, fields=fields).execute()
        LOG.info("File ID: %s", file.get(FileField.ID))
        return self._convert_to_drive_file_object(file)

    def _remove_file_if_exists(self, existing_files, name_of_file, parent_folder_id):
        if len(existing_files) > 0:
            for file in existing_files:
                # File exists, remove it as one Google drive folder can have multiple files with the same name!
                request = self.files_service.delete(fileId=file.id)
                response = request.execute()
                # TODO
                print(response)

    def _create_folder_structure(self, path: str) -> DriveFileStructure:
        structure: DriveFileStructure = DriveFileStructure(path)
        for folder_name in structure:
            if structure.has_any_file():
                parent_drive_dir = structure.get_last_file_or_dir()
            else:
                parent_drive_dir = None
            cached_drive_dir: DriveApiFile = self._load_dir_from_cache(folder_name)
            if cached_drive_dir:
                structure.add_file(cached_drive_dir)
                continue
            drive_api_file: DriveApiFile = self._create_or_find_folder(folder_name, parent_drive_file=parent_drive_dir)
            drive_api_file._parent = parent_drive_dir
            structure.add_file(drive_api_file)
            self._add_dir_to_cache(folder_name, drive_api_file)
        return structure

    def _create_or_find_folder(self, name: str, parent_drive_file: DriveApiFile) -> DriveApiFile:
        fields_str = self._get_field_names(None, DriveApiOperationType.UPLOAD_OR_CREATE)
        folders: List[DriveApiFile] = self._get_files(name, mimetype=DriveApiMimeType.FOLDER)
        if not folders:
            file_metadata = {"name": name, "mimeType": DriveApiMimeType.FOLDER.value}
            if parent_drive_file:
                file_metadata["parents"] = [parent_drive_file.id]
            new_folder = self.files_service.create(body=file_metadata, fields=fields_str).execute()
            LOG.info("Folder ID: %s", new_folder.get(FileField.ID))
            return DriveApiWrapper._convert_to_drive_file_object(new_folder)
        elif len(folders) == 1:
            LOG.debug(
                "Found folder with name: %s, Drive link: %s, ID: %s",
                folders[0].name,
                folders[0].link,
                folders[0].id,
            )
            return folders[0]
        else:
            raise ValueError(
                "Expected to find one folder with name '{}', " "but found multiple. Results: {}".format(name, folders)
            )

    @classmethod
    def convert_mime_type(cls, mime_type):
        if mime_type in DriveApiMimeTypes.MIME_MAPPINGS:
            return DriveApiMimeTypes.MIME_MAPPINGS[mime_type]
        else:
            LOG.warning(
                "MIME type not found among possible values: %s. Using MIME type value as is",
                mime_type,
            )
            return mime_type

    @classmethod
    def _convert_to_drive_file_object(cls, item) -> DriveApiFile:
        LOG.debug("Converting item to %s. Object data: %s", DriveApiFile.__name__, item)
        list_of_owners_dicts = DriveApiWrapper._safe_get(item, FileField.F_OWNER)
        if not list_of_owners_dicts:
            list_of_owners_dicts = {}
        owners = [DriveApiUser(owner_dict) for owner_dict in list_of_owners_dicts]

        unknown_user = {
            GenericUserField.EMAIL_ADDRESS: GenericUserField.UNKNOWN_USER,
            GenericUserField.DISPLAY_NAME: GenericUserField.UNKNOWN_USER,
        }
        sharing_user_dict = item[FileField.SHARING_USER] if FileField.SHARING_USER in item else unknown_user
        sharing_user = DriveApiUser(sharing_user_dict)

        return DriveApiFile(
            DriveApiWrapper._safe_get(item, FileField.ID),
            DriveApiWrapper._safe_get(item, FileField.NAME),
            DriveApiWrapper._safe_get(item, FileField.LINK),
            DriveApiWrapper._safe_get(item, FileField.CREATED_TIME),
            DriveApiWrapper._safe_get(item, FileField.MODIFIED_TIME),
            DriveApiWrapper._safe_get(item, FileField.SHARED_WITH_ME_TIME),
            DriveApiWrapper._safe_get(item, FileField.MIMETYPE),
            owners,
            sharing_user,
            DriveApiWrapper._safe_get(item, FileField.PARENTS),
        )

    @staticmethod
    def _safe_get(d: Dict[str, str], key: str):
        if key not in d:
            return None
        return d[key]
