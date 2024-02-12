from enum import Enum


class ServiceType(Enum):
    DRIVE = ("drive", ["https://www.googleapis.com/auth/drive.metadata.readonly"], "v3")
    GMAIL = ("gmail", ["https://www.googleapis.com/auth/gmail.readonly"], "v1")
    SHEETS = ("sheets", ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"], "v3")
    CALENDAR = ("calendar", ["https://www.googleapis.com/auth/calendar.readonly"], "v3")
    CALENDAR_WRITE = ("calendar", ["https://www.googleapis.com/auth/calendar"], "v3")

    def __init__(self, name, scopes, api_version):
        self.service_name = name
        self.default_scopes = scopes
        self.default_api_version = api_version
