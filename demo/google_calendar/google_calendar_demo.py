import datetime
import logging
import sys
from os.path import expanduser

from googleapiwrapper.common import ServiceType
from googleapiwrapper.google_auth import GoogleApiAuthorizer
from googleapiwrapper.google_calendar import CalendarApiWrapper
from pythoncommons.file_utils import FileUtils


def main():
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    SECRET_PROJECTS_DIR = FileUtils.join_path(expanduser("~"), ".secret", "projects", "cloudera")
    authorizer = GoogleApiAuthorizer(
        ServiceType.CALENDAR,
        project_name="calendarutils",
        secret_basedir=SECRET_PROJECTS_DIR,
        account_email="snemeth@cloudera.com",
        scopes=ServiceType.CALENDAR.default_scopes,
    )
    wrapper = CalendarApiWrapper(authorizer)
    wrapper.print_events()



if __name__ == "__main__":
    main()
