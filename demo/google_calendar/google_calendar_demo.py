import datetime
from os.path import expanduser

from googleapiwrapper.common import ServiceType
from googleapiwrapper.google_auth import GoogleApiAuthorizer
from googleapiwrapper.google_calendar import CalendarApiWrapper
from pythoncommons.file_utils import FileUtils


def main():
    SECRET_PROJECTS_DIR = FileUtils.join_path(expanduser("~"), ".secret", "projects", "cloudera")
    authorizer = GoogleApiAuthorizer(
        ServiceType.CALENDAR,
        project_name="calendarutils",
        secret_basedir=SECRET_PROJECTS_DIR,
        account_email="snemeth@cloudera.com",
        scopes=ServiceType.CALENDAR.default_scopes,
    )
    wrapper = CalendarApiWrapper(authorizer)

    list_events(wrapper)


def list_events(wrapper):
    """Shows basic usage of the Google Calendar API.
    Prints the start and name of the next 10 events on the user's calendar.
    """
    # Call the Calendar API
    now = datetime.datetime.utcnow().isoformat() + "Z"  # 'Z' indicates UTC time
    print("Getting the upcoming 100 events")
    events = wrapper.list_events(min_time=now, max_results=100)
    # Prints the start and name of the next 10 events
    for event in events:
        start = event["start"].get("dateTime", event["start"].get("date"))
        print(start, event["summary"])


if __name__ == "__main__":
    main()
