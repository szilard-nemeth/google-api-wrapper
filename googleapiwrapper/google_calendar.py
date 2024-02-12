import dataclasses
import datetime
import json
import logging
from dataclasses import dataclass
from typing import Optional, Any, Union

from googleapiclient.discovery import build
from googleapiwrapper.common import ServiceType
from googleapiwrapper.google_auth import GoogleApiAuthorizer

LOG = logging.getLogger(__name__)


def get_timezone():
    import datetime

    now = datetime.datetime.now()
    local_now = now.astimezone()
    local_tz = local_now.tzinfo
    local_tzname = local_tz.tzname(local_now)
    return local_tzname


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o, dict_factory=lambda x: {k: v for (k, v) in x if v is not None})
        return super().default(o)


@dataclass
class CalendarDateTime:
    dateTime: str
    timeZone: Optional[str] = None


@dataclass
class CalendarDate:
    date: str


@dataclass
class CalendarEvent:
    """
    event = {
            'summary': 'Google I/O 2015',
            'location': '800 Howard St., San Francisco, CA 94103',
            'description': 'A chance to hear more about Google\'s developer products.',
            'start': {
                'dateTime': '2015-05-28T09:00:00-07:00',
                'timeZone': 'America/Los_Angeles',
            },
            'end': {
                'dateTime': '2015-05-28T17:00:00-07:00',
                'timeZone': 'America/Los_Angeles',
            },
            'recurrence': [
                'RRULE:FREQ=DAILY;COUNT=2'
            ],
            'attendees': [
                {'email': 'lpage@example.com'},
                {'email': 'sbrin@example.com'},
            ],
            'reminders': {
                'useDefault': False,
                'overrides': [
                    {'method': 'email', 'minutes': 24 * 60},
                    {'method': 'popup', 'minutes': 10},
                ],
            },
        }
    """
    summary: str
    description: str
    start: Union[CalendarDateTime, CalendarDate]
    end: Union[CalendarDateTime, CalendarDate]
    location: Optional[str] = None
    recurrence: Optional[Any] = None
    attendees: Optional[Any] = None
    reminders: Optional[Any] = None


class CalendarApiWrapper:
    DEFAULT_API_VERSION = "v3"
    DEFAULT_PAGE_SIZE = 100

    def __init__(
        self,
        authorizer: GoogleApiAuthorizer,
        api_version: str = None,
    ):
        self.authed_session = authorizer.authorize()
        if not api_version:
            api_version = authorizer.service_type.default_api_version
        self.service = self._build_service(api_version, authorizer)
        self.events_service = self.service.events()

    def _build_service(self, api_version, authorizer):
        return build(
            authorizer.service_type.service_name,
            api_version,
            credentials=self.authed_session.authed_creds,
        )

    def list_events(self, min_time, max_results=100, single_events=True, order_by="startTime"):
        events_result = self.events_service.list(
            calendarId="primary",
            timeMin=min_time,
            maxResults=max_results,
            singleEvents=single_events,
            orderBy=order_by,
        ).execute()
        events = events_result.get("items", [])

        if not events:
            LOG.warning("No upcoming events found.")
            return

        return events

    def print_events(self, max_results=100):
        """Shows basic usage of the Google Calendar API.
        Prints the start and name of the next 10 events on the user's calendar.
        """
        # Call the Calendar API
        now = datetime.datetime.utcnow().isoformat() + "Z"  # 'Z' indicates UTC time
        LOG.info("Getting the upcoming %d events", max_results)
        events = self.list_events(min_time=now, max_results=max_results)

        # Prints the start and name of the next 10 events
        for event in events:
            start = event["start"].get("dateTime", event["start"].get("date"))
            LOG.info("EVENT: %s %s", start, event["summary"])

    def test_create_event(self):
        # event_obj = CalendarEvent(summary="test summary", description="test description",
        #                           start=CalendarDateTime("2024-02-12T15:00:00-00:00", timeZone=get_timezone()),
        #                           end=CalendarDateTime("2024-02-12T17:00:00-00:00", timeZone=get_timezone()))
        event_obj = CalendarEvent(summary="test summary",
                                  description="test description",
                                  start=CalendarDate("2024-02-12"),
                                  end=CalendarDate("2024-02-13"))

        #event_json = json.dumps(event_obj, cls=EnhancedJSONEncoder)
        #print(event_json)
        event = self.events_service.insert(calendarId='primary', body=dataclasses.asdict(event_obj)).execute()
        LOG.info('Event created: %s' % (event.get('htmlLink')))

    def create_all_day_event(self, summary, description, start: CalendarDate, end: CalendarDate):
        event_obj = CalendarEvent(summary=summary,
                                  description=description,
                                  start=start,
                                  end=end)
        event = self.events_service.insert(calendarId='primary', body=dataclasses.asdict(event_obj)).execute()
        LOG.info('Event created: %s' % (event.get('htmlLink')))
