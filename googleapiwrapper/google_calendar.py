import logging

from googleapiclient.discovery import build
from googleapiwrapper.common import ServiceType
from googleapiwrapper.google_auth import GoogleApiAuthorizer

LOG = logging.getLogger(__name__)


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
