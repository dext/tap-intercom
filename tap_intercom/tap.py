"""Intercom tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
import typing as t

import os

from tap_intercom.streams import (
    ReportExportStream,
    ContentExportStream,
    ConversationsStream,
    ConversationPartsStream,
    CollectionsStream,
    ContactsListStream,
    ContactsStream,
    AdminsStream,
    ArticlesStream,
    EventsStream,
    TagsStream,
    TeamsStream,
    TicketsListStream,
    TicketsStream,
    SegmentsStream,
)

STREAMS_DCT = {
    "conversations": ConversationsStream,
    "conversation_parts": ConversationPartsStream,
    "collections": CollectionsStream,
    "contacts_list": ContactsListStream,
    "contacts": ContactsStream,
    "admins": AdminsStream,
    "articles": ArticlesStream,
    "events": EventsStream,
    "tags": TagsStream,
    "teams": TeamsStream,
    "tickets_list": TicketsListStream,
    "tickets": TicketsStream,
    "segments": SegmentsStream,
}

CONTENT_EXPORT_STREAMS = {
    "answer": ContentExportStream,
    "answer_combined": ContentExportStream,
    "checkpoint": ContentExportStream,
    "click": ContentExportStream,
    "completion": ContentExportStream,
    "dismissal": ContentExportStream,
    "open": ContentExportStream,
    "overview": ContentExportStream,
    "receipt": ContentExportStream,
    "reply": ContentExportStream,
    "series_completion": ContentExportStream,
    "series_disengagement": ContentExportStream,
    "tour_step_view": ContentExportStream,
}
REPORTING_EXPORT_STREAMS = {
    "admin_status_change": ReportExportStream,
    # "call": ReportExportStream,
    # "call_team_stats": ReportExportStream,
    # "call_teammate_stats": ReportExportStream,
    "consolidated_conversation_part": ReportExportStream,
    "conversation": ReportExportStream,
    "conversation_rating_sent": ReportExportStream,
    "conversation_sla_status_log": ReportExportStream,
    "conversation_state": ReportExportStream,
    "copilot_prompt_response_pair": ReportExportStream,
    "teammate_handling_conversation": ReportExportStream,
    "ticket_time_in_state": ReportExportStream,
    "tickets_report": ReportExportStream,
}

REPLICATION_KEY_MAPPING = {
    "conversations": "updated_at",
    "conversation_parts": "updated_at",
    "contacts_list": None,
    "contacts": "updated_at",
    "collections": "updated_at",
    "events": None,
    "admins": None,
    "articles": "updated_at",
    "tags": None,
    "teams": None,
    "tickets_list": None,
    "tickets": "updated_at",
    "segments": None,
}
class TapIntercom(Tap):
    """Intercom tap class."""

    name = "tap-intercom"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            required=True,
            secret=True,
            description="The key to authenticate against the API service",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "end_date",
            th.DateTimeType,
            description="The latest record date to sync",
        ),
        th.Property(
            "base_url",
            th.StringType,
            default="https://api.intercom.io",
            description="The base URL for the Intercom API",
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.IntercomStream]:
        """Returns:
            A list of discovered streams.
        """

        streams = [
            ContentExportStream(name="answer", primary_keys=[], replication_key="answered_at", tap=self),
            ContentExportStream(name="answer_combined", primary_keys=[], replication_key="completed_at", tap=self),
            ContentExportStream(name="checkpoint", primary_keys=[], replication_key="created_at", tap=self),
            ContentExportStream(name="click", primary_keys=[], replication_key="clicked_at", tap=self),
            ContentExportStream(name="completion", primary_keys=[], replication_key="completed_at", tap=self),
            ContentExportStream(name="dismissal", primary_keys=[], replication_key="dismissed_at", tap=self),
            ContentExportStream(name="open", primary_keys=[], replication_key="opened_at", tap=self),
            ContentExportStream(name="overview", primary_keys=[], replication_key="created_at", tap=self),
            ContentExportStream(name="receipt", primary_keys=[], replication_key="received_at", tap=self),
            ContentExportStream(name="reply", primary_keys=[], replication_key="replied_at", tap=self),
            ContentExportStream(name="series_completion", primary_keys=[], replication_key="completed_at", tap=self),
            ContentExportStream(name="series_disengagement", primary_keys=[], replication_key="disengaged_at", tap=self),
            ContentExportStream(name="tour_step_view", primary_keys=[], replication_key="viewed_at", tap=self),

            ReportExportStream(name="admin_status_change", primary_keys=[], replication_key="teammate_status_period_started_at", tap=self),
            # ReportExportStream(name="call", primary_keys=[], replication_key="completed_at", tap=self),
            # ReportExportStream(name="call_team_stats", primary_keys=[], replication_key="created_at", tap=self),
            # ReportExportStream(name="call_teammate_stats", primary_keys=[], replication_key="clicked_at", tap=self),
            ReportExportStream(name="consolidated_conversation_part", primary_keys=[], replication_key="action_time", tap=self),
            ReportExportStream(name="conversation", primary_keys=[], replication_key="conversation_started_at", tap=self),
            ReportExportStream(name="conversation_rating_sent", primary_keys=[], replication_key="conversation_rating_updated_at", tap=self),
            ReportExportStream(name="conversation_sla_status_log", primary_keys=[], replication_key="sla_started_at", tap=self),
            ReportExportStream(name="conversation_state", primary_keys=[], replication_key="conversation_state_started_at", tap=self),
            ReportExportStream(name="copilot_prompt_response_pair", primary_keys=[], replication_key="replied_at", tap=self),
            ReportExportStream(name="teammate_handling_conversation", primary_keys=[], replication_key="conversation_started_at", tap=self),
            ReportExportStream(name="ticket_time_in_state", primary_keys=[], replication_key="ticket_created_at", tap=self),
            ReportExportStream(name="tickets_report", primary_keys=[], replication_key="ticket_created_at", tap=self),
        ]


        primary_keys = {}

        if "primary_keys" in self.config:
            primary_keys = self.config.get("primary_keys")

        for stream_name in STREAMS_DCT.keys():
            stream_class = STREAMS_DCT[stream_name]
            replication_key = REPLICATION_KEY_MAPPING[stream_name]

            pk = []

            if (len(primary_keys) > 0) and (stream_name in primary_keys):
                pk = primary_keys[stream_name]

            stream = stream_class(tap=self, name=stream_name, primary_keys=pk, replication_key=replication_key)
            streams.append(stream)

        return streams

    @t.final
    def sync_all(self) -> None:
        super().sync_all()
        os.system("rm -rf /tmp/intercom_data")


if __name__ == "__main__":
    TapIntercom.cli()

