"""Intercom tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
import typing as t

import os

from tap_intercom.streams import (
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

