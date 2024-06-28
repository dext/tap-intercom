"""Intercom tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
import typing as t

from tap_intercom import streams

import os
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

        return [
            streams.ContentExportStream(name="answer", replication_key="answered_at", tap=self),
            streams.ContentExportStream(name="answer_combined", replication_key="completed_at", tap=self),
            streams.ContentExportStream(name="checkpoint", replication_key="created_at", tap=self),
            streams.ContentExportStream(name="click", replication_key="clicked_at", tap=self),
            streams.ContentExportStream(name="completion", replication_key="completed_at", tap=self),
            streams.ContentExportStream(name="dismissal", replication_key="dismissed_at", tap=self),
            streams.ContentExportStream(name="open", replication_key="opened_at", tap=self),
            streams.ContentExportStream(name="overview", replication_key="created_at", tap=self),
            streams.ContentExportStream(name="receipt", replication_key="received_at", tap=self),
            streams.ContentExportStream(name="reply", replication_key="replied_at", tap=self),
            streams.ContentExportStream(name="series_completion", replication_key="completed_at", tap=self),
            streams.ContentExportStream(name="series_disengagement", replication_key="disengaged_at", tap=self),
            streams.ContentExportStream(name="tour_step_view", replication_key="viewed_at", tap=self),
            streams.ConversationsStream(self),
            streams.ConversationPartsStream(self),
            streams.CollectionsStream(self),
            streams.ContactsListStream(self),
            streams.ContactsStream(self),
            streams.AdminsStream(self),
            streams.ArticlesStream(self),
            streams.EventsStream(self),
            streams.TagsStream(self),
            streams.TeamsStream(self),
            streams.TicketsListStream(self),
            streams.TicketsStream(self),
            streams.SegmentsStream(self),
        ]


    @t.final
    def sync_all(self) -> None:
        super().sync_all()
        os.system("rm -rf /tmp/intercom_data")


if __name__ == "__main__":
    TapIntercom.cli()

