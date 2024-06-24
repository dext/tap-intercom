"""Intercom tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_intercom import streams


class TapIntercom(Tap):
    """Intercom tap class."""

    name = "tap-intercom"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "access_token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
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
    ).to_dict()

    def discover_streams(self) -> list[streams.IntercomStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
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


if __name__ == "__main__":
    TapIntercom.cli()
