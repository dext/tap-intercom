"""Intercom tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers
import typing as t

from tap_intercom.streams import ContentExportStream

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
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """

        streams = [
            ContentExportStream(name="answer", replication_key="answered_at", tap=self),
            ContentExportStream(name="answer_combined", replication_key="completed_at", tap=self),
            ContentExportStream(name="completion", replication_key="completed_at", tap=self),
            ContentExportStream(name="overview", replication_key="created_at", tap=self),
            ContentExportStream(name="receipt", replication_key="recieved_at", tap=self),
            ContentExportStream(name="reply", replication_key="replyed_at", tap=self),
        ]
        return streams


    @t.final
    def sync_all(self) -> None:
        super().sync_all()
        # os.system("rm -rf /tmp/intercom_data")


if __name__ == "__main__":
    TapIntercom.cli()
