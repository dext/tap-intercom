"""Intercom tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_intercom.streams import ContentExportStream

import json

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
        schemas = {
            "answer": self.load_schema("answer.json"),
            "answer_combined": self.load_schema("answer_combined.json"),
            "completion": self.load_schema("completion.json"),
            "overview": self.load_schema("overview.json"),
            "receipt": self.load_schema("receipt.json"),
            "reply": self.load_schema("reply.json"),
        }
        streams = [
            ContentExportStream(name="answer", schema=schemas["answer"], tap=self),
            ContentExportStream(name="answer_combined", schema=schemas["answer_combined"], tap=self),
            ContentExportStream(name="completion", schema=schemas["completion"], tap=self),
            ContentExportStream(name="overview", schema=schemas["overview"], tap=self),
            ContentExportStream(name="receipt", schema=schemas["receipt"], tap=self),
            ContentExportStream(name="reply", schema=schemas["reply"], tap=self),
        ]
        return streams

    def load_schema(self, stream_name: str) -> dict:
        """Load the schema for a given stream."""
        with open(f'/Users/daniela.angelova/projects/tap-intercom/tap_intercom/schemas/{stream_name}', 'r') as file:
            return json.load(file)


if __name__ == "__main__":
    TapIntercom.cli()
