"""REST client handling, including IntercomStream base class."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Callable, Iterable
import typing
import base64
from urllib.parse import urlencode
from datetime import datetime
from urllib.parse import parse_qsl

T = typing.TypeVar("T")
TPageToken = typing.TypeVar("TPageToken")
_TToken = typing.TypeVar("_TToken")

import requests
from requests import Response
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.pagination import BaseOffsetPaginator

import requests


if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class IntercomStream(RESTStream):
    """Intercom stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://api.intercom.io"

    records_jsonpath = "$[*]"  # Or override `parse_response`.

    @property
    def authenticator(self):
        """Return the authenticator."""
        return BearerTokenAuthenticator.create_for_stream(self, token=self.config.get("access_token"))

    @property
    def timeout(self):
        return 400

    def prepare_request_payload(
    self,
    context: dict | None,
    next_page_token: Any | None,
    ) -> dict | None:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Developers may override this method if the API requires a custom payload along
        with the request. (This is generally not required for APIs which use the
        HTTP 'GET' method.)

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.
        """
        if self.rest_method == "POST":
            start_date = self.config.get("start_date")
            if start_date:
                if type(start_date) == str:
                    start_date = int(datetime.timestamp(datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")))
                    self.logger.info(f"start_date: {start_date}")
            end_date = self.config.get("end_date")
            if end_date:
                if type(end_date) == str:
                    end_date = int(datetime.timestamp(datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")))
                    self.logger.info(f"end_date: {end_date}")
            payload = {
                    "created_at_after": start_date,
                    "created_at_before": end_date
                    }
            return payload
        return None

