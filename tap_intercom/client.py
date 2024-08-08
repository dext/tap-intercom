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


if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]


from singer_sdk.pagination import BaseHATEOASPaginator
class IntercomPaginator(BaseHATEOASPaginator):
    def get_next_url(self, response):
        data = response.json().get("pages", {})

        if data.get("next") is not None:
            if "starting_after" in data.get("next"):
                return data.get("next").get("starting_after")
            return data.get("next")


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


    def get_url_params(self, context, next_page_token):
        params = {}
        if self.rest_method == "GET":
            params = {"per_page": 150}

            if next_page_token:
                page_token = dict(parse_qsl(next_page_token.query))
                if "page" in page_token:
                    params["page"] = page_token["page"]
                else:
                    params["starting_after"] = next_page_token.path
        self.logger.info(50 * "-")
        self.logger.info(f"Params: {params}")
        self.logger.info(50 * "-")
        return params

    def get_new_paginator(self) -> BaseOffsetPaginator:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        return IntercomPaginator()

    def prepare_request_payload(
        self,
        context: dict | None,
        next_page_token: _TToken | None,
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
            body = {"sort": {"field": "updated_at", "order": "ascending"}}
            value = []
            start_date = self.config.get("start_date")
            if start_date:
                if type(start_date) == str:
                    start_date = int(datetime.timestamp(datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")))
                value = [{"field": "updated_at", "operator": ">", "value": start_date}]
            end_date = self.config.get("end_date")
            if end_date:
                if type(end_date) == str:
                    end_date = int(datetime.timestamp(datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")))
                value.append({"field": "updated_at", "operator": "<", "value": end_date})
            body["query"] = {"operator": "AND", "value": value}

            if next_page_token:
                body["pagination"] = {"per_page": 150, "starting_after": next_page_token.path}

            self.logger.info(50 * "-")
            self.logger.info(f"Request body: {body}")
            self.logger.info(50 * "-")
            return body
        else:
            return None

