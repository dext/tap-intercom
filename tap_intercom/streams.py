"""Stream type classes for tap-intercom."""

from __future__ import annotations

from singer_sdk.helpers._util import utc_now

import typing as t
import requests
from pathlib import Path
from typing import Iterable

import singer_sdk
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.streams import Stream
from singer_sdk.typing import (
    IntegerType,
    StringType,
    DateTimeType,
    ObjectType,
    Property,
    PropertiesList,
    ArrayType,
    BooleanType,
)

from tap_intercom.client import IntercomStream

import requests
import time
from datetime import datetime, timedelta
import os
import re

import zipfile




class ConversationsStream(IntercomStream):
    name = "conversations"
    path = "/conversations/search"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    records_jsonpath = "$.conversations[*]"
    rest_method = "POST"

    schema = PropertiesList(
        Property("type", StringType),
        Property("id", StringType),
        Property("ticket", StringType),
        Property("title", StringType),
        Property("created_at", IntegerType),
        Property("updated_at", IntegerType),
        Property("waiting_since", IntegerType),
        Property("snoozed_until", IntegerType),
        Property("open", BooleanType),
        Property("state", StringType),
        Property("read", BooleanType),
        Property("priority", StringType),
        Property("admin_assignee_id", IntegerType),
        Property("team_assignee_id", IntegerType),
        Property(
            "tags",
            ObjectType(
                Property("type", StringType),
                Property(
                    "tags",
                    ArrayType(
                        ObjectType(
                            Property("type", StringType),
                            Property("id", StringType),
                            Property("name", StringType),
                            Property("applied_at", IntegerType),
                            Property(
                                "applied_by",
                                ObjectType(
                                    Property("type", StringType),
                                    Property("id", StringType),
                                ),
                            ),
                        )
                    ),
                ),
            ),
        ),
        Property(
            "conversation_rating",
            ObjectType(
                Property("rating", IntegerType),
                Property("remark", StringType),
                Property("created_at", IntegerType),
                Property(
                    "contact",
                    ObjectType(
                        Property("type", StringType),
                        Property("id", StringType),
                        Property("external_id", StringType),
                    ),
                ),
                Property(
                    "teammate",
                    ObjectType(
                        Property("type", StringType),
                        Property("id", StringType),
                    ),
                ),
            ),
        ),
        Property(
            "source",
            ObjectType(
                Property("type", StringType),
                Property("id", StringType),
                Property("delivered_as", StringType),
                Property("subject", StringType),
                Property("body", StringType),
                Property(
                    "author",
                    ObjectType(
                        Property("type", StringType),
                        Property("id", StringType),
                        Property("name", StringType),
                        Property("email", StringType),
                    ),
                ),
                Property(
                    "attachments",
                    ArrayType(
                        ObjectType(
                            Property("type", StringType),
                            Property("name", StringType),
                            Property("url", StringType),
                            Property("content_type", StringType),
                            Property("filesize", IntegerType),
                            Property("width", StringType),
                            Property("height", StringType),
                        )
                    ),
                ),
                Property("url", StringType),
                Property("redacted", BooleanType),
            ),
        ),
        Property(
            "contacts",
            ObjectType(
                Property("type", StringType),
                Property(
                    "contacts",
                    ArrayType(
                        ObjectType(
                            Property("type", StringType),
                            Property("id", StringType),
                            Property("external_id", StringType),
                        )
                    ),
                ),
            ),
        ),
        Property(
            "teammates",
            ObjectType(
                Property("type", StringType),
                Property(
                    "teammates",
                    ArrayType(
                        ObjectType(
                            Property("type", StringType),
                            Property("id", StringType),
                        )
                    ),
                ),
            ),
        ),
        Property(
            "first_contact_reply",
            ObjectType(
                Property("created_at", IntegerType),
                Property("type", StringType),
                Property("url", StringType),
            ),
        ),
        Property(
            "sla_applied",
            ObjectType(
                Property("type", StringType),
                Property("sla_name", StringType),
                Property("sla_status", StringType),
            ),
        ),
        Property(
            "statistics",
            ObjectType(
                Property("type", StringType),
                Property("time_to_assignment", IntegerType),
                Property("time_to_admin_reply", IntegerType),
                Property("time_to_first_close", IntegerType),
                Property("time_to_last_close", IntegerType),
                Property("median_time_to_reply", IntegerType),
                Property("first_contact_reply_at", IntegerType),
                Property("first_assignment_at", IntegerType),
                Property("first_admin_reply_at", IntegerType),
                Property("first_close_at", IntegerType),
                Property("last_assignment_at", IntegerType),
                Property("last_assignment_admin_reply_at", IntegerType),
                Property("last_contact_reply_at", IntegerType),
                Property("last_admin_reply_at", IntegerType),
                Property("last_close_at", IntegerType),
                Property("last_closed_by_id", IntegerType),
                Property("count_reopens", IntegerType),
                Property("count_assignments", IntegerType),
                Property("count_conversation_parts", IntegerType),
            ),
        ),
        Property(
            "linked_objects",
            ObjectType(
                Property("type", StringType),
                Property("total_count", IntegerType),
                Property("has_more", BooleanType),
                Property(
                    "data",
                    ArrayType(
                        ObjectType(
                            Property("type", StringType),
                            Property("id", StringType),
                            Property("category", StringType),
                        )
                    ),
                ),
            ),
        ),
    ).to_dict()

    def get_child_context(self, record: dict, context: t.Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"conversation_id": record["id"]}


class ConversationPartsStream(IntercomStream):
    name = "conversation_parts"
    parent_stream_type = ConversationsStream
    state_partitioning_keys = []
    path = "/conversations/{conversation_id}"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    records_jsonpath = "$.conversation_parts.conversation_parts[*]"

    schema = PropertiesList(
        Property("type", StringType),
        Property("id", StringType),
        Property("part_type", StringType),
        Property("body", StringType),
        Property("created_at", IntegerType),
        Property("updated_at", IntegerType),
        Property("notified_at", IntegerType),
        Property(
            "assigned_to",
            ObjectType(
                Property("type", StringType),
                Property("id", StringType),
            ),
        ),
        Property(
            "author",
            ObjectType(
                Property("type", StringType),
                Property("id", StringType),
                Property("name", StringType),
                Property("email", StringType),
            ),
        ),
        Property(
            "attachments",
            ArrayType(
                ObjectType(
                    Property("type", StringType),
                    Property("name", StringType),
                    Property("url", StringType),
                    Property("content_type", StringType),
                    Property("filesize", IntegerType),
                    Property("width", StringType),
                    Property("height", StringType),
                )
            ),
        ),
        Property("external_id", StringType),
        Property("redacted", BooleanType),
    ).to_dict()


class ContactsListStream(IntercomStream):
    name = "contacts_list"
    path = "/contacts"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    records_jsonpath = "$.data[*]"

    schema = th.PropertiesList(
        th.Property("type", th.StringType),
        th.Property("id", th.StringType),
        th.Property("external_id", th.StringType),
        th.Property("workspace_id", th.StringType),
        th.Property("role", th.StringType),
        th.Property("email", th.StringType),
        th.Property("email_domain", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("formatted_phone", th.StringType),
        th.Property("name", th.StringType),
        th.Property("owner_id", th.IntegerType),
        th.Property("has_hard_bounced", th.BooleanType),
        th.Property("marked_email_as_spam", th.BooleanType),
        th.Property("unsubscribed_from_emails", th.BooleanType),
        th.Property("created_at", th.IntegerType),
        th.Property("updated_at", th.IntegerType),
        th.Property("signed_up_at", th.StringType),
        th.Property("last_seen_at", th.StringType),
        th.Property("last_replied_at", th.StringType),
        th.Property("last_contacted_at", th.StringType),
        th.Property("last_email_opened_at", th.StringType),
        th.Property("last_email_clicked_at", th.StringType),
        th.Property("language_override", th.StringType),
        th.Property("browser", th.StringType),
        th.Property("browser_version", th.StringType),
        th.Property("browser_language", th.StringType),
        th.Property("os", th.StringType),
        th.Property("android_app_name", th.StringType),
        th.Property("android_app_version", th.StringType),
        th.Property("android_device", th.StringType),
        th.Property("android_os_version", th.StringType),
        th.Property("android_sdk_version", th.StringType),
        th.Property("android_last_seen_at", th.StringType),
        th.Property("ios_app_name", th.StringType),
        th.Property("ios_app_version", th.StringType),
        th.Property("ios_device", th.StringType),
        th.Property("ios_os_version", th.StringType),
        th.Property("ios_sdk_version", th.StringType),
        th.Property("ios_last_seen_at", th.StringType),
        th.Property("custom_attributes", th.StringType),
        th.Property("avatar", th.StringType),
        th.Property("tags", th.ObjectType(
            th.Property("data", th.StringType),
            th.Property("url", th.StringType),
            th.Property("total_count", th.IntegerType),
            th.Property("has_more", th.BooleanType),
            )
        ),
        th.Property("notes", th.ObjectType(
            th.Property("data", th.StringType),
            th.Property("url", th.StringType),
            th.Property("total_count", th.IntegerType),
            th.Property("has_more", th.BooleanType),
            )
        ),
        th.Property("companies", th.ObjectType(
            th.Property("url", th.StringType),
            th.Property("total_count", th.IntegerType),
            th.Property("has_more", th.BooleanType),
            )
        ),
        th.Property("location", th.ObjectType(
            th.Property("type", th.StringType),
            th.Property("country", th.StringType),
            th.Property("region", th.StringType),
            th.Property("city", th.StringType),
            )
        ),
        th.Property("social_profiles", th.ObjectType(
            th.Property("data", th.StringType),
            )
        ),
    ).to_dict()

    def get_child_context(self, record: dict, context: t.Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"contact_id": record["id"]}

class ContactsStream(IntercomStream):
    name = "contacts"
    parent_stream_type = ContactsListStream
    state_partitioning_keys = []
    path = "/contacts/{contact_id}"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"

    schema = th.PropertiesList(
        th.Property("type", th.StringType),
        th.Property("id", th.StringType),
        th.Property("workspace_id", th.StringType),
        th.Property("external_id", th.StringType),
        th.Property("role", th.StringType),
        th.Property("email", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("name", th.StringType),
        th.Property("avatar", th.StringType),
        th.Property("owner_id", th.StringType),
        th.Property("social_profiles", th.ObjectType(
            th.Property("type", th.StringType),
            th.Property("data", th.StringType),
            )
        ),
        th.Property("has_hard_bounced", th.BooleanType),
        th.Property("marked_email_as_spam", th.BooleanType),
        th.Property("unsubscribed_from_emails", th.BooleanType),
        th.Property("created_at", th.IntegerType),
        th.Property("updated_at", th.IntegerType),
        th.Property("signed_up_at", th.IntegerType),
        th.Property("last_seen_at", th.StringType),
        th.Property("last_replied_at", th.StringType),
        th.Property("last_contacted_at", th.StringType),
        th.Property("last_email_opened_at", th.StringType),
        th.Property("last_email_clicked_at", th.StringType),
        th.Property("language_override", th.StringType),
        th.Property("browser", th.StringType),
        th.Property("browser_version", th.StringType),
        th.Property("browser_language", th.StringType),
        th.Property("os", th.StringType),
        th.Property("location", th.ObjectType(
            th.Property("type", th.StringType),
            th.Property("country", th.StringType),
            th.Property("region", th.StringType),
            th.Property("city", th.StringType),
            th.Property("country_code", th.StringType),
            th.Property("continent_code", th.StringType),
            )
        ),
        th.Property("android_app_name", th.StringType),
        th.Property("android_app_version", th.StringType),
        th.Property("android_device", th.StringType),
        th.Property("android_os_version", th.StringType),
        th.Property("android_sdk_version", th.StringType),
        th.Property("android_last_seen_at", th.StringType),
        th.Property("ios_app_name", th.StringType),
        th.Property("ios_app_version", th.StringType),
        th.Property("ios_device", th.StringType),
        th.Property("ios_os_version", th.StringType),
        th.Property("ios_sdk_version", th.StringType),
        th.Property("ios_last_seen_at", th.StringType),
        th.Property("custom_attributes", th.ObjectType()),
        th.Property("tags", th.ObjectType(
            th.Property("type", th.StringType),
            th.Property("data", th.StringType),
            th.Property("url", th.StringType),
            th.Property("total_count", th.IntegerType),
            th.Property("has_more", th.BooleanType),
            )
        ),
        th.Property("notes", th.ObjectType(
            th.Property("type", th.StringType),
            th.Property("data", th.StringType),
            th.Property("url", th.StringType),
            th.Property("total_count", th.IntegerType),
            th.Property("has_more", th.BooleanType),
            )
        ),
        th.Property("companies", th.ObjectType(
            th.Property("type", th.StringType),
            th.Property("data", th.StringType),
            th.Property("url", th.StringType),
            th.Property("total_count", th.IntegerType),
            th.Property("has_more", th.BooleanType),
            )
        ),
        th.Property("opted_out_subscription_types", th.ObjectType(
            th.Property("type", th.StringType),
            th.Property("data", th.StringType),
            th.Property("url", th.StringType),
            th.Property("total_count", th.IntegerType),
            th.Property("has_more", th.BooleanType),
            )
        ),
        th.Property("opted_in_subscription_types", th.ObjectType(
            th.Property("type", th.StringType),
            th.Property("data", th.StringType),
            th.Property("url", th.StringType),
            th.Property("total_count", th.IntegerType),
            th.Property("has_more", th.BooleanType),
            )
        ),
        th.Property("utm_campaign", th.StringType),
        th.Property("utm_content", th.StringType),
        th.Property("utm_medium", th.StringType),
        th.Property("utm_source", th.StringType),
        th.Property("utm_term", th.StringType),
        th.Property("referrer", th.StringType),
    ).to_dict()

class CollectionsStream(IntercomStream):
    name = "collections"
    path = "/help_center/collections"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    records_jsonpath = "$.data[*]"

    schema = PropertiesList(
        Property("id", StringType),
        Property("workspace_id", StringType),
        Property("name", StringType),
        Property("url", StringType),
        Property("order", IntegerType),
        Property("created_at", IntegerType),
        Property("updated_at", IntegerType),
        Property("description", StringType),
        Property("icon", StringType),
        Property("parent_id", StringType),
        Property("help_center_id", IntegerType),
    ).to_dict()


class EventsStream(IntercomStream):
    name = "events"
    path = "/events"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    records_jsonpath = "$.events[*]"

    schema = th.PropertiesList(
        th.Property("type", th.StringType),
        th.Property("events", th.ArrayType(StringType)),
        th.Property("pages", th.ObjectType(
            th.Property("next", th.StringType),
            )
        ),
        th.Property("email", th.StringType),
        th.Property("intercom_user_id", th.StringType),
        th.Property("user_id", th.StringType),
    ).to_dict()

class AdminsStream(IntercomStream):
    name = "admins"
    path = "/admins"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    records_jsonpath = "$.admins[*]"

    schema = PropertiesList(
        Property("type", StringType),
        Property("id", StringType),
        Property("name", StringType),
        Property("email", StringType),
        Property("job_title", StringType),
        Property("away_mode_enabled", BooleanType),
        Property("away_mode_reassign", BooleanType),
        Property("has_inbox_seat", BooleanType),
        Property("team_ids", ArrayType(IntegerType)),
        Property("avatar", StringType),
        Property(
            "team_priority_level",
            ObjectType(
                Property("primary_team_ids", ArrayType(IntegerType)),
                Property("secondary_team_ids", ArrayType(IntegerType)),
            ),
        ),
    ).to_dict()


class ArticlesStream(IntercomStream):
    name = "articles"
    path = "/articles"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    replication_key = "updated_at"
    records_jsonpath = "$.data[*]"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("type", th.StringType),
        th.Property("workspace_id", th.StringType),
        th.Property("parent_id", th.IntegerType),
        th.Property("parent_type", th.StringType),
        th.Property("parent_ids", th.ArrayType(th.StringType)),
        th.Property("title", th.StringType),
        th.Property("description", th.StringType),
        th.Property("body", th.StringType),
        th.Property("author_id", th.IntegerType),
        th.Property("state", th.StringType),
        th.Property("created_at", th.IntegerType),
        th.Property("updated_at", th.IntegerType),
        th.Property("url", th.StringType),
    ).to_dict()


class TagsStream(IntercomStream):
    name = "tags"
    path = "/tags"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    records_jsonpath = "$.data[*]"
    schema = PropertiesList(
        Property("type", StringType),
        Property("id", StringType),
        Property("name", StringType),
        Property("applied_at", IntegerType),
        Property("applied_by", ObjectType(Property("type", StringType), Property("id", StringType))),
    ).to_dict()


class TeamsStream(IntercomStream):
    name = "teams"
    path = "/teams"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    records_jsonpath = "$.teams[*]"
    schema = PropertiesList(
        Property("type", StringType),
        Property("id", StringType),
        Property("name", StringType),
        Property("admin_ids", ArrayType(IntegerType)),
        Property(
            "admin_priority_level",
            ObjectType(
                Property("primary_admin_ids", ArrayType(IntegerType)),
                Property("secondary_admin_ids", ArrayType(IntegerType)),
            ),
        ),
    ).to_dict()


class TicketsListStream(IntercomStream):
    name = "tickets_list"
    path = "/tickets/search"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    rest_method = "POST"
    records_jsonpath = "$.tickets[*]"

    schema = th.PropertiesList(
        th.Property("type", th.StringType),
        th.Property("id", th.StringType),
        th.Property("ticket_id", th.StringType),
        th.Property("category", th.StringType),
        th.Property("ticket_attributes", th.ObjectType(
            th.Property("name", th.StringType),
            th.Property("question", th.StringType),
            )
        ),
        th.Property("ticket_state", th.StringType),
        th.Property("ticket_type", th.ObjectType(
            th.Property("type", th.StringType),
            th.Property("id", th.StringType),
            th.Property("category", th.StringType),
            th.Property("name", th.StringType),
            th.Property("description", th.StringType),
            th.Property("icon", th.StringType),
            th.Property("workspace_id", th.StringType),
            th.Property("ticket_type_attributes", th.StringType),
            th.Property("archived", th.BooleanType),
            th.Property("created_at", th.IntegerType),
            th.Property("updated_at", th.IntegerType),
            )
        ),
        th.Property("contacts", th.ObjectType(
            th.Property("type", th.StringType),
            th.Property("contacts", th.ArrayType(StringType)),
            )
        ),
        th.Property("admin_assignee_id", th.StringType),
        th.Property("team_assignee_id", th.StringType),
        th.Property("created_at", th.IntegerType),
        th.Property("updated_at", th.IntegerType),
        th.Property("open", th.BooleanType),
        th.Property("snoozed_until", th.IntegerType),
        th.Property("linked_objects", th.ObjectType(
            th.Property("type", th.StringType),
            th.Property("total_count", th.IntegerType),
            th.Property("has_more", th.BooleanType),
            th.Property("data", th.ArrayType(StringType)),
            )
        ),
        th.Property("ticket_parts", th.ObjectType(
            th.Property("type", th.StringType),
            th.Property("ticket_parts", th.ArrayType(StringType)),
            th.Property("total_count", th.IntegerType),
            )
        ),
        th.Property("is_shared", th.BooleanType)
    ).to_dict()

    def get_child_context(self, record: dict, context: t.Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"ticket_id": record["id"]}

class TicketsStream(IntercomStream):
    name = "tickets"
    parent_stream_type = TicketsListStream
    state_partitioning_keys = []
    path = "/tickets/{ticket_id}"
    primary_keys: t.ClassVar[list[str]] = ["ticket_id"]
    replication_key = "updated_at"

    schema = th.PropertiesList(
        th.Property("type", th.StringType),
        th.Property("id", th.StringType),
        th.Property("ticket_id", th.StringType),
        th.Property("category", th.StringType),
        th.Property("ticket_attributes", th.ObjectType(
            th.Property("name", th.StringType),
            th.Property("question", th.StringType),
            )
        ),
        th.Property("ticket_state", th.StringType),
        th.Property("ticket_state_internal_label", th.StringType),
        th.Property("ticket_state_external_label", th.StringType),
        th.Property("ticket_type", th.ObjectType(
            th.Property("type", th.StringType),
            th.Property("id", th.StringType),
            th.Property("category", th.StringType),
            th.Property("name", th.StringType),
            th.Property("description", th.StringType),
            th.Property("icon", th.StringType),
            th.Property("workspace_id", th.StringType),
            th.Property("ticket_type_attributes", th.StringType),
            th.Property("archived", th.BooleanType),
            th.Property("created_at", th.IntegerType),
            th.Property("updated_at", th.IntegerType),
            )
        ),
        th.Property("contacts", th.ObjectType(
            th.Property("type", th.StringType),
            th.Property("contacts", th.ArrayType(StringType)),
            )
        ),
        th.Property("admin_assignee_id", th.StringType),
        th.Property("team_assignee_id", th.StringType),
        th.Property("created_at", th.IntegerType),
        th.Property("updated_at", th.IntegerType),
        th.Property("open", th.BooleanType),
        th.Property("snoozed_until", th.IntegerType),
        th.Property("linked_objects", th.ObjectType(
            th.Property("type", th.StringType),
            th.Property("total_count", th.IntegerType),
            th.Property("has_more", th.BooleanType),
            th.Property("data", th.ArrayType(StringType)),
            )
        ),
        th.Property("ticket_parts", th.ObjectType(
            th.Property("type", th.StringType),
            th.Property("ticket_parts", th.ArrayType(StringType)),
            th.Property("total_count", th.IntegerType),
            )
        ),
        th.Property("is_shared", th.BooleanType)
    ).to_dict()



class SegmentsStream(IntercomStream):
    name = "segments"
    path = "/segments"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    records_jsonpath = "$.segments[*]"

    schema = PropertiesList(
        Property("type", StringType),
        Property("id", StringType),
        Property("name", StringType),
        Property("created_at", StringType),
        Property("updated_at", StringType),
        Property("person_type", StringType),
        Property("count", IntegerType),
    ).to_dict()

class ContentExportStream(IntercomStream):
    """Define custom stream to fetch job_identifier."""

    def __init__(self, name, replication_key, *args, **kwargs):
        self.name = name
        self.replication_key = replication_key
        super().__init__(*args, **kwargs)

    replication_method = "INCREMENTAL"
    replication_key = None

    @property
    def schema_filepath(self) -> Path:
        SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
        return SCHEMAS_DIR / f"{self.name}.json"

    def get_records(self, context):
        self.request_content_export(context)
        stream_filename = self.get_filename()

        if stream_filename:
            with open(f'/tmp/intercom_data/{stream_filename}', 'r') as current_file:
                first_line = current_file.readline()
                columns = first_line.strip().split(',')

                for line in current_file:
                    if line != '':
                        yield dict(zip(columns, line.strip().split(',')))

            current_time = self.hour_rounder(utc_now()).strftime("%Y-%m-%dT%H:%M:%SZ")
            self._tap.state['bookmarks'][self.name] = {'replication_key': self.replication_key, 'replication_key_value': current_time}


    def request_content_export(self, context):
        self.check_folder('/tmp/intercom_data')
        if not os.listdir('/tmp/intercom_data'):
            job_identifier = self.get_job_identifier(context)

            while True:
                status = self.check_status(job_identifier)
                self.logger.info(f"Status for job_identifier({job_identifier}): {status}")

                if status == 'completed':
                    break
                else:
                    time.sleep(10)

            self.download_export(job_identifier)

    def get_job_identifier(self, context):
        payload = self.get_payload(context)
        response = requests.post(
            f"{self.config['base_url']}/export/content/data",
            headers={'Authorization': f'Bearer {self.config.get("access_token")}',
            'Accept': 'application/json'},
            json=payload
        )
        return response.json().get("job_identifier")

    def get_payload(self, context):
        start_date = self.get_starting_replication_key_value(context)

        if type(start_date) == str:
            start_date = int(datetime.timestamp(datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")))

        end_date = self.config.get("end_date")
        if not end_date:
            end_date = int(datetime.timestamp(utc_now()))

        if type(end_date) == str:
            end_date = int(datetime.timestamp(datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")))

        payload = {
                "created_at_after": start_date,
                "created_at_before": end_date
                }
        return payload

    def check_status(self, job_identifier: str) -> str:
        response = requests.get(
            f"{self.config['base_url']}/export/content/data/{job_identifier}",
            headers={'Authorization': f'Bearer {self.config.get("access_token")}', 'Accept': 'application/json'}
        )
        return response.json()["status"]

    def download_export(self, job_identifier: str):
        response = requests.get(
            f"{self.config['base_url']}/download/content/data/{job_identifier}",
            headers={'Authorization': f'Bearer {self.config.get("access_token")}', 'Accept': 'application/octet-stream'}
        )

        file_name = f'tmp_intercom_data.zip'
        with open(f'/tmp/intercom_data/{file_name}', 'wb') as file:
            file.write(response.content)
            self.logger.info("Files have been downloaded")
        self.decompress_zip(f'/tmp/intercom_data/{file_name}')
        self.delete_zipfile(f'/tmp/intercom_data/{file_name}')

    def check_folder(self, folder: str):
        if not os.path.exists(folder):
            os.makedirs(folder)

    def decompress_zip(self, file_path: str):
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall("/tmp/intercom_data/")

    def delete_zipfile(self, file_path: str):
        os.remove(file_path)

    def get_filename(self):
        files = os.listdir('/tmp/intercom_data/')
        for file in files:
            if re.match(self.name + r'_\d{8}-\d{6}\.csv', file):
                return file

    def hour_rounder(self, timestamp):
        return (timestamp.replace(second=0, microsecond=0, minute=0, hour=timestamp.hour)
                +timedelta(hours=timestamp.minute//30))
