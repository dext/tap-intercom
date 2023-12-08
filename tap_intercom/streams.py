"""Stream type classes for tap-intercom."""

from __future__ import annotations

import typing as t
import requests
from pathlib import Path
from typing import Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers
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
    records_jsonpath = "$.contacts[*]"
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


class TagsStream(IntercomStream):
    name = "tags"
    path = "/tags"
    primary_keys: t.ClassVar[list[str]] = ["id"]
    records_jsonpath = "$.teams[*]"
    schema = PropertiesList(
        Property("id", IntegerType),
        Property("direct_link", StringType),
        Property("name", StringType),
        Property("color", StringType),
        Property("description", StringType),
    ).to_dict()
