"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import os

from singer_sdk.testing import get_tap_test_class

from tap_intercom.tap import TapIntercom

SAMPLE_CONFIG = {
    "start_date": (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=1)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    ),
    "access_token": os.getenv("TAP_INTERCOM_ACCESS_TOKEN"),
}


# Run standard built-in tap tests from the SDK:
TestTapIntercom = get_tap_test_class(
    tap_class=TapIntercom,
    config=SAMPLE_CONFIG,
)
