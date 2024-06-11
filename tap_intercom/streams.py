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

import requests
import time
import os


class ContentExportStream(IntercomStream):
    """Define custom stream to fetch job_identifier."""
    name = "content_export"
    path = "/export/content/data"
    rest_method = "POST"

    schema = th.PropertiesList(
        th.Property("job_identifier", th.StringType),
        th.Property("status", th.StringType),
        th.Property("download_url", th.StringType),
        th.Property("download_expires_at", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: t.Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {"job_identifier": record["job_identifier"]}


class DownloadExportStream(IntercomStream):
    """Define custom stream that downloads content data."""
    name = "data_export"
    parent_stream_type = ContentExportStream
    path = "/download/content/data/{job_identifier}"
    ignore_parent_replication_keys = True
    rest_method = "GET"

    def sync(self, context):

        self.logger.info(f"CONTEXT: {context}")
        job_identifier = context.get("job_identifier")

        while True:
            status = self.check_status(job_identifier)
            print('----------- STATUS -----------\n', status)
            if status == 'completed':
                break
            else:
                time.sleep(30)

        url = f"{self.config['base_url']}/download/content/data/{job_identifier}"
        self.logger.info(f"DOWNLOAD PATH: {url}")
        response = requests.get(url, headers={'Authorization': f'Bearer {self.config.get("access_token")}', 'Accept': 'application/octet-stream'})

        self.check_folder('temp_intercom_data')

        with open(f'temp_intercom_data/intercom_data_{time.time()}.gzip', 'wb') as file:
            file.write(response.content)

    def check_status(self, job_identifier: str) -> str:
        response = requests.get(
            f"{self.config['base_url']}/export/content/data/{job_identifier}",
            headers={'Authorization': f'Bearer {self.config.get("access_token")}', 'Accept': 'application/json'})
        return response.json()["status"]

    def check_folder(self, folder):
        if not os.path.exists(folder):
            os.makedirs(folder)

    def decompress_gzip(self, file_path: str):
        with gzip.open(file_path, 'rb') as f_in:
            with open(file_path.replace('.gzip', ''), 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

    schema = th.PropertiesList().to_dict()
