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

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
class ContentExportStream(IntercomStream):
    """Define custom stream to fetch job_identifier."""
    replication_method = "INCREMENTAL"
    replication_key = None

    def get_records(self, context):
        self.request_content_export()
        stream_filename = self.get_filename()

        with open(f'/tmp/intercom_data/{stream_filename}', 'r') as current_file:
            first_line = current_file.readline()
            columns = first_line.strip().split(',')

            for line in current_file:
                if line != '':
                    yield dict(zip(columns, line.strip().split(',')))

        current_time = self.hour_rounder(utc_now()).isoformat()
        self._tap.state['bookmarks'][self.name] = {'replication_key': self.replication_key, 'replication_key_value': current_time}
        # self._tap.write_state()

    def request_content_export(self):
        self.check_folder('/tmp/intercom_data')
        if not os.listdir('/tmp/intercom_data'):
            job_identifier = self.get_job_identifier()

            while True:
                status = self.check_status(job_identifier)
                self.logger.info(f"Status for job_identifier({job_identifier}): {status}")

                if status == 'completed':
                    break
                else:
                    time.sleep(10)

            self.download_export(job_identifier)

    def get_job_identifier(self):
        payload = self.get_payload()
        response = requests.post(
            f"{self.config['base_url']}/export/content/data",
            headers={'Authorization': f'Bearer {self.config.get("access_token")}',
            'Accept': 'application/json'},
            json=payload
        )
        r = response.json().get("job_identifier")
        return response.json().get("job_identifier")

    def get_payload(self):
        start_date = self.config.get("start_date")
        if start_date:
            if type(start_date) == str:
                start_date = int(datetime.timestamp(datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ")))
                # self.logger.info(f"start_date: {start_date}")
        end_date = self.config.get("end_date")
        if end_date:
            if type(end_date) == str:
                end_date = int(datetime.timestamp(datetime.strptime(end_date, "%Y-%m-%dT%H:%M:%SZ")))
                # self.logger.info(f"end_date: {end_date}")
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
            zip_ref.extractall('/tmp/intercom_data/')

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

