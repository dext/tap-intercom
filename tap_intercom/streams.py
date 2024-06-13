"""Stream type classes for tap-intercom."""

from __future__ import annotations

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
from datetime import datetime
import os

import zipfile

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
class ContentExportStream(IntercomStream):
    """Define custom stream to fetch job_identifier."""
    def __init__(self, name, schema, *args, **kwargs):
        super().__init__(name=name, *args, **kwargs)

    def sync(self):
        job_identifier = self.request_content_export()
        print('----------- JOB IDENTIFIER -----------\n', job_identifier)
        while True:
            status = self.check_status(job_identifier)
            print('----------- STATUS -----------\n', status)
            if status == 'completed':
                break
            else:
                time.sleep(10)

        self.download_export(job_identifier)

        streams = os.listdir('temp_intercom_data')

        for stream in streams:

            self.name = stream.split('_')[0]
            stream_id = stream.split('_')[0]

            self.logger.info(50 * "*")
            self.logger.info(f"STREAM: {stream}")
            self.logger.info(selected)
            self.logger.info(50 * "*")



        #     for record in self.get_records(stream):

        #         self._write_record_message(record)
        #         if "completed_at" in record:
        #             self.stream_state[stream_id] = record["completed_at"]
        #         self._write_state_message()

        # os.system("rm -rf temp_intercom_data")


    def get_records(self, stream: str):
        with open(f'temp_intercom_data/{stream}', 'r') as current_file:
            first_line = current_file.readline()
            columns = first_line.strip().split(',')

            for line in current_file:
                yield dict(zip(columns, line.strip().split(',')))

    # def write_schema(self, stream: str):
    #     with open(f'temp_intercom_data/{stream}', 'r') as current_file:
    #         first_line = current_file.readline()
    #         columns = first_line.strip().split(',')
    #         properties = []
    #         for column in columns:
    #             properties.append(Property(column, StringType))

    #         with open(f'/Users/daniela.angelova/projects/tap-intercom/tap_intercom/schemas/{stream}.json', 'w') as file:
    #             file.write(PropertiesList(*properties).to_json())
    #         return PropertiesList(*properties).to_dict()


    def request_content_export(self):
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

        self.check_folder('temp_intercom_data')

        file_name = f'intercom_data_{time.time()}.zip'
        with open(f'temp_intercom_data/{file_name}', 'wb') as file:
            file.write(response.content)
            self.decompress_gzip(f'temp_intercom_data/{file_name}')
            self.delete_zipfile(f'temp_intercom_data/{file_name}')

    def check_folder(self, folder: str):
        if not os.path.exists(folder):
            os.makedirs(folder)

    def decompress_gzip(self, file_path: str):
        with zipfile.ZipFile(file_path, 'r') as zip_ref:
            zip_ref.extractall('temp_intercom_data')

    def delete_zipfile(self, file_path: str):
        os.remove(file_path)


