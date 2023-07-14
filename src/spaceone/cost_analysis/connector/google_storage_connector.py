import logging
# import boto3
# import io
# import pandas as pd
# import numpy as np
import google.oauth2.service_account
from google.cloud import storage
from googleapiclient.discovery import build

from spaceone.core import utils
from spaceone.core.connector import BaseConnector
from spaceone.cost_analysis.error import *

MAX_OBJECTS = 100000

_LOGGER = logging.getLogger(__name__)


class GoogleStorageConnector(BaseConnector):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.project_id = None
        self.session = None
        self.storage_client = None
        self.google_client = None

    def create_session(self, options: dict, secret_data: dict, schema: str):
        self._check_secret_data(secret_data)
        self.project_id = secret_data['project_id']

        credentials = google.oauth2.service_account.Credentials.from_service_account_info(secret_data)
        self.storage_client = storage.Client(project=secret_data['project_id'], credentials=credentials)
        self.google_client = build('storage', 'v1', credentials=credentials)

    def list_buckets(self):
        buckets = self.google_client.buckets().list(project=self.project_id).execute()
        return buckets.get('items', [])

    def list_objects(self, bucket_name, prefix=None):
        self.google_client.buckets().get(bucket=bucket_name).execute()
        objects = self.google_client.objects().list(bucket=bucket_name, prefix=prefix).execute()
        return objects.get('items', [])

    def get_blob(self, bucket_name, file_path):
        bucket = self.storage_client.get_bucket(bucket_name)
        blob = bucket.get_blob(file_path)
        return blob

    @staticmethod
    def _check_secret_data(secret_data):
        if 'project_id' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.project_id')

        if 'private_key' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.private_key')

        if 'token_uri' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.token_uri')

        if 'client_email' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.client_email')

        if 'collect' not in secret_data:
            raise ERROR_REQUIRED_PARAMETER(key='secret_data.collect')