import logging
import io
import re
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil import rrule

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.connector import GoogleStorageConnector

_LOGGER = logging.getLogger(__name__)

_PAGE_SIZE = 2000


class CostManager(BaseManager):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.google_storage_connector: GoogleStorageConnector = self.locator.get_connector(GoogleStorageConnector)

    def get_data(self, options, secret_data, schema, task_options):
        self.google_storage_connector.create_session(options, secret_data, schema)
        self._check_task_options(task_options)

        start = task_options['start']
        bucket_name = f'mzc-{task_options["bucket_name"]}'
        files_info = self.google_storage_connector.list_objects(bucket_name)
        file_names = [file_info['name'] for file_info in files_info]
        sub_billing_account_ids = self._list_sub_billing_account_ids(file_names)

        date_ranges = self._get_date_range(start)
        for date in date_ranges:
            year, month = date.split('-')

            for sub_billing_account_id in sub_billing_account_ids:
                file_path = f'{sub_billing_account_id}/{year}/{month}/'
                if file_path not in file_names:
                    _LOGGER.debug(f'[get_data] target_file is not exist(/{bucket_name}/{file_path})')
                    continue
                else:
                    if csv_file_path := self._get_csv_file_path(file_path, file_names):

                        blob = self.google_storage_connector.get_blob(bucket_name, csv_file_path)
                        response_stream = self._get_cost_data(data=blob.download_as_bytes(), target_file=csv_file_path)
                        for results in response_stream:
                            yield self._make_cost_data(results)
                    yield []

    @staticmethod
    def _check_task_options(task_options):
        if 'start' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.start')

        if 'bucket_name' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.bucket_name')

    @staticmethod
    def _get_date_range(start):
        date_ranges = []
        start_time = datetime.strptime(start, '%Y-%m-%d')
        now = datetime.utcnow()
        for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_time, until=now):
            billed_month = dt.strftime('%Y-%m')
            date_ranges.append(billed_month)

        return date_ranges

    @staticmethod
    def _get_csv_file_path(file_path, file_names):
        csv_files = [
            file_name for file_name in file_names
            if file_name.startswith(file_path) and file_name.endswith('.csv')
        ]

        if len(csv_files) > 1:
            raise ERROR_TOO_MANY_CSV_FILES(target_dir=csv_files)
        else:
            return csv_files[0]

    def _get_cost_data(self, data, target_file):
        data_frame = pd.read_csv(io.BytesIO(data))
        data_frame = data_frame.replace({np.nan: None})

        data_frame = self._apply_strip_to_columns(data_frame)
        costs_data = data_frame.to_dict('records')

        _LOGGER.debug(f'[get_cost_data] costs count({target_file}): {len(costs_data)}')

        # Paginate
        page_count = int(len(costs_data) / _PAGE_SIZE) + 1

        for page_num in range(page_count):
            offset = _PAGE_SIZE * page_num
            yield costs_data[offset:offset + _PAGE_SIZE]

    @staticmethod
    def _apply_strip_to_columns(data_frame):
        columns = list(data_frame.columns)
        columns = [column.strip() for column in columns]
        data_frame.columns = columns
        return data_frame

    @staticmethod
    def _make_cost_data(results):
        costs_data = []

        for result in results:
            try:
                data = {
                    'cost': result['소계'],
                    'currency': 'KRW',
                    'usage_quantity': result['Usage'],
                    'provider': 'google_cloud',
                    'product': result['Service Name'],
                    'region_code': result.get('Region') if result.get('Region') else 'global',
                    'account': result.get('Project ID'),
                    'usage_type': result['SKU Name'],
                    'usage_unit': result['Usage Unit'],
                    'billed_at': datetime.strptime(result['End Date'], '%Y-%m-%d'),
                    'additional_info': {
                        'Project Name': result.get('Project Name'),
                        'Cost Type': result.get('Cost Type')
                    },
                }

            except Exception as e:
                _LOGGER.error(f'[_make_cost_data] make data error: {e}', exc_info=True)
                raise e

            costs_data.append(data)

        return costs_data

    @staticmethod
    def _list_sub_billing_account_ids(file_names):
        pattern = r'^[A-Z0-9]{6}-[A-Z0-9]{6}-[A-Z0-9]{6}/$'

        sub_billing_account_ids = []
        for file_name in file_names:
            if re.match(pattern, file_name):
                sub_billing_account_id, _ = file_name.split('/')
                sub_billing_account_ids.append(sub_billing_account_id)
        return sub_billing_account_ids
