import logging
import io
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil import rrule

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.error import *
from spaceone.cost_analysis.connector import GoogleStorageConnector, SpaceONEConnector

_LOGGER = logging.getLogger(__name__)

_DEFAULT_BUCKET_NAME = 'mzc-billing'
_PAGE_SIZE = 2000


class CostManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.google_storage_connector: GoogleStorageConnector = self.locator.get_connector(GoogleStorageConnector)
        self.space_connector: SpaceONEConnector = self.locator.get_connector(SpaceONEConnector)

    def get_data(self, options, secret_data, schema, task_options):
        self.google_storage_connector.create_session(options, secret_data, schema)
        self._check_task_options(task_options)

        start = task_options['start']
        account_id = task_options['account_id']
        service_account_id = task_options['service_account_id']
        is_sync = task_options['is_sync']
        sub_billing_account = task_options['sub_billing_account']

        if is_sync == 'false':
            self._update_sync_state(options, secret_data, schema, service_account_id)

        prefix = f'billing-data/{sub_billing_account}'
        files_info = self.google_storage_connector.list_objects(_DEFAULT_BUCKET_NAME, prefix=prefix)
        file_names = [file_info['name'] for file_info in files_info]

        date_ranges = self._get_date_range(start)
        for date in date_ranges:
            year, month = date.split('-')

            target_file_path = prefix + f'/{year}/{month}/'
            if target_file := self._create_target_file_path(file_names, target_file_path):

                blob = self.google_storage_connector.get_blob(_DEFAULT_BUCKET_NAME, target_file)
                response_stream = self._get_cost_data(data=blob.download_as_bytes(), target_file=target_file)
                for results in response_stream:
                    yield self._make_cost_data(results, account_id)

            else:
                continue

        yield []

    @staticmethod
    def _check_task_options(task_options):
        if 'start' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.start')

        if 'account_id' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.account_id')

        if 'service_account_id' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.service_account_id')

        if 'is_sync' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.is_sync')

        if 'sub_billing_account' not in task_options:
            raise ERROR_REQUIRED_PARAMETER(key='task_options.sub_billing_account')

    def _update_sync_state(self, options, secret_data, schema, service_account_id):
        self.space_connector.init_client(options, secret_data, schema)
        service_account_info = self.space_connector.get_service_account(service_account_id)
        tags = service_account_info.get('tags', {})
        tags['is_sync'] = 'true'
        self.space_connector.update_service_account(service_account_id, tags)

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
    def _create_target_file_path(file_names, target_file_path):
        if target_file_path not in file_names:
            _LOGGER.debug(
                f'[get_data] target_file is not exist(/{_DEFAULT_BUCKET_NAME}/{target_file_path})')
        else:
            target_dir = [file_name for file_name in file_names
                          if file_name.startswith(target_file_path) and file_name.endswith('.csv')]

            if len(target_dir) > 1:
                raise ERROR_TOO_MANY_CSV_FILES(target_dir=target_dir)
            else:
                return target_dir[0]
        return None

    @staticmethod
    def _get_cost_data(data, target_file):
        data_frame = pd.read_csv(io.BytesIO(data))
        data_frame = data_frame.replace({np.nan: None})
        costs_data = data_frame.to_dict('records')

        _LOGGER.debug(f'[get_cost_data] costs count({target_file}): {len(costs_data)}')

        # Paginate
        page_count = int(len(costs_data) / _PAGE_SIZE) + 1

        for page_num in range(page_count):
            offset = _PAGE_SIZE * page_num
            yield costs_data[offset:offset + _PAGE_SIZE]

    @staticmethod
    def _make_cost_data(results, account_id):
        costs_data = []

        for result in results:
            try:
                data = {
                    'cost': result['Final Cost'],
                    'currency': result['Currency'],
                    'usage_quantity': result['Usage'],
                    'provider': 'google_cloud',
                    'product': result['Service Name'],
                    'region_code': result.get('Region', 'global'),
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

            if account_id != 'global':
                if data['account'] == account_id:
                    costs_data.append(data)
            else:
                costs_data.append(data)
        return costs_data
