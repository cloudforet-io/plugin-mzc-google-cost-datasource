import logging
from datetime import datetime, timedelta

from spaceone.core.manager import BaseManager
from spaceone.cost_analysis.connector import GoogleStorageConnector
from spaceone.cost_analysis.model import Tasks
from spaceone.cost_analysis.error import *

_LOGGER = logging.getLogger(__name__)


class JobManager(BaseManager):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.google_storage_connector: GoogleStorageConnector = self.locator.get_connector(GoogleStorageConnector)

    def get_tasks(self, options, secret_data, schema, start, last_synchronized_at, domain_id):

        tasks = []
        changed = []

        start_time = self._get_start_time(start, last_synchronized_at)
        start_date = start_time.strftime('%Y-%m-%d')
        changed_time = start_time
        self.google_storage_connector.create_session(options, secret_data, schema)

        collect_info = secret_data['collect']
        bucket_name = collect_info['bucket']
        sub_account_ids = collect_info.get('sub_account_id', [])

        for bucket in self.google_storage_connector.list_buckets():
            if bucket['name'] == bucket_name:
                if sub_account_ids:
                    for sub_account_id in sub_account_ids:
                        tasks.append({
                            'task_options': {
                                'bucket_name': bucket_name,
                                'sub_account_id': sub_account_id,
                                'start': start_date
                            }
                        })
                        changed.append({'start': changed_time})
                else:
                    tasks.append({
                        'task_options': {
                            'bucket_name': bucket_name,
                            'start': start_date
                        }
                    })
                    changed.append({'start': changed_time})

        tasks = Tasks({'tasks': tasks, 'changed': changed})
        tasks.validate()
        _LOGGER.debug(f'[get_tasks] create JobTasks: {tasks.to_primitive()}')
        return tasks.to_primitive()

    @staticmethod
    def _get_start_time(start, last_synchronized_at=None):
        if start:
            start_time: datetime = start
        elif last_synchronized_at:
            start_time: datetime = last_synchronized_at - timedelta(days=7)
            start_time = start_time.replace(day=1)
        else:
            start_time: datetime = datetime.utcnow() - timedelta(days=365)
            start_time = start_time.replace(day=1)

        start_time = start_time.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

        return start_time
