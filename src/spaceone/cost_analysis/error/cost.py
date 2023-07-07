from spaceone.core.error import *


class ERROR_CONNECTOR_CALL_API(ERROR_UNKNOWN):
    _message = 'API Call Error: {reason}'


class ERROR_SUB_BILLING_ACCOUNT_NOT_FOUND(ERROR_UNKNOWN):
    _message = 'Sub Billing Account Not Found: {service_account_id}'


class ERROR_NOT_FOUND_BUCKET(ERROR_UNKNOWN):
    _message = 'Not Found Bucket: {bucket_name}'


class ERROR_TOO_MANY_CSV_FILES(ERROR_UNKNOWN):
    _message = 'Too many csv files: {target_dir}'
