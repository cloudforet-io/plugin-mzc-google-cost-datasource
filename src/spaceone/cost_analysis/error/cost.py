from spaceone.core.error import *


class ERROR_TOO_MANY_CSV_FILES(ERROR_UNKNOWN):
    _message = 'Too many csv files: {target_dir}'
