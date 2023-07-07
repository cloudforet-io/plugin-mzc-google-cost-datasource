LOG = {
    'filters': {
        'masking': {
            'rules': {
                'DataSource.verify': [
                    'secret_data'
                ],
                'Job.get_tasks': [
                    'secret_data'
                ],
                'Cost.get_data': [
                    'secret_data'
                ]
            }
        }
    }
}

CONNECTORS = {
    'SpaceConnector': {
        'backend': 'spaceone.core.connector.space_connector.SpaceConnector',
        'endpoints': {
            'identity': 'grpc+ssl://identity.api.dev.spaceone.dev:443/v1'
        }
    },
}
