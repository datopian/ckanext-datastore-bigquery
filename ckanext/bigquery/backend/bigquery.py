# -*- coding: utf-8 -*-
from ckanext.datastore.backend import DatastoreBackend

# from bq2ckan import BigQuery2CKAN

log = logging.getLogger(__name__)


class DatastoreBigQueryBackend(DatastoreBackend):
    def __init__(self):
        # TODO: how do we want credentials to get passed in via config or env variable ??
        credentials = config.get('google_cloud_credentials', None)
        project = config.get('project', None)
        self._engine = BigQuery2CKAN(credentials, project_id, ...)

    def _log_or_raise(self, message):
        if self.config.get('debug'):
            log.critical(message)
        else:
            raise DatastoreException(message)

     def search(self, context, data_dict):
         return self._engine.search(data_dict)

