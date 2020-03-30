# -*- coding: utf-8 -*-
import logging
import os

from ckan.common import config
from ckanext.datastore.backend import DatastoreBackend

from src import ckan_to_bigquery as ckan2bq
from ckan.common import config

log = logging.getLogger(__name__)

class DatastoreBigQueryBackend(DatastoreBackend):
    def __init__(self):
        self._engine = None

    def _get_engine(self):
        # TODO: how do we want credentials to get passed in via config or env variable ??
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.get('ckanext.bigquery.google_cloud_credentials', None)
        project = config.get('ckanext.bigquery.project', None)
        self._engine = ckan2bq.Client(project)
        return self._engine

    def _log_or_raise(self, message):
        if self.config.get('debug'):
            log.critical(message)
        else:
            raise DatastoreException(message)

    def search(self, context, data_dict):
        # we need to call bg2ckan lib -> search
        # we need to mock the resource_id
        engine = self._get_engine()

        return engine.search(data_dict)

    def resource_id_from_alias(self, alias):
        if self.resource_exists(alias):
            return True, alias
        return False, alias

    def resource_exists(self, id):
        # TODO: make this more rigorous
        return True
