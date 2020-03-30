# -*- coding: utf-8 -*-
import logging

from ckanext.datastore.backend import DatastoreBackend

#from bq2ckan import BigQuery2CKAN
import bq2ckan as BigQuery2CKAN
from src import ckan_to_bigquery
from ckan.common import config

log = logging.getLogger(__name__)


class DatastoreBigQueryBackend(DatastoreBackend):
    def __init__(self):
        self._engine = None

    def _get_engine(self):
        # TODO: how do we want credentials to get passed in via config or env variable ??
        cloud_credentials = config.get('ckanext.bigquery.google_cloud_credentials', None)
        project = config.get('ckanext.bigquery.project', None)
        self._engine = BigQuery2CKAN.get_google_client(cloud_credentials)
        return self._engine

    def _log_or_raise(self, message):
        if self.config.get('debug'):
            log.critical(message)
        else:
            raise DatastoreException(message)

    def search(self, context, data_dict):
        # we need to call bg2ckan lib -> search
        # we need to mock the resource_id
        resource_id = '201401'
        return ckan_to_bigquery.search(data_dict)

    def resource_id_from_alias(self, alias):
        if self.resource_exists(alias):
            return True, alias
        return False, alias


    def resource_exists(self, id):
        return self._get_engine().execute(u'SELECT * FROM "bigquerytest-271707.NHS.201401" LIMIT 10').fetchone()