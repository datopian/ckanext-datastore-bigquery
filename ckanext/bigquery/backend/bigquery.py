# -*- coding: utf-8 -*-
import logging

from ckanext.datastore.backend import DatastoreBackend

# from bq2ckan import BigQuery2CKAN
from google.cloud import bigquery
from ckan.common import config


log = logging.getLogger(__name__)


class DatastoreBigQueryBackend(DatastoreBackend):
    def __init__(self):
        self._engine = None

    def _get_engine(self):
        # TODO: how do we want credentials to get passed in via config or env variable ??
        cloud_credentials = config.get('ckanext.bigquery.google_cloud_credentials', None)
        project = config.get('ckanext.bigquery.project', None)
        self._engine = bigquery.Client.from_service_account_json(cloud_credentials)
        return self._engine

    def _log_or_raise(self, message):
        if self.config.get('debug'):
            log.critical(message)
        else:
            raise DatastoreException(message)

    # def search(self, context, data_dict):
    #     return self._engine.search(data_dict)

    def resource_id_from_alias(self, alias):
        if self.resource_exists(alias):
            return True, alias
        return False, alias


    def resource_exists(self, id):
        return self._get_engine().execute(
            u'''
            select * from id
            limit 1'''.format(
                id)
        ).fetchone()