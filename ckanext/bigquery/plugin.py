import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

from ckanext.bigquery.logic import action
from ckanext.bigquery.backend.bigquery import DatastoreBigQueryBackend


class BigqueryPlugin(plugins.SingletonPlugin):
    plugins.implements(IDatastoreBackend)

    # IDatastoreBackend
    def register_backends(self):
        return {
            'bigquery': DatastoreBigQueryBackend
        }

