import ckan.plugins as plugins

from ckanext.bigquery.backend.bigquery import DatastoreBigQueryBackend
from ckanext.datastore.interfaces import IDatastoreBackend

class BigqueryPlugin(plugins.SingletonPlugin):
    plugins.implements(IDatastoreBackend)

    # IDatastoreBackend
    def register_backends(self):
        return {
            u'bigquery': DatastoreBigQueryBackend,
        }

