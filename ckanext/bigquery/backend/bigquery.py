# -*- coding: utf-8 -*-
import logging
import os
from typing import Any

from ckan.common import config
from ckanext.datastore.backend import DatastoreBackend
from google.cloud import bigquery
import ckan.plugins.toolkit as toolkit

from src import ckan_to_bigquery as ckan2bq
from src.api_tracker import ga_api_tracker, ga_search_sql_api_tracker

log = logging.getLogger(__name__)

class DatastoreBigQueryBackend(DatastoreBackend):
    def __init__(self):
        self._engine = None
        # https://github.com/ckan/ckan/issues/5333
        # Check whether users have disabled datastore_search_sql
        self.enable_sql_search = toolkit.asbool(config.get('ckan.datastore.sqlsearch.enabled', True))

    def _get_engine(self):
        '''To be able to run google cloud bigquery/storage operations you need to setup your credentials.
        
        Follow https://cloud.google.com/docs/authentication/getting-started
        '''
        creds = config.get('ckanext.bigquery.google_cloud_credentials', None)
        read_only_creds = config.get('ckanext.bigquery.google_cloud_credentials_read_only', None)
        project = config.get('ckanext.bigquery.project', None)
        dataset = config.get('ckanext.bigquery.dataset', None)
        self._engine = ckan2bq.Client(project, dataset, creds, read_only_creds)
        return self._engine

    def _log_or_raise(self, message):
        if self.config.get('debug'):
            log.critical(message)
        else:
            raise Exception(message)

    def search(self, context, data_dict):
        ga_api_tracker(data_dict['resource_id'])
        # we need to call bg2ckan lib -> search
        # we need to mock the resource_id
        engine = self._get_engine()
        return engine.search(data_dict)
    
    def search_sql(self, context, data_dict):
        ga_search_sql_api_tracker(data_dict['sql'])
        # TODO: try / except
        # TODO: timeouts etc

        # TODO: restrict table access (??)
        # table_names = datastore_helpers.get_table_names_from_sql(context, sql)
        # log.debug('Tables involved in input SQL: {0!r}'.format(table_names))

        # if any(t.startswith('pg_') for t in table_names):
        #    raise toolkit.NotAuthorized({
        #        'permissions': ['Not authorized to access system tables']
        #    })
        # context['check_access'](table_names)
        engine = self._get_engine()
        return engine.search_sql(data_dict)
        

    def resource_id_from_alias(self, alias):
        if self.resource_exists(alias):
            return True, alias
        return False, alias

    def resource_exists(self, id):
        # TODO: make this more rigorous
        return True

    def resource_fields(self, id: str) -> dict[str, Any]:
        """
        Return dictionary of field information for a resource in BigQuery.
        
        :param id: The resource ID (i.e. BigQuery table name)
        :returns: A dictionary with metadata about the resource and its fields
        """
        engine = self._get_engine()
        client: bigquery.Client = engine.bqclient # Get the actual BQ client
        
        info = {'meta': {}, 'fields': []}
        
        try:
            # Resource id for dereferencing aliases
            info['meta']['id'] = id

            context = ckan2bq.get_context()
            log.info(f"Trying to get resource_show for {id}")
            resource = toolkit.get_action('resource_show')(context, {'id': id})

            log.info(f"Resource metadata for {id}: {resource}")

            bq_table_name = resource.get('bq_table_name', '')
            project_id = config.get('ckanext.bigquery.project', None)
            dataset = config.get('ckanext.bigquery.dataset', None)
            
            table_ref = bigquery.TableReference.from_string(
                    f"{project_id}.{dataset}.{bq_table_name}"
              )

            # Get table object using the correct client method
            table = client.get_table(table_ref)
            
            # Count of rows in table
            info['meta']['count'] = table.num_rows
            
            # Table type
            info['meta']['table_type'] = 'TABLE'  # BigQuery doesn't have the same table types as PostgreSQL
            
            # Size of table in bytes
            info['meta']['size'] = table.num_bytes
            
            # We don't have direct equivalents for these in BigQuery, but we can include them for compatibility
            info['meta']['db_size'] = None  # No direct equivalent
            info['meta']['idx_size'] = None  # BigQuery doesn't use traditional indexes
            
            # Get aliases if any (implement if your BigQuery setup supports aliases)
            info['meta']['aliases'] = []  # Implement if you support aliases
            
            # Get field information
            fields = []
            for field in table.schema:
                field_info = {
                    'id': field.name,
                    'type': self._bq_to_ckan_type(field.field_type),
                    'info': {},
                    'schema': {
                        'native_type': field.field_type,
                        'mode': field.mode,
                        'description': field.description,
                        'is_index': False,  # BigQuery doesn't use traditional indexes
                        'uniquekey': False,  # BigQuery doesn't enforce unique constraints the same way
                        'notnull': field.mode == 'REQUIRED'
                    }
                }
                
                # Add any field description as info
                if field.description:
                    field_info['info']['description'] = field.description
                    
                fields.append(field_info)
                
            info['fields'] = fields
            
        except Exception as e:
            log.error(f"Error getting resource fields for {id}: {str(e)}")
            # Optionally re-raise or handle the error as needed
            
        return info

    def _bq_to_ckan_type(self, bq_type: str) -> str:
        """
        Convert BigQuery data types to CKAN datastore types.
        
        :param bq_type: BigQuery data type
        :returns: Equivalent CKAN datastore type
        """
        type_mapping = {
            'STRING': 'text',
            'INTEGER': 'int',
            'INT64': 'int',
            'FLOAT': 'float',
            'FLOAT64': 'float',
            'NUMERIC': 'numeric',
            'BOOLEAN': 'bool',
            'BOOL': 'bool',
            'TIMESTAMP': 'timestamp',
            'DATE': 'date',
            'TIME': 'time',
            'DATETIME': 'timestamp',
            'RECORD': 'nested',
            'STRUCT': 'nested',
            'BYTES': 'text',
            'GEOGRAPHY': 'text',
            'ARRAY': 'text[]',  # This is a simplification, might need refinement
            'JSON': 'json'
        }
        
        return type_mapping.get(bq_type, 'text')  # Default to text for unknown types
