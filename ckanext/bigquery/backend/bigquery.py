# -*- coding: utf-8 -*-
import logging
import os
from typing import Any
from uuid import UUID

from ckan.common import config
from ckan.lib.search import SearchError, SearchQueryError
from ckan.logic import NotAuthorized, NotFound
from ckanext.datastore.backend import DatastoreBackend
import ckan.plugins.toolkit as toolkit
from google.api_core.exceptions import (
    BadRequest,
    Forbidden,
    GatewayTimeout,
    NotFound as GoogleNotFound,
    RetryError,
    ServiceUnavailable,
    TooManyRequests,
)

from src import ckan_to_bigquery as ckan2bq
from src.api_tracker import ga_api_tracker, ga_search_sql_api_tracker

log = logging.getLogger(__name__)


RATE_LIMIT_REASONS = {
    'billingTierLimitExceeded',
    'quotaExceeded',
    'rateLimitExceeded',
    'resourceExhausted',
}


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
        try:
            UUID(data_dict['resource_id'])
        except (AttributeError, TypeError, ValueError):
            pass
        else:
            resource = toolkit.get_action('resource_show')(
                context, {'id': data_dict['resource_id']}
            )
            bq_table_name = resource.get('bq_table_name')
            if not bq_table_name:
                raise SearchQueryError(
                    "Resource '{}' has no bq_table_name".format(
                        data_dict['resource_id']
                    )
                )
            data_dict = dict(data_dict, bq_table_name=bq_table_name)

        # we need to call bg2ckan lib -> search
        # we need to mock the resource_id
        engine = self._get_engine()
        return self._execute_query(engine.search, data_dict)
    
    def search_sql(self, context, data_dict):
        ga_search_sql_api_tracker(data_dict.get('sql', ''))

        # TODO: restrict table access (??)
        # table_names = datastore_helpers.get_table_names_from_sql(context, sql)
        # log.debug('Tables involved in input SQL: {0!r}'.format(table_names))

        # if any(t.startswith('pg_') for t in table_names):
        #    raise toolkit.NotAuthorized({
        #        'permissions': ['Not authorized to access system tables']
        #    })
        # context['check_access'](table_names)
        engine = self._get_engine()
        return self._execute_query(engine.search_sql, data_dict)

    def _execute_query(self, query, data_dict):
        try:
            return query(data_dict)
        except BadRequest as error:
            log.info("Invalid BigQuery SQL: %s", error.message)
            raise SearchQueryError(error.message) from error
        except GoogleNotFound as error:
            log.info("BigQuery dataset or table not found: %s", error.message)
            raise NotFound(
                "Dataset or table not found: {}".format(error.message)
            ) from error
        except TooManyRequests as error:
            raise SearchError(self._rate_limit_message(error)) from error
        except Forbidden as error:
            if self._error_reasons(error) & RATE_LIMIT_REASONS:
                raise SearchError(self._rate_limit_message(error)) from error
            log.info("BigQuery access denied: %s", error.message)
            raise NotAuthorized(
                "You do not have access to this dataset"
            ) from error
        except (GatewayTimeout, TimeoutError) as error:
            message = str(error) or "Query timed out"
            raise SearchError(message) from error
        except RetryError as error:
            raise SearchError(
                "BigQuery request failed after retries; try again later"
            ) from error
        except ServiceUnavailable as error:
            raise SearchError(
                "BigQuery is temporarily unavailable; try again later"
            ) from error

    @staticmethod
    def _error_reasons(error):
        return {
            item.get('reason')
            for item in (error.errors or [])
            if isinstance(item, dict) and item.get('reason')
        }

    @staticmethod
    def _rate_limit_message(error):
        response = getattr(error, 'response', None)
        headers = getattr(response, 'headers', {}) or {}
        retry_after = headers.get('retry-after') or headers.get('Retry-After')
        if retry_after:
            return "Rate limit exceeded; try again in {} seconds".format(
                retry_after
            )
        return "Rate or quota limit exceeded; try again later"
        

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
        
        info = {'meta': {}, 'fields': []}
        
        try:
            # Resource id for dereferencing aliases
            info['meta']['id'] = id
            
            # Get table metadata from BigQuery
            table_ref = engine.get_table_reference(id)
            table = engine.client.get_table(table_ref)
            
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
