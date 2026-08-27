"""Tests for plugin.py."""
# encoding: utf-8

from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

import pytest

from ckan.common import config
from ckan.lib.search import SearchError, SearchQueryError
from ckan.logic import NotAuthorized, NotFound
import ckan.tests.helpers as helpers
from ckanext.datastore.backend import DatastoreBackend
from ckanext.bigquery.backend.bigquery import DatastoreBigQueryBackend
from google.api_core.exceptions import (
    BadRequest,
    Forbidden,
    GatewayTimeout,
    NotFound as GoogleNotFound,
    RetryError,
    ServiceUnavailable,
    TooManyRequests,
)
from src.ckan_to_bigquery import Client, QueryTimeoutError


class_to_patch = (
    u'ckanext.bigquery.backend.'
    'bigquery.DatastoreBigQueryBackend'
)

class TestBigQueryIDatastoreBackendPlugin():
    def test_backends_correctly_registered(self):
        DatastoreBackend.register_backends()
        assert u"bigquery" in DatastoreBackend._backends
        assert u"postgres" in DatastoreBackend._backends


    @helpers.change_config(u'ckan.datastore.write_url', u'bigquery://x')
    @helpers.change_config(u'ckan.datastore.read_url', u'bigquery://x')
    def test_bigquery_engine(self):
        DatastoreBackend.set_active_backend(config)
        assert isinstance(
            DatastoreBackend.get_active_backend(), DatastoreBigQueryBackend)

    def test_backend_functionality(self):
        DatastoreBackend.set_active_backend(config)

        # table name in big query atm
        res_id = 'ckanext_testing'

        out = helpers.call_action(u'datastore_search', resource_id=res_id)
        assert out['result']['total'] == 10
        # returned results are random
        # we can't check for exact result
        # first = out['result']['records'][0]
        # assert first == expected1

    @patch.object(DatastoreBigQueryBackend, '_get_engine')
    @patch('ckanext.bigquery.backend.bigquery.toolkit.get_action')
    def test_search_resolves_uuid_to_bigquery_table(
            self, get_action, get_engine):
        resource_id = 'e4e5508c-3f9e-4676-b5bb-e126ef724060'
        resource_show = get_action.return_value
        resource_show.return_value = {'bq_table_name': 'EPD_SNOMED_202011'}
        get_engine.return_value.search.return_value = {
            'resource_id': resource_id,
        }

        result = DatastoreBigQueryBackend().search(
            {'user': 'test'}, {'resource_id': resource_id, 'limit': 1}
        )

        get_action.assert_called_once_with('resource_show')
        resource_show.assert_called_once_with(
            {'user': 'test'}, {'id': resource_id}
        )
        get_engine.return_value.search.assert_called_once_with({
            'resource_id': resource_id,
            'bq_table_name': 'EPD_SNOMED_202011',
            'limit': 1,
        })
        assert result['resource_id'] == resource_id

    @patch.object(DatastoreBigQueryBackend, '_get_engine')
    @patch('ckanext.bigquery.backend.bigquery.toolkit.get_action')
    def test_search_keeps_bigquery_table_resource_id(
            self, get_action, get_engine):
        get_engine.return_value.search.return_value = {}

        DatastoreBigQueryBackend().search(
            {}, {'resource_id': 'EPD_SNOMED_202011'}
        )

        get_action.assert_not_called()
        get_engine.return_value.search.assert_called_once_with({
            'resource_id': 'EPD_SNOMED_202011',
        })

    @patch('ckanext.bigquery.backend.bigquery.toolkit.get_action')
    def test_search_uuid_requires_bigquery_table_name(self, get_action):
        get_action.return_value.return_value = {}

        with pytest.raises(SearchQueryError) as error:
            DatastoreBigQueryBackend().search({}, {
                'resource_id': 'e4e5508c-3f9e-4676-b5bb-e126ef724060',
            })

        assert str(error.value) == (
            "Resource 'e4e5508c-3f9e-4676-b5bb-e126ef724060' "
            'has no bq_table_name'
        )

    @patch.object(DatastoreBigQueryBackend, '_get_engine')
    def test_search_sql_returns_query_error_for_invalid_sql(self, get_engine):
        message = 'Syntax error: Unexpected identifier "FROMM" at [1:10]'
        get_engine.return_value.search_sql.side_effect = BadRequest(message)

        backend = DatastoreBigQueryBackend()
        with pytest.raises(SearchQueryError) as error:
            backend.search_sql({}, {
                'resource_id': 'PCA_202501',
                'sql': 'SELECT * FROMM `PCA_202501`',
            })

        assert str(error.value) == message

    def test_search_sql_returns_query_error_for_unquoted_table(self):
        with pytest.raises(SearchQueryError) as error:
            Client.search_sql(object(), {
                'resource_id': 'PCA_202501',
                'sql': 'SELECT * FROM PCA_202501',
            })

        assert str(error.value) == (
            'SQL must contain a backtick-quoted table identifier, '
            'for example FROM `PCA_202501`'
        )

    @pytest.mark.parametrize('data_dict, message', [
        ({'sql': 'SELECT 1'}, 'resource_id is mandatory'),
        ({'resource_id': 'PCA_202501'}, 'sql must be a non-empty string'),
        ({'resource_id': 'PCA_202501', 'sql': ''},
         'sql must be a non-empty string'),
    ])
    def test_search_sql_validates_required_parameters(self, data_dict, message):
        with pytest.raises(SearchQueryError) as error:
            Client.search_sql(object(), data_dict)

        assert str(error.value) == message

    def test_search_sql_rejects_mismatched_resource_id(self):
        with pytest.raises(SearchQueryError) as error:
            Client.search_sql(object(), {
                'resource_id': 'PCA_202501',
                'sql': 'SELECT * FROM `PCA_202502`',
            })

        assert "does not match resource_id" in str(error.value)

    @patch('src.ckan_to_bigquery.resource_show', return_value={})
    @patch('src.ckan_to_bigquery.get_context', return_value={})
    def test_search_sql_rejects_invalid_bulk_parameter(
            self, get_context, resource_show):
        client = Client.__new__(Client)
        client.resource_details = {}

        with pytest.raises(SearchQueryError) as error:
            client.search_sql({
                'resource_id': 'PCA_202501',
                'sql': 'SELECT * FROM `PCA_202501`',
                'bulk': 'sometimes',
            })

        assert str(error.value) == 'bulk must be true or false'

    @patch('src.ckan_to_bigquery.resource_show', return_value={})
    @patch('src.ckan_to_bigquery.get_context', return_value={})
    def test_search_sql_treats_false_bulk_parameter_as_false(
            self, get_context, resource_show):
        client = Client.__new__(Client)
        client.resource_details = {}
        client.search_sql_normal = Mock(return_value='normal result')

        result = client.search_sql({
            'resource_id': 'PCA_202501',
            'sql': 'SELECT * FROM `PCA_202501`',
            'bulk': 'false',
        })

        assert result == 'normal result'

    def test_missing_bigquery_table_returns_not_found(self):
        backend = DatastoreBigQueryBackend()
        query = Mock(side_effect=GoogleNotFound('Table missing'))

        with pytest.raises(NotFound) as error:
            backend._execute_query(query, {})

        assert str(error.value) == (
            'Dataset or table not found: Table missing'
        )

    def test_bigquery_permission_failure_returns_not_authorized(self):
        backend = DatastoreBigQueryBackend()
        query = Mock(side_effect=Forbidden(
            'Access denied', errors=[{'reason': 'accessDenied'}]
        ))

        with pytest.raises(NotAuthorized) as error:
            backend._execute_query(query, {})

        assert str(error.value) == 'You do not have access to this dataset'

    @pytest.mark.parametrize('bigquery_error', [
        TooManyRequests('Too many requests'),
        Forbidden('Quota exceeded', errors=[{'reason': 'quotaExceeded'}]),
    ])
    def test_bigquery_rate_limit_returns_search_error(self, bigquery_error):
        backend = DatastoreBigQueryBackend()
        query = Mock(side_effect=bigquery_error)

        with pytest.raises(SearchError) as error:
            backend._execute_query(query, {})

        assert str(error.value) == (
            'Rate or quota limit exceeded; try again later'
        )

    def test_bigquery_rate_limit_includes_retry_after(self):
        response = SimpleNamespace(headers={'Retry-After': '30'})
        backend = DatastoreBigQueryBackend()
        query = Mock(side_effect=TooManyRequests(
            'Too many requests', response=response
        ))

        with pytest.raises(SearchError) as error:
            backend._execute_query(query, {})

        assert str(error.value) == (
            'Rate limit exceeded; try again in 30 seconds'
        )

    @pytest.mark.parametrize('bigquery_error, message', [
        (GatewayTimeout('Backend timeout'), '504 Backend timeout'),
        (TimeoutError('Query timed out after 60 seconds'),
         'Query timed out after 60 seconds'),
        (RetryError('Retry deadline exceeded', TimeoutError()),
         'BigQuery request failed after retries; try again later'),
        (ServiceUnavailable('Unavailable'),
         'BigQuery is temporarily unavailable; try again later'),
    ])
    def test_transient_bigquery_failure_returns_search_error(
            self, bigquery_error, message):
        backend = DatastoreBigQueryBackend()
        query = Mock(side_effect=bigquery_error)

        with pytest.raises(SearchError) as error:
            backend._execute_query(query, {})

        assert str(error.value) == message

    @helpers.change_config('ckanext.bigquery.query_timeout', '12')
    def test_search_sql_timeout_cancels_query(self):
        client = self._search_sql_client()
        query_job = client.bqclient_readonly.query.return_value
        query_job.result.side_effect = TimeoutError

        with pytest.raises(QueryTimeoutError) as error:
            client.search_sql_normal('SELECT * FROM `PCA_202501`')

        assert str(error.value) == 'Query timed out after 12 seconds'
        query_job.cancel.assert_called_once_with()

    def test_search_sql_zero_rows_has_success_message(self):
        client = self._search_sql_client()
        rows = MagicMock(total_rows=0)
        rows.__iter__.return_value = iter([])
        client.bqclient_readonly.query.return_value.result.return_value = rows

        result = client.search_sql_normal('SELECT * FROM `PCA_202501`')

        assert result['result']['records'] == []
        assert result['result']['message'] == (
            'Query executed successfully, returned 0 rows'
        )

    def test_bulk_export_has_row_limit_warning(self):
        client = Client.__new__(Client)
        client._query_history_lookup = Mock(return_value=(
            [{'result': '[]'}], 'encoded query', 'modified time'
        ))

        result = client.bulk_export(
            'SELECT * FROM `PCA_202501`', row_limit=32000
        )

        assert result['warning'] == (
            'Query exceeded the maximum inline result limit of 32000 rows; '
            'use gc_urls or reduce the result with filters'
        )

    @staticmethod
    def _search_sql_client():
        client = Client.__new__(Client)
        client.bqclient_readonly = Mock()
        client.job_config = Mock()
        client.log_data = {}
        client.resource_details = {}
        client.checkUserAgent = Mock(return_value=True)
        client.create_egress_log = Mock()
        query_job = client.bqclient_readonly.query.return_value
        query_job._properties = {'statistics': {}}
        return client

expected1 = {
        u'BNF_CODE': u'0304010I0AAAAAA', u'TOTAL_QUANTITY': 56.0, u'POSTCODE': u'PR1 6YA', 
        u'YEAR_MONTH': 201401, u'UNIDENTIFIED': False, u'PRACTICE_NAME': u'ISSA MEDICAL CENTRE - KHAN',
        u'BNF_CHAPTER_PLUS_CODE': u'03: Respiratory System', u'ACTUAL_COST': 2.70676, u'QUANTITY': 7.0, 
        u'REGIONAL_OFFICE_CODE': u'Y54', u'ITEMS': 8, u'ADDRESS_4': u'LANCASHIRE',
        u'AREA_TEAM_CODE': u'Q47', u'ADDRESS_2': u'DEEPDALE', u'ADDRESS_3': u'PRESTON', 
        u'BNF_CHEMICAL_SUBSTANCE': u'0304010I0', u'ADQUSAGE': 56.0, u'PCO_CODE': u'01E00',
        u'REGIONAL_OFFICE_NAME': u'NORTH OF ENGLAND', u'NIC': 1.92, 
        u'CHEMICAL_SUBSTANCE_BNF_DESCR': u'Cetirizine hydrochloride', u'PRACTICE_CODE': u'P81705', 
        u'PCO_NAME': u'GREATER PRESTON CCG', u'AREA_TEAM_NAME': u'LANCASHIRE AREA', 
        u'BNF_DESCRIPTION': u'Cetirizine 10mg tablets', u'ADDRESS_1': u'73 ST GREGORY ROAD'
    }
