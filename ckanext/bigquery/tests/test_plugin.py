"""Tests for plugin.py."""
# encoding: utf-8

from mock import patch, Mock, call
from nose.tools import (
    assert_equal,
    assert_in,
    assert_is_instance,
    assert_raises
)

import ckan.plugins as plugins
from ckan.common import config
import ckan.tests.factories as factories
import ckan.tests.helpers as helpers
from ckanext.datastore.backend import DatastoreBackend
from ckanext.datastore.backend.postgres import DatastorePostgresqlBackend
from ckanext.bigquery.backend.bigquery import DatastoreBigQueryBackend


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
        assert_is_instance(
            DatastoreBackend.get_active_backend(),
            DatastoreBigQueryBackend)

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

    @patch(u'ckanext.bigquery.backend.bigquery.ga_api_tracker')
    @patch(u'ckanext.bigquery.backend.bigquery.toolkit.get_action')
    def test_uuid_resource_id_uses_bigquery_table_name(
            self, get_action, ga_api_tracker):
        resource_id = u'e4e5508c-3f9e-4676-b5bb-e126ef724060'
        resource_show = Mock(return_value={
            u'bq_table_name': u'EPD_SNOMED_202011'
        })
        get_action.return_value = resource_show
        engine = Mock()
        engine.search.return_value = {u'resource_id': resource_id}
        backend = DatastoreBigQueryBackend()
        backend._get_engine = Mock(return_value=engine)
        data_dict = {u'resource_id': resource_id, u'limit': 1}

        result = backend.search({}, data_dict)

        resource_show.assert_called_once_with({}, {u'id': resource_id})
        engine.search.assert_called_once_with({
            u'resource_id': resource_id,
            u'bq_table_name': u'EPD_SNOMED_202011',
            u'limit': 1,
        })
        assert_equal(result[u'resource_id'], resource_id)
        assert_equal(data_dict, {u'resource_id': resource_id, u'limit': 1})

    @patch(u'ckanext.bigquery.backend.bigquery.ga_api_tracker')
    @patch(u'ckanext.bigquery.backend.bigquery.toolkit.get_action')
    def test_table_name_resource_id_does_not_call_resource_show(
            self, get_action, ga_api_tracker):
        engine = Mock()
        backend = DatastoreBigQueryBackend()
        backend._get_engine = Mock(return_value=engine)
        data_dict = {u'resource_id': u'EPD_SNOMED_202011'}

        backend.search({}, data_dict)

        assert_equal(get_action.call_count, 0)
        engine.search.assert_called_once_with(data_dict)

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
