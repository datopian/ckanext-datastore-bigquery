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

        res_id = '201401'
        project_id = config.get('ckanext.bigquery.project', None)
        dataset = 'NHS'

        out = helpers.call_action(u'datastore_search', resource_id=res_id)
        assert out['result']['total'] == 10
        first = out['result']['records'][0]
        assert first == expected1

expected1 = {
        'ACTUAL_COST': 6.59007,
        'ADDRESS_1': '-', 'ADDRESS_2': '-', 'ADDRESS_3': '-', 'ADDRESS_4': '-',
        'ADQUSAGE': 0.0, 'AREA_TEAM_CODE': '-', 'AREA_TEAM_NAME': 'UNIDENTIFIED',
        'BNF_CHAPTER_PLUS_CODE': '20: Dressings',
        'BNF_CHEMICAL_SUBSTANCE': '2003',
        'BNF_CODE': '20030100373',
        'BNF_DESCRIPTION': 'Softpore dressing 10cm x 20cm',
        'CHEMICAL_SUBSTANCE_BNF_DESCR': 'Wound Management & Other Dressings',
        'ITEMS': 2, 'NIC': 7.0, 'PCO_CODE': '-', 'PCO_NAME': 'UNIDENTIFIED',
        'POSTCODE': '-',
        'PRACTICE_CODE': '-', 'PRACTICE_NAME': 'UNIDENTIFIED DOCTORS',
        'QUANTITY': 10.0,
        'REGIONAL_OFFICE_CODE': '-', 'REGIONAL_OFFICE_NAME':
        'UNIDENTIFIED', 'TOTAL_QUANTITY': 20.0, 'UNIDENTIFIED': True,
        'YEAR_MONTH': 201401
    }
