"""Tests for plugin.py."""
# encoding: utf-8

import pytest
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

    @patch(class_to_patch + u'._get_engine')
    def test_backend_functionality(self, get_engine):
        engine = get_engine()
        execute = engine.execute
        fetchall = execute().fetchall
        execute.reset_mock()

        DatastoreBackend.set_active_backend(config)

        execute.reset_mock()
        res_id = '201401'
        project_id = config.get('ckanext.bigquery.project', None)
        dataset = 'NHS'

        # Before exec this we need to implement the search funtion
        helpers.call_action(
            u'datastore_search', resource_id=res_id)
        execute.assert_called_with(
            u'SELECT * FROM "{0}.{1}.{2}" LIMIT 10'.format(project_id, dataset, res_id)
        )