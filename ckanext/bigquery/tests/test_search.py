# encoding: utf-8

import json
import nose
import urllib
import pprint

import ckan.plugins as p
import ckan.lib.create_test_data as ctd
import ckan.model as model
import ckan.tests.legacy as tests

from ckanext.bigquery.tests.test_helpers import DatastoreBigQueryFunctionalTestBase

import ckan.tests.helpers as helpers
import ckan.tests.factories as factories

assert_equals = nose.tools.assert_equals
assert_raises = nose.tools.assert_raises
assert_in = nose.tools.assert_in


class TestSearchAction(DatastoreBigQueryFunctionalTestBase):
    def test_action_bigquery_datastore_search(self):
        # define resource and search_data
        # we need the resource id in order to be able to search in the
        # correct BigQuery table
        resource = factories.Resource()
        assert_equals(resource, None)
        search_data = {
            'resource_id': resource['id'],
            'fields': 'from',
            'q': {
                'from': 'Brazil'
            },
        }

        result = helpers.call_action('datastore_search', **search_data)
        assert_equals(len(result['records']), 1)


