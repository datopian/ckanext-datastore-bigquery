"""Tests for plugin.py."""
import ckanext.bigquery.plugin as plugin
import nose
from ckan.common import config
from google.cloud import bigquery

assert_equals = nose.tools.assert_equals
assert_raises = nose.tools.assert_raises
assert_in = nose.tools.assert_in

def test_plugin():
    pass

def test_connect_bigquery():
    '''
        Test connection to BigQuery
        Parameters that need to be set are the following:
            - credentials.json file from Google Cloud
    '''
    credentials = config.get('google_cloud_credentials', None)
    assert_equals(None, credentials, None)
    client = bigquery.Client.from_service_account_json(credentials)

    return client