"""Tests for plugin.py."""
import ckanext.bigquery.plugin as plugin
import nose
from ckan.common import config
from google.cloud import bigquery

assert_equals = nose.tools.assert_equals
assert_raises = nose.tools.assert_raises
assert_in = nose.tools.assert_in

credentials = config.get('google_cloud_credentials', None)
client = bigquery.Client.from_service_account_json(credentials)

def test_plugin():
    pass

def test_connect_bigquery():
    '''
        Test connection to BigQuery
        Parameters that need to be set are the following:
            - credentials.json file from Google Cloud
    '''
    credentials = config.get('google_cloud_credentials', None)
    if credentials is None:
        assert_equals(credentials, None)

    # init BiqQuery client
    client = bigquery.Client.from_service_account_json(credentials)


def test_search_bigquery():
    QUERY = (
        'SELECT * FROM `bigquerytest-271707.NHS.201401` '
        'LIMIT 100')

    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish