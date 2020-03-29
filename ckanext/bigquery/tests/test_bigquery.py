# encoding: utf-8

import os
import json
import ckanext.datastore_bigquery.plugin as plugin
from google.cloud import bigquery
from google.cloud import storage
from ckan.common import config
from ckanext.bigquery.tests.helpers import DatastoreBigQueryFunctionalTestBase

# read and setup the credentials for bigquery
def test_search():
    print 'We are in test search'


def test_connect_bigquery():
    '''
        Test connection to BigQuery
        Parameters that need to be set are the following:
            - credentials.json file from Google Cloud
    '''
    credentials = config.get('google_cloud_credentials', None)
    client = bigquery.Client.from_service_account_json(credentials)

    return client


class TestDatastoreBigQuerySearchNewTest(DatastoreBigQueryFunctionalTestBase):
    # read credentials and setup connection to big query
    print 'Testin BigQuer'
    client = test_connect_bigquery()


