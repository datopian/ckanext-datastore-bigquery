# encoding: utf-8
from ckan.tests.helpers import FunctionalTestBase

class DatastoreBigQueryFunctionalTestBase(FunctionalTestBase):
    _load_plugins = (u'bigquery', )