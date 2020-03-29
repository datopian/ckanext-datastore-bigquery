# -*- coding: utf-8 -*-

import copy
import logging
import sys
import sqlalchemy
import os
import pprint
import sqlalchemy.engine.url as sa_url
import urllib
import urllib2
import urlparse
import datetime
import hashlib
import json
from cStringIO import StringIO

from six import string_types, text_type

import ckan.lib.cli as cli
import ckan.plugins as p
import ckan.plugins.toolkit as toolkit
from ckan.lib.lazyjson import LazyJSONObject

import ckanext.datastore.helpers as datastore_helpers
import ckanext.datastore.interfaces as interfaces

from psycopg2.extras import register_default_json, register_composite
import distutils.version
from sqlalchemy.exc import (ProgrammingError, IntegrityError,
                            DBAPIError, DataError)

import ckan.model as model
import ckan.plugins as plugins
from ckan.common import config, OrderedDict
from google.cloud import bigquery

from ckanext.datastore.backend import (
    DatastoreBackend,
    DatastoreException,
    _parse_sort_clause
)
from ckanext.datastore.backend import InvalidDataError

log = logging.getLogger(__name__)

_pg_types = {}
_type_names = set()
_engines = {}

_TIMEOUT = 60000  # milliseconds

# See http://www.postgresql.org/docs/9.2/static/errcodes-appendix.html
_PG_ERR_CODE = {
    'unique_violation': '23505',
    'query_canceled': '57014',
    'undefined_object': '42704',
    'syntax_error': '42601',
    'permission_denied': '42501',
    'duplicate_table': '42P07',
    'duplicate_alias': '42712',
}

_DATE_FORMATS = ['%Y-%m-%d',
                 '%Y-%m-%d %H:%M:%S',
                 '%Y-%m-%dT%H:%M:%S',
                 '%Y-%m-%dT%H:%M:%SZ',
                 '%d/%m/%Y',
                 '%m/%d/%Y',
                 '%d-%m-%Y',
                 '%m-%d-%Y']

_INSERT = 'insert'
_UPSERT = 'upsert'
_UPDATE = 'update'


if not os.environ.get('DATASTORE_LOAD'):
    ValidationError = toolkit.ValidationError
else:
    log.warn("Running datastore without CKAN")

    class ValidationError(Exception):
        def __init__(self, error_dict):
            pprint.pprint(error_dict)

is_single_statement = datastore_helpers.is_single_statement

_engines = {}


def literal_string(s):
    """
    Return s as a postgres literal string
    """
    return u"'" + s.replace(u"'", u"''").replace(u'\0', '') + u"'"


def identifier(s):
    """
    Return s as a quoted postgres identifier
    """
    return u'"' + s.replace(u'"', u'""').replace(u'\0', '') + u'"'


def _connect_to_bigquery():
    credentials = config.get('google_cloud_credentials', None)
    project = config.get('project', None)

    client = bigquery.Client(credentials, project)

    return client


class DatastoreBigQueryBackend(DatastoreBackend):
    # get bigquery db
    def _get_bigquery_db(self):
        return _connect_to_bigquery()


    def _log_or_raise(self, message):
        if self.config.get('debug'):
            log.critical(message)
        else:
            raise DatastoreException(message)


