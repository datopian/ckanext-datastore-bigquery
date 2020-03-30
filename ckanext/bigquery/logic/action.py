# encoding: utf-8

import logging
import json
import datetime

from dateutil.parser import parse as parse_date

import ckan.lib.navl.dictization_functions
import ckan.logic as logic
import ckan.plugins as p
import ckanext.datastore.logic.schema as dsschema
import ckan.plugins.toolkit as toolkit

from ckanext.bigquery.backend.bigquery import DatastoreBigQueryBackend

log = logging.getLogger(__name__)
_get_or_bust = logic.get_or_bust
_validate = ckan.lib.navl.dictization_functions.validate


@toolkit.chained_action
def bigquery_datastore_search(action, context, data_dict):
    '''
        Override of datastore_search action call. This action call should take the exact same parameters
        that datastore_search is accepting.
        Link to datastore_search parameters:
        https://docs.ckan.org/en/2.8/maintaining/datastore.html#ckanext.datastore.logic.action.datastore_search

        The difference between datastore_search and bigquery_datastore_search is that we are going to connect to
        big query as a backend and execute the query there.
    '''
    log.info('We are calling from bigquery')
    # Do we need to get the active backend?
    backend = DatastoreBigQueryBackend.get_active_backend()
    print backend

    # Leave schema check last
    schema = context.get('schema', dsschema.datastore_search_schema())
    data_dict, errors = _validate(data_dict, schema, context)
    if errors:
       raise p.toolkit.ValidationError(errors)

    res_id = data_dict['resource_id']

    result = None

    return action(context, data_dict)
