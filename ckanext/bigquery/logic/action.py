# encoding: utf-8

import logging
import json
import datetime

from dateutil.parser import parse as parse_date

import ckan.lib.navl.dictization_functions
import ckan.logic as logic
import ckan.plugins as p
from ckan.logic import side_effect_free

log = logging.getLogger(__name__)
_get_or_bust = logic.get_or_bust
_validate = ckan.lib.navl.dictization_functions.validate


@logic.side_effect_free
def bigquery_datastore_search(context, data_dict):
    log.info('We are calling from bigquery')
    result = None
    return result
