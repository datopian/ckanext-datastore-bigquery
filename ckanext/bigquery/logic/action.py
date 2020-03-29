# encoding: utf-8

import logging
import json
import datetime

from dateutil.parser import parse as parse_date

import ckan.lib.navl.dictization_functions
import ckan.logic as logic
import ckan.plugins as p
from ckan.logic import side_effect_free


def bigquery_datastore_search(context, data_dict):
    print 'We are in bigquery'
    result = None
    return result
