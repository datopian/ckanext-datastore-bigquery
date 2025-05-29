import os
import requests
from ckan.common import config
import ckan.plugins.toolkit as tk
import hashlib
import logging
import re


log = logging.getLogger(__name__)

def ga_api_tracker(resource_id):
    try:
        tid = config.get('googleanalytics.id', None)
        endpoint = config.get('googleanalytics.endpoint', None)
        client_id = hashlib.md5(tk.current_user.name).hexdigest()
        payload = {
            'v': 1, # Version.
            'tid': tid, # Tracking ID
            'cid': client_id, # Anonymous Client ID
            't': 'event', # Hit type (event, pageview, etc)
            'ec': 'Resource', # Event Category
            'ea': 'API', # Event Action
            'el': resource_id # Event label
        }
        log.warning('sending payload to GA: {}'.format(payload))
        r = requests.post(endpoint, data=payload, headers={'User-Agent': 'User Agent 1.0'})
        if r.ok:
            log.info('ga for search api sent')
        else:
            log.info('ga for search api failed')
    except Exception as e:
        log.error(str(e))

def ga_search_sql_api_tracker(sql):
    try:
        # try to extract table name from sql statement
        table = tables_in_query(sql)
        log.warning('table name: {}'.format(table))
        if (table):
            ga_api_tracker(table)
    except Exception as e:
        log.error(str(e))


def tables_in_query(sql_str):
    try:
        # remove the /* */ comments
        q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)
        # remove whole line -- and # comments
        lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]
        # remove trailing -- and # comments
        q = " ".join([re.split("--|#", line)[0] for line in lines])
        # split on blanks, parens and semicolons
        tokens = re.split(r"[\s)(;]+", q)
        # scan the tokens. if we see a FROM, we set the get_next
        # flag, and grab the next one (unless it's SELECT).
        result = ''
        get_next = False
        for tok in tokens:
            if get_next:
                if tok.lower() not in ["", "select"]:
                    result = tok 
                get_next = False
            get_next = tok.lower() in ["from"]
        return result
    except Exception as e:
        log.error(str(e))
