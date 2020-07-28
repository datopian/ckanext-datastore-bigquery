import os
import requests
from ckan.common import config
import logging

def ga_api_tracker(resource_id):
    tid = config.get('googleanalytics.id', None)
    endpoint = config.get('googleanalytics.endpoint', None)
    client_id = config.get('googleanalytics.client.id', None)
    payload = {
        'v': 1, # Version.
        'tid': tid, # Tracking ID
        'cid': client_id, # Anonymous Client ID
        't': 'event', # Hit type (event, pageview, etc)
        'ec': 'Resource', # Event Category
        'ea': 'API', # Event Action
        'el': resource_id # Event label
    }
    #logging.info('sending payload to GA: {}'.format(payload))
    r = requests.post(endpoint, data=payload, headers={'User-Agent': 'User Agent 1.0'})
    
    if r.ok:
        logging.info('ga for search api sent')
    else:
        logging.info('ga for search api failed')