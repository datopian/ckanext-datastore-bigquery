# -*- coding: utf-8 -*-
import os
import logging

from google.cloud import bigquery


def get_google_client(credentials):
    '''
        Connnect to BigQuery.
        Parameters:
            credentials - path to the .json file. Read from .env or ckan.ini file
    '''
    big_query = bigquery.Client.from_service_account_json(credentials)
    return big_query


def search(client, project_id, data_dict):
    '''
        Search BigQuery
        Parameters:
            client - initialized BiqQuery client
            project_id - name of the BigQuery project. Read from .env or ckan.ini file
            data_dict - list of parameters that we need to search by
    '''
    return None


def output_format(results):
    '''
        A function for setting up the output in CKAN format
    '''
    out = {}
    return out