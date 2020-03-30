import os

from google.cloud import bigquery

def search(table):
    results = search_raw(table)
    # TODO: set resource_id, set _links, handle errors, set fields, ...
    out = {
        "help": "https://demo.ckan.org/api/3/action/help_show?name=datastore_search",
        "success": True,
        "result": {
            "include_total": True,
            "resource_id": "553a4d08-7670-48c2-a994-9e180131b22e",
            "fields": [ ],
            "records_format": "objects",
            "records": results,
            "_links": {
              "start": "/api/3/action/datastore_search?resource_id=553a4d08-7670-48c2-a994-9e180131b22e",
              "next": "/api/3/action/datastore_search?offset=100&resource_id=553a4d08-7670-48c2-a994-9e180131b22e"
             },
            "total": len(results)
          }
      }
    return out

def search_raw(table):
    client = bigquery.Client()
    projectid = os.environ['BIGQUERY_PROJECT_ID']
    dataset = 'NHS'
    query = 'SELECT * FROM `%s.%s.%s` LIMIT 10' % (projectid, dataset, table)
    query_job = client.query(query)
    rows = query_job.result()
    records = [dict(row) for row in rows]
    return records

def search_limit(table, limit):
    client = bigquery.Client()
    projectid = os.environ['BIGQUERY_PROJECT_ID']
    dataset = 'NHS'
    query = 'SELECT * FROM `%s.%s.%s` LIMIT %s' % (projectid, dataset, table, limit)
    query_job = client.query(query)
    rows = query_job.result()
    records = [dict(row) for row in rows]
    return records

def search_sort(table, sort):
    client = bigquery.Client()
    projectid = os.environ['BIGQUERY_PROJECT_ID']
    dataset = 'NHS'
    query = 'SELECT * FROM `%s.%s.%s` ORDER BY %s LIMIT 2' % (projectid, dataset, table, sort)
    query_job = client.query(query)
    rows = query_job.result()
    records = [dict(row) for row in rows]
    return records

def search_filter(table, filter):
    client = bigquery.Client()
    projectid = os.environ['BIGQUERY_PROJECT_ID']
    dataset = 'NHS'
    query = 'SELECT * FROM `%s.%s.%s` WHERE %s LIMIT 10' % (projectid, dataset, table, filter)
    query_job = client.query(query)
    rows = query_job.result()
    records = [dict(row) for row in rows]
    return records

def search_filters(table, filter1, filter2):
    client = bigquery.Client()
    projectid = os.environ['BIGQUERY_PROJECT_ID']
    dataset = 'NHS'
    query = 'SELECT * FROM `%s.%s.%s` WHERE %s AND %s LIMIT 10' % (projectid, dataset, table, filter1, filter2)
    query_job = client.query(query)
    rows = query_job.result()
    records = [dict(row) for row in rows]
    return records

def search_free_text(table, q):
    # TODO: investigate how we can search through all the columns in BigQuery
    # We don't need this for now since the client is only going to filer data
    # not search by it
    client = bigquery.Client()
    projectid = os.environ['BIGQUERY_PROJECT_ID']
    dataset = 'NHS'
    query = 'SELECT * FROM `%s.%s.%s`' \
            'WHERE REGEXP_CONTAINS(TO_JSON_STRING(%s), %s)' \
            'LIMIT 5' % (projectid, dataset, table, table, q)
    query_job = client.query(query)
    rows = query_job.result()
    records = [dict(row) for row in rows]
    return records

def search_distinct(table, distinct):
    client = bigquery.Client()
    projectid = os.environ['BIGQUERY_PROJECT_ID']
    dataset = 'NHS'
    query = 'SELECT %s * FROM `%s.%s.%s` LIMIT 2' % (distinct, projectid, dataset, table)
    query_job = client.query(query)
    rows = query_job.result()
    records = [dict(row) for row in rows]
    return records

def search_field(table, filter):
    client = bigquery.Client()
    projectid = os.environ['BIGQUERY_PROJECT_ID']
    dataset = 'NHS'
    query = 'SELECT %s FROM `%s.%s.%s` LIMIT 10' % (filter, projectid, dataset, table)
    query_job = client.query(query)
    rows = query_job.result()
    records = [dict(row) for row in rows]
    return records

def search_fields(table, filter1, filter2):
    client = bigquery.Client()
    projectid = os.environ['BIGQUERY_PROJECT_ID']
    dataset = 'NHS'
    query = 'SELECT %s, %s FROM `%s.%s.%s` LIMIT 10' % (filter1, filter2, projectid, dataset, table)
    query_job = client.query(query)
    rows = query_job.result()
    records = [dict(row) for row in rows]
    return records

def table_schema(table):
    client = bigquery.Client()
    projectid = os.environ['BIGQUERY_PROJECT_ID']
    dataset = 'NHS'
    query = 'SELECT * FROM `%s.%s.%s` LIMIT 10' % (projectid, dataset, table)
    query_job = client.query(query)
    rows = query_job.result()
    schema = rows.schema
    return schema