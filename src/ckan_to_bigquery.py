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

