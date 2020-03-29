import os

from google.cloud import bigquery

def search(table):
    client = bigquery.Client()
    projectid = os.environ['BIGQUERY_PROJECT_ID']
    dataset = 'NHS'

    # Perform a query.
    query = 'SELECT * FROM `%s.%s.%s` LIMIT 10' % (projectid, dataset, table)
    query_job = client.query(query)
    rows = query_job.result()
    records = [dict(row) for row in rows]
    return records

