import os

from google.cloud import bigquery
from api_tracker import tables_in_query
import logging

log = logging.getLogger(__name__)

class Client(object):
    def __init__(self, project_id, dataset):
        self.project_id = project_id
        self.dataset = dataset
        dataset = '%s.%s' % (self.project_id, self.dataset)
        job_config = bigquery.job.QueryJobConfig(default_dataset=dataset)
        self.bqclient = bigquery.Client(default_query_job_config=job_config)

    def _table_id(self, table_name):
        return '%s.%s.%s' % (self.project_id, self.dataset, table_name)

    def search(self, data_dict):
        '''
            Search BigQuery
            Parameters:
                data_dict - list of parameters that we need to search by
                TODO: add data_dict = add an example of format
        '''
        results = self.search_raw(data_dict)
        # TODO: set resource_id, set _links, handle errors, set fields, ...
        out = {
            "help": "https://demo.ckan.org/api/3/action/help_show?name=datastore_search",
            "success": True,
            "result": {
                "include_total": True,
                "resource_id": data_dict['resource_id'],
                "fields": [ ],
                "records_format": "objects",
                "records": results,
                "_links": {
                "start": "/api/3/action/datastore_search?resource_id="+data_dict['resource_id'],
                "next": "/api/3/action/datastore_search?offset=100&resource_id="+data_dict['resource_id']
                },
                "total": len(results)
            }
        }
        return out

    def search_raw(self, data_dict=None, **kwargs):
        '''Search bigquery and return raw results.
        
        Allow passing a dict or key word arguments
        '''
        if data_dict:
            print(data_dict)
            _kwargs = dict(data_dict)
        else:
            _kwargs = kwargs
        _kwargs['table'] = _kwargs['resource_id']
        # default for limit is 100
        _kwargs['limit'] = min(10000, _kwargs.get('limit', 100))

        query = 'SELECT * FROM `{table}` '.format(**_kwargs)
        if 'field' in _kwargs:
            query += ' WHERE {field} '.format(**_kwargs)
        if 'sort' in _kwargs:
            query += ' ORDER BY {sort} '.format(**_kwargs)
        query +=  ' LIMIT {limit}'.format(**_kwargs)

        query_job = self.bqclient.query(query)
        rows = query_job.result()
        records = [dict(row) for row in rows]
        return records

    def search_sql(self, sql):
        '''
        NB: table must be full table id ...
        '''
        # limit the number of results to ckan.datastore.search.rows_max + 1
        # (the +1 is so that we know if the results went over the limit or not)
        try:
             # check rows_max parameter set in CKAN config,
             # while testing as microlibrary (not as ckan ext) ckan is unknown
            from ckan.common import config
            rows_max = int(config.get('ckan.datastore.search.rows_max', 32000))
        except:
            rows_max = 32000 # set default rows limit 
        

        # limit the number of results to return by rows_max
        sql_initial = sql
        sql = 'SELECT * FROM ({0}) AS blah LIMIT {1} ;'.format(sql, rows_max+1)
        query = sql
        
        #total_bytes_processed = self.search_sql_dry_run(query)
        #log.warning("This query: {0} will process {1} bytes.".format(query,total_bytes_processed))
        
        query_job = self.bqclient.query(query)
        
        
        rows = query_job.result() 
        records = [dict(row) for row in rows]
        
        
        # check if results truncated ...
        if len(records) == rows_max + 1:
            
            sql_query_job = self.bqclient.query(sql_initial)
            destination_table = sql_query_job.destination
            log.warning("destination table: {}".format(destination_table))
            destination_uri = self.extract_table(destination_table, sql_initial)
            log.warning("extract job result: {}".format(destination_uri))
            return {
                "help":"https://demo.ckan.org/api/3/action/help_show?name=datastore_search_sql",
                "success": "true",
                "records_truncated": "true",
                "gc_url": destination_uri,
                "result":{
                    "records": records,
                    "fields": []
                }
            }
        else:
            return {
                    "help":"https://demo.ckan.org/api/3/action/help_show?name=datastore_search_sql",
                    "success": "true",
                    "result":{
                        "records": records,
                        "fields": []
                    }
                }

    def extract_table(self, table_ref, sql):
        bucket_name = 'datopian-test'
        project = "bigquerytest-271707"
        dataset_id = "nhs_test"
        table_id = tables_in_query(sql)
        job_config = bigquery.job.ExtractJobConfig()
        job_config.compression = bigquery.Compression.GZIP
        destination_uris = "gs://{}/{}/{}".format(bucket_name, table_ref.table_id, table_id+"-*.csv.gz")
        extract_job = self.bqclient.extract_table(
            table_ref,
            destination_uris = [destination_uris],
            # Location must match that of the source table.
            location="europe-west2",
            job_config=job_config
        )  # API request
        res = extract_job.result()  # Waits for job to complete.
        res_destination_uris = res.destination_uris
        log.warning("extract_job result: {}".format(res_destination_uris))
        log.warning(
            "Exported {}:{}.{} to {}".format(project, dataset_id, table_ref, res_destination_uris)
        )
        return res_destination_uris

    def search_sql_dry_run(self, query):

        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        query_job = self.bqclient.query(query, job_config=job_config)
        # A dry run query completes immediately.

        #records = [dict(row) for row in rows]
        log.warning("records len: {}".format(query_job.total_bytes_processed))
        return  query_job.total_bytes_processed
        

'''
    def search_filters(self, table, filter1, filter2):
        client = bigquery.Client()
        projectid = self.project_id
        dataset = 'NHS'
        query = 'SELECT * FROM `%s.%s.%s` WHERE %s AND %s LIMIT 10' % (projectid, dataset, table, filter1, filter2)
        query_job = client.query(query)
        rows = query_job.result()
        records = [dict(row) for row in rows]
        return records

    def search_free_text(self, table, q):
        # TODO: investigate how we can search through all the columns in BigQuery
        # We don't need this for now since the client is only going to filer data
        # not search by it
        client = bigquery.Client()
        projectid = self.project_id
        dataset = 'NHS'
        query = 'SELECT * FROM `%s.%s.%s`' \
                'WHERE REGEXP_CONTAINS(TO_JSON_STRING(%s), %s)' \
                'LIMIT 5' % (projectid, dataset, table, table, q)
        query_job = client.query(query)
        rows = query_job.result()
        records = [dict(row) for row in rows]
        return records

    def search_distinct(self, table, distinct):
        client = bigquery.Client()
        projectid = self.project_id
        dataset = 'NHS'
        query = 'SELECT %s * FROM `%s.%s.%s` LIMIT 2' % (distinct, projectid, dataset, table)
        query_job = client.query(query)
        rows = query_job.result()
        records = [dict(row) for row in rows]
        return records

    def search_field(self, table, filter):
        client = bigquery.Client()
        projectid = self.project_id
        dataset = 'NHS'
        query = 'SELECT %s FROM `%s.%s.%s` LIMIT 10' % (filter, projectid, dataset, table)
        query_job = client.query(query)
        rows = query_job.result()
        records = [dict(row) for row in rows]
        return records

    def search_fields(self, table, filter1, filter2):
        client = bigquery.Client()
        projectid = self.project_id
        dataset = 'NHS'
        query = 'SELECT %s, %s FROM `%s.%s.%s` LIMIT 10' % (filter1, filter2, projectid, dataset, table)
        query_job = client.query(query)
        rows = query_job.result()
        records = [dict(row) for row in rows]
        return records

    def table_schema(self, table):
        client = bigquery.Client()
        projectid = self.project_id
        dataset = 'NHS'
        query = 'SELECT * FROM `%s.%s.%s` LIMIT 10' % (projectid, dataset, table)
        query_job = client.query(query)
        rows = query_job.result()
        schema = rows.schema
        return schema
'''
