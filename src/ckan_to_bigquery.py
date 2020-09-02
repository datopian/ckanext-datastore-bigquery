import os

from google.cloud import bigquery
from api_tracker import tables_in_query
from google.cloud import storage
from google.api_core import exceptions
from google.api_core import retry
from google.api_core.retry import if_exception_type

from ckan.common import config
import logging

log = logging.getLogger(__name__)

class Client(object):
    def __init__(self, project_id, dataset, creds, read_only_creds):
        self.project_id = project_id
        self.dataset = dataset
        dataset = '%s.%s' % (self.project_id, self.dataset)
        self.job_config = bigquery.job.QueryJobConfig(default_dataset=dataset)
        self.bqclient = bigquery.Client.from_service_account_json(creds)
        self.bqclient_readonly = bigquery.Client.from_service_account_json(read_only_creds)
        self.storage_client = storage.Client.from_service_account_json(creds)

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

        query_job = self.bqclient_readonly.query(query, job_config=self.job_config)
        rows = query_job.result()
        records = [dict(row) for row in rows]
        return records

    def search_sql_normal(self, sql):
        '''
        NB: table must be full table id ...
        '''
        log.warning("***** search_sql *****")
        log.warning("sql = {}".format(sql))
        # limit the number of results to ckan.datastore.search.rows_max + 1
        # (the +1 is so that we know if the results went over the limit or not)
        rows_max = int(config.get('ckan.datastore.search.rows_max', 32000))
        sql_initial = sql
        # limit the number of results to return by rows_max
        sql = 'SELECT * FROM ({0}) AS blah LIMIT {1} ;'.format(sql, rows_max+1)
        query_job = self.bqclient_readonly.query(sql, job_config=self.job_config)
        rows = query_job.result() 
        records = [dict(row) for row in rows]
        # check if results truncated ...
        if len(records) == rows_max + 1:
           return self.bulk_export(sql_initial)
        else:
            # do normal
            return {
                    "help":"https://demo.ckan.org/api/3/action/help_show?name=datastore_search_sql",
                    "success": "true",
                    "result":{
                        "records": records,
                        "fields": []
                    }
                }

    def search_sql(self, data_dict):
        # default is_bulk export value
        is_bulk = False
        is_bulk = bool('bulk' in data_dict)
        log.warning("is_bulk - {}".format(is_bulk))
        if is_bulk:
            # do bulk export
            return self.bulk_export(data_dict['sql'])
        else:
            log.warning("do standard search_sql")
            return self.search_sql_normal(data_dict['sql'])

    def bulk_export(self, sql_initial):
        try:
            sql_query_job = self.bqclient.query(sql_initial, job_config=self.job_config)
            # get temp table containing query result
            destination_table = sql_query_job.destination
            log.warning("destination table: {}".format(destination_table))
            destination_urls = self.extract_query_to_gcs(destination_table, sql_initial)
            log.warning("extract job result: {}".format(destination_urls))
            return {
                "help":"https://demo.ckan.org/api/3/action/help_show?name=datastore_search_sql",
                "success": "true",
                "records_truncated": "true",
                "gc_urls": destination_urls
            }
        except Exception as ex:
            log.error("Error: {}".format(str(ex)))

    @retry.Retry(predicate=if_exception_type(exceptions.NotFound))
    def extract_query_to_gcs(self, table_ref, sql):
        '''Take query results from temp table and extract to gcs bucket
        
        Parameters:
        table_ref: temp table with the query results
        sql: sql statement to get table name from

        Returns:
        res_destination_urls: gcs files public url(s) contain extracted query results
        '''
        bucket_name = config.get('ckanext.bigquery.bucket', None)
        location = config.get('ckanext.bigquery.location', None)
        table_name = tables_in_query(sql)
        job_config = bigquery.job.ExtractJobConfig()
        job_config.compression = bigquery.Compression.GZIP
        destination_uris = "gs://{}/{}/{}".format(bucket_name, table_ref.table_id, table_name+"-*.csv.gz")
        # extract table into Cloud Storage files.
        extract_job = self.bqclient.extract_table(
            table_ref,
            destination_uris = [destination_uris],
            # Location must match that of the source table.
            location=location,
            job_config=job_config
        )  # API request
        res = extract_job.result()  # Waits for job to complete.
        res_destination_uris = res.destination_uris
        log.warning(
            "Exported {} to {}".format(table_ref, res_destination_uris)
        )
        # gc blob containing query result prefix 
        prefix = table_ref.table_id+ '/'
        log.warning("Prefix: {}".format(prefix))
        res_destination_urls = self.retrieve_gc_urls(bucket_name, prefix)   
        return res_destination_urls
       
    def retrieve_gc_urls(self, bucket_name, prefix=''):
        #client = storage.Client()
        objects = self.storage_client.list_blobs(bucket_name, prefix=prefix)
        objects_as_files = [ 
                self._gcs_object_to_file_url(fileinfo, bucket_name) for fileinfo in objects
            ]
        return objects_as_files

    def _gcs_object_to_file_url(self, obj, bucket_name):
        return {
            'url': 'https://storage.googleapis.com/'+ bucket_name + '/' + obj.name 
        }  

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
