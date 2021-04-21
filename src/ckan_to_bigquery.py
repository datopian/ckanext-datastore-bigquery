import os

from google.cloud import bigquery
from api_tracker import tables_in_query
from google.cloud import storage
from google.api_core import exceptions
from google.api_core import retry
from google.api_core.retry import if_exception_type

from ckan.common import config, request
import ckan.plugins.toolkit as toolkit
import logging
import datetime

log = logging.getLogger(__name__)

class Client(object):
    def __init__(self, project_id, dataset, creds, read_only_creds):
        self.project_id = project_id
        self.dataset = dataset
        self.log_data = {}
        self.resource_details = {}
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
        include_total = False
        include_total = bool('include_total' in data_dict) and data_dict.get('include_total', False)

        # Fetching the metadata of the table from dataset for the schema and total count
        # Will reduce the API calls by 2 for each call to datastore_search

        dataset_ref = self.bqclient_readonly.dataset(self.dataset, project=self.project_id)
        table_ref = dataset_ref.table(data_dict['resource_id'])
        start = str(datetime.datetime.now())
        table_meta_data = self.bqclient_readonly.get_table(table_ref)  # API call
        log.warning("table_meta_data {}".format(table_meta_data))
        fields = self.table_schema_from_bq_schema(table_meta_data.schema)
        log.warning('Data_dict {}'.format(data_dict.get('__extras')))
        if '__extras' in data_dict:
            self.log_data['api_call_type'] = data_dict.get('__extras').get('api_call_type')
        self.resource_details['big_query_resource_name'] = data_dict.get('resource_id')

        end = str(datetime.datetime.now())
        schema_data = {
            "start": start,
            "end": end,
            "total_count": table_meta_data.num_rows,
            "table_size": table_meta_data.num_bytes
        }
        self.create_egress_log(schema_data=schema_data)
        query_fields = []
        if len(data_dict.get('fields', [])) > 0:
            
            data_fields = data_dict.get('fields')
            for x in fields:
                if x['id'] in data_fields:
                    field = {'id' : x['id'], 'type' : x['type']}
                    query_fields.append(field)

        results = self.search_raw(fields, query_fields, data_dict)
        if include_total:
            total = table_meta_data.num_rows
        else:
            total = len(results)
        self.create_egress_log()
        out = {
            "include_total": include_total,
            "resource_id": data_dict['resource_id'],
            "fields": fields,
            "records_format": "objects",
            "records": results,
            "_links": {
            "start": "/api/3/action/datastore_search?resource_id="+data_dict['resource_id'],
            "next": "/api/3/action/datastore_search?offset=100&resource_id="+data_dict['resource_id']
            },
            "total": total
        }
        return out

    def search_raw(self, fields, query_fields, data_dict=None, **kwargs):
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
        _kwargs['limit'] =  _kwargs.get('limit', 100)
        if 'distinct' in _kwargs:
            distinct = 'DISTINCT'
        else:
            distinct = ''
        if 'fields' in _kwargs and len(query_fields) > 0:
            ## string
            string_fields = ''
            for field in query_fields:
                string_fields += str(field['id'])
                string_fields += ','
            query = 'SELECT {0} {1} '.format(distinct, string_fields, **_kwargs)
        else:
            query = 'SELECT * '
        query += ' FROM `{table}` '.format(**_kwargs)
        #if 'field' in _kwargs:
        #   query += ' WHERE {field} '.format(**_kwargs)
        if 'filters' or 'q' in _kwargs:
            query += ' {0} '.format(self.where_clauses(fields, data_dict))
        if 'sort' in _kwargs:
            query += ' ORDER BY {sort} '.format(**_kwargs)
        query +=  ' LIMIT {limit}'.format(**_kwargs)
        if 'offset' in _kwargs:
            query += ' OFFSET {offset}'.format(**_kwargs)
        log.warning("query - {}".format(query))
        self.log_data['query'] = query
        query_job = self.bqclient_readonly.query(query, job_config=self.job_config)
        rows = query_job.result()
        self.log_data['job_details'] = query_job._properties.get('statistics')
        records = [dict(row) for row in rows]
        return records
    
    def get_total_num_of_query_rows(self, fields, data_dict=None, **kwargs):
        if data_dict:
            print(data_dict)
            _kwargs = dict(data_dict)
        else:
            _kwargs = kwargs
        _kwargs['table'] = _kwargs['resource_id']

        query = 'SELECT * FROM `{table}` '.format(**_kwargs)
        if 'filters' in _kwargs:
            log.warning("filters in _kwargs")
            query += ' {0} '.format(self.where_clauses(fields, data_dict))

        count_sql_string = 'SELECT count(*) FROM ({0}) AS blah ;'.format(query)
        log.warning("count_sql_string: {}".format(count_sql_string))

        query_job = self.bqclient_readonly.query(count_sql_string, job_config=self.job_config)
        rows = query_job.result() 
        records = [dict(row) for row in rows]
        log.warning("records {}".format(records[0]['f0_']))
        return records[0]['f0_']

    def get_bq_table_schema(self, data_dict=None, **kwargs):
        if data_dict:
            print(data_dict)
            _kwargs = dict(data_dict)
        else:
            _kwargs = kwargs
        _kwargs['table'] = _kwargs['resource_id']

        query = 'SELECT * FROM `{table}` limit 1 '.format(**_kwargs)
        
        query_job = self.bqclient_readonly.query(query, job_config=self.job_config)
        rows = query_job.result()
        bq_table_schema = rows.schema
        return bq_table_schema

    def table_schema_from_bq_schema(self, bq_schema):
        table_fields = [{"id": field.name, "type": field.field_type} for field in bq_schema]
        return table_fields

    def get_field_type(self, fields, field):
        string_types = ['STRING']
        #num_types = ['INTEGER', 'FLOAT']
        field_type = [x['type'] for x in fields if x['id'] == field ]
        log.warning("field_type - {}".format(field_type[0]))
        if field_type[0] in string_types:
            return 'string'
        else:
            return 'num'

    def where_clauses(self, fields, data_dict):
        log.warning("FIELDS: {}".format(fields))
        filters = data_dict.get('filters', {})
        q = data_dict.get('q')
        if filters or q:
            where_str = ' WHERE '
        else:
            where_str = ''
        if filters:
            where_filters = ''
            for key, value in filters.iteritems():
                single_where_statament = '' 
                log.warning("filter: {0} = {1}".format(key, value))
                for value_item in value:
                    if self.get_field_type(fields, key) == 'num':
                        single_where_statament += '{0} = {1} OR '.format(key, value_item)
                    else:
                        single_where_statament += '{0} = "{1}" OR '.format(key, value_item)
                where_filters += '({0}) AND '.format(single_where_statament[:-3]) 
            log.warning("where_filters: {}".format(where_filters[:-4]))
            where_str += where_filters[:-4]
        # add full-text search where clause
        if q:
            where_q = ''
            for key, value in q.iteritems():
                if self.get_field_type(fields, key) == 'string':
                    where_q_str = ' LOWER({0}) like LOWER("{1}%") '.format(key, value[:-2])
                else:
                    where_q_str = ' CAST({0} as STRING)  like "{1}%" '.format(key, value[:-2])
                where_q += where_q_str
            where_str += where_q
        log.warning("where_str: {}".format(where_str))
        return where_str

    def search_sql_normal(self, sql):
        '''
        NB: table must be full table id ...
        '''
        log.warning("***** search_sql *****")
        log.warning("sql = {}".format(sql))
        # limit the number of results to ckan.datastore.search.rows_max + 1
        # (the +1 is so that we know if the results went over the limit or not)
        rows_max = int(config.get('ckan.datastore.search.rows_max', 32000))
        self.log_data['api_call_type'] = "dataexplorer-filtered-download"

        sql_initial = sql
        # limit the number of results to return by rows_max
        sql = 'SELECT * FROM ({0}) AS blah LIMIT {1} ;'.format(sql, rows_max+1)

        log.warning("query - {}".format(sql))
        self.log_data['query'] = sql
        query_job = self.bqclient_readonly.query(sql, job_config=self.job_config)
        rows = query_job.result() 
        self.log_data['job_details'] = query_job._properties.get('statistics')
        records = [dict(row) for row in rows]
        # check if results truncated ...
        if len(records) == rows_max + 1:
           return self.bulk_export(sql_initial)
        else:
            self.create_egress_log()
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
        log.warning("Data_dict {}".format(data_dict))
        if data_dict.get('resource_id'):
            self.resource_details['big_query_resource_name'] = data_dict.get('resource_id')
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
            self.log_data['query'] = sql_initial
            sql_query_job = self.bqclient.query(sql_initial, job_config=self.job_config)
            # get temp table containing query result
            sql_query_job.result() 
            destination_table = sql_query_job.destination
            log.warning("destination table: {}".format(destination_table))
            destination_urls = self.extract_query_to_gcs(destination_table, sql_initial)
            log.warning("extract job result: {}".format(destination_urls))
            
            self.log_data['job_details'] = sql_query_job._properties.get('statistics')
            self.log_data['api_call_type'] = "dataexplorer-bulk-download"
            self.create_egress_log()
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

    def convertDate(self, milliseconds):
        try:
            date = datetime.datetime.fromtimestamp(milliseconds/1000.0)
            return date.strftime('%Y-%m-%d %H:%M:%S.%f')
        except:
            return milliseconds

    def create_egress_log(self, schema_data=None):
        log_data = self.log_data
        # Get Environ for the IP, user-agent and Remote User
        environ = request.environ
        user = environ.get('REMOTE_USER', None)
        if 'HTTP_X_REAL_IP' in environ:
            ip = environ.get('HTTP_X_REAL_IP', None)
        else:
            ip = environ.get('REMOTE_ADDR', None)
        agent = environ.get('HTTP_USER_AGENT',None)
        if not schema_data:
            start_time = self.convertDate(log_data.get('job_details').get('startTime'))
            end_time = self.convertDate(log_data.get('job_details').get('endTime'))
            total_bytes_processed = log_data.get('job_details').get('query').get('totalBytesProcessed')
            total_bytes_billed = log_data.get('job_details').get('query').get('totalBytesBilled')
            cache_hit = log_data.get('job_details').get('query').get('cacheHit')
            data = {
                "timestamp": str(datetime.datetime.now()),
                "schema_and_count_call": False,
                "log_egress":{
                    "ip": ip,
                    "http_user_agent": agent,
                    "remote_user": user,
                    "start_time": start_time,
                    "end_time": end_time,
                    "query_executed": log_data.get('query'),
                    "total_bytes_processed":total_bytes_processed,
                    "total_bytes_billed":total_bytes_billed,
                    "resource_name": self.resource_details.get('big_query_resource_name'),
                    "big_query_resource_name":self.resource_details.get('big_query_resource_name'),
                    "cache_hit": cache_hit,
                    "api_call_type": log_data.get('api_call_type', 'direct-api')
                }

            } 
        else:
            data = {
                "timestamp": str(datetime.datetime.now()),
                "schema_and_count_call": True,
                "log_egress":{
                    "ip": ip,
                    "http_user_agent": agent,
                    "remote_user": user,
                    "start_time": schema_data.get('start'),
                    "end_time": schema_data.get('start'),
                    "query_executed": None,
                    "total_bytes_processed": None,
                    "total_bytes_billed": None,
                    "resource_name": self.resource_details.get('big_query_resource_name'),
                    "big_query_resource_name":self.resource_details.get('big_query_resource_name'),
                    "cache_hit": None,
                    "total_count": schema_data.get('total_count'),
                    "table_size": schema_data.get('table_size'),
                    "api_call_type": log_data.get('api_call_type', 'direct-api')
                }

            } 
        log_name = config.get('egress_log_name', 'nhs-egress-log')
        log.warning("{} {}".format(log_name, data))


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
