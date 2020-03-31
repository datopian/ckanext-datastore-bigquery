import os

from google.cloud import bigquery

class Client(object):
    def __init__(self, project_id, dataset):
        self.project_id = project_id
        self.dataset = dataset

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
                "start": "/api/3/action/datastore_search?resource_id=553a4d08-7670-48c2-a994-9e180131b22e",
                "next": "/api/3/action/datastore_search?offset=100&resource_id=553a4d08-7670-48c2-a994-9e180131b22e"
                },
                "total": len(results)
            }
        }
        return out

    def search_raw(self, data_dict=None, **kwargs):
        if data_dict:
            print(data_dict)
            _kwargs = dict(data_dict)
        else:
            _kwargs = kwargs
        _kwargs['table_id'] = self._table_id(_kwargs['resource_id'])
        # default for limit is 100
        _kwargs['limit'] = min(10000, _kwargs.get('limit', 100))

        query = 'SELECT * FROM `{table_id}` '.format(**_kwargs)
        if 'field' in _kwargs:
            query += ' WHERE {field} '.format(**_kwargs)
        if 'sort' in _kwargs:
            query += ' ORDER BY {sort} '.format(**_kwargs)
        query +=  ' LIMIT {limit}'.format(**_kwargs)
        print(query)

        client = bigquery.Client()
        query_job = client.query(query)
        rows = query_job.result()
        records = [dict(row) for row in rows]
        return records

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
