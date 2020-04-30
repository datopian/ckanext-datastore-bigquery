# ckanext-datastore-bigquery

An extension that enables `datastore_search` and `datastore_search_sql` API queries running against the data stored in Google Cloud BigQuery tables instead of standard CKAN datastore search queries ([ckan datastore api](https://docs.ckan.org/en/2.8/maintaining/datastore.html#the-datastore-api)).


## Getting Started

You can run the extension like standalone instance on your local machine for development and tests purposes.
Also, you can use it as a ckan plugin instead of datastore standard plugin.

### Prerequisites

Install the requirements:

```
pip install requirements.txt
```

You need to get your cloud credentials for the relevant
service from here: https://cloud.google.com/docs/authentication/getting-started
Then save them to the json file.


### Installation

You need CKAN instance running (tested with ckan v2.8.2).

Install extension:
```
pip install ckanext-datastore-bigquery && \
pip install google-cloud-bigquery
```

Enable the extension in CKAN by adding to your .ini file:

```
ckan.plugins = bigquery
ckan.datastore.write_url = bigquery://id_of_your_bq_project
ckan.datastore.read_url = bigquery://id_of_your_bq_project
ckan.datastore.sqlsearch.enabled = True
ckanext.bigquery.google_cloud_credentials = path_to_your_cloud_credentials_json_file
ckanext.bigquery.project = id_of_your_bigquery_project
ckanext.bigquery.dataset id_of_your_bigquery_dataset

```

## Running tests

Put the credentials file(taht you created in Prerequisites section) as:
`.bigquery_test_credentials.json`
at the root of your project.

You can run the tests for standalone instance:
`pytest test`

Or you can run tests in an integrated ckan plugin:
`nosetests -v --ckan --with-pylons=test.ini ckanext`

