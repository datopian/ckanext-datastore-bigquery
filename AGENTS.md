# Repository Guide

## Architecture

- `setup.py` registers the CKAN plugin `bigquery`; `ckanext/bigquery/plugin.py` only registers the `bigquery://` datastore backend.
- `ckanext/bigquery/backend/bigquery.py` adapts CKAN's `IDatastoreBackend` API. Query construction, BigQuery/GCS access, bulk-export history, and egress logging live in `src/ckan_to_bigquery.py`; API tracking and SQL table-name extraction live in `src/api_tracker.py`.
- Keep both package trees when changing imports or packaging: `ckanext` is the CKAN integration and root `src` is an installed Python package used by it.

## Environment

- Install the extension and runtime dependencies separately: `pip install -e .` then `pip install -r requirements.txt`. `dev-requirements.txt` is empty.
- Do not infer supported Python from `setup.py` or `.travis.yml`: their Python 2.7 metadata is stale, while current code requires Python 3 (including `dict[str, Any]` syntax).
- Backend construction requires both `ckanext.bigquery.google_cloud_credentials` and `ckanext.bigquery.google_cloud_credentials_read_only`, plus `ckanext.bigquery.project` and `ckanext.bigquery.dataset`. SQL queries time out after `ckanext.bigquery.query_timeout` seconds (default `60`). Bulk exports additionally use `ckanext.bigquery.bucket` and `ckanext.bigquery.location`.
- Treat `test.ini` as a template only: it contains a developer-specific credential path and assumes a CKAN source checkout at `../../src/ckan/test-core.ini`.

## Tests

- Standalone tests are live BigQuery integration tests, not unit tests: `pytest test`. They hard-code project `bigquerytest-271707`, dataset `nhs_production`, table `EPD_201401`, and load `test/google.json` plus `test/google_readonly.json`.
- Run one standalone test with `pytest test/test_ckan_to_bigquery.py::TestSearch::test_search`. Never create or commit those credential files; unlike the obsolete root `.bigquery_test_credentials.json` name, they are not ignored.
- CKAN plugin tests require a configured CKAN environment and services: `pytest -q ckanext/bigquery/tests/test_plugin.py`. Focus a test with `pytest -q ckanext/bigquery/tests/test_plugin.py::TestBigQueryIDatastoreBackendPlugin::test_bigquery_engine`.
- `TestBigQueryIDatastoreBackendPlugin.test_backend_functionality` also queries a real BigQuery table (`ckanext_testing`); do not expect the CKAN suite to be hermetic.
- `.travis.yml`, `bin/travis-*.sh`, and the README's `nosetests` command are legacy Python 2/Paster paths, not reliable current verification. There is no configured lint, format, or typecheck command.

## Behavioral Constraints

- Normal searches use the read-only BigQuery client. Query-history writes, temporary-table exports, and GCS extraction use the writable client; preserve this split.
- `datastore_search_sql` requires `resource_id` and SQL containing a backtick-quoted table identifier. Results above `ckan.datastore.search.rows_max` (default `32000`) switch to bulk export and return `gc_urls` rather than records.
