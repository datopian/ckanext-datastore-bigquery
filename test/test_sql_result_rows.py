"""Credential-free regression tests for SQL result serialization.

Compile the actual method from source so these tests can run without the
CKAN environment and cloud SDKs required by the module's top-level imports.
"""
import ast
import datetime
import logging
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock


def load_search_sql_normal():
    path = Path(__file__).resolve().parents[1] / 'src' / 'ckan_to_bigquery.py'
    tree = ast.parse(path.read_text())
    client = next(node for node in tree.body
                  if isinstance(node, ast.ClassDef) and node.name == 'Client')
    method = next(node for node in client.body
                  if isinstance(node, ast.FunctionDef)
                  and node.name == 'search_sql_normal')
    module = ast.Module(body=[method], type_ignores=[])
    namespace = {
        'config': {},
        'log': logging.getLogger(__name__),
        'datetime': datetime,
        'sys': sys,
    }
    exec(compile(module, str(path), 'exec'), namespace)
    return namespace['search_sql_normal']


class QueryRows(list):
    @property
    def total_rows(self):
        return len(self)


class TestSQLResultRows(unittest.TestCase):
    def search(self, rows):
        client = Mock()
        client.log_data = {}
        client.bqclient_readonly.query.return_value.result.return_value = QueryRows(rows)
        result = load_search_sql_normal()(client, 'SELECT * FROM `table` LIMIT 1')
        client.create_egress_log.assert_called_once_with()
        return result['result']

    def test_one_row_with_23_columns_is_returned_once(self):
        row = {'column_{}'.format(i): i for i in range(23)}
        self.assertEqual(self.search([row])['records'], [row])

    def test_multiple_rows_are_returned_once_each(self):
        rows = [{'id': 1, 'name': 'first'}, {'id': 2, 'name': 'second'}]
        self.assertEqual(self.search(rows)['records'], rows)

    def test_value_conversions_are_preserved(self):
        row = {'large': 12345678911, 'date': datetime.date(2026, 9, 1), 'small': 3293}
        self.assertEqual(self.search([row])['records'], [{
            'large': '12345678911', 'date': '2026-09-01', 'small': 3293,
        }])
        self.assertIsInstance(row['date'], datetime.date)

    def test_empty_result(self):
        result = self.search([])
        self.assertEqual(result['records'], [])
        self.assertEqual(result['message'], 'Query executed successfully, returned 0 rows')


if __name__ == '__main__':
    unittest.main()
