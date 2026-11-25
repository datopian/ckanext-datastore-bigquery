"""Credential-free tests for matching datastore_search ``fields``.

Regression for duplicate Data Explorer filter values: ``fields=TOTAL_QUANTITY``
also selected ``QUANTITY``, so ``SELECT DISTINCT`` returned one row per pair.
"""
import ast
from pathlib import Path
import unittest


def load_requested_fields():
    path = Path(__file__).resolve().parents[1] / 'src' / 'ckan_to_bigquery.py'
    tree = ast.parse(path.read_text())
    func = next(node for node in tree.body
                if isinstance(node, ast.FunctionDef)
                and node.name == 'requested_fields')
    namespace = {}
    exec(compile(ast.Module(body=[func], type_ignores=[]), str(path), 'exec'),
         namespace)
    return namespace['requested_fields']


class TestRequestedFields(unittest.TestCase):
    requested_fields = staticmethod(load_requested_fields())

    def test_string_does_not_match_substrings(self):
        fields = self.requested_fields('TOTAL_QUANTITY')
        self.assertIn('TOTAL_QUANTITY', fields)
        self.assertNotIn('QUANTITY', fields)

    def test_comma_separated_string(self):
        self.assertEqual(self.requested_fields('PERIOD, BNF_NAME'),
                         {'PERIOD', 'BNF_NAME'})

    def test_list(self):
        self.assertEqual(self.requested_fields(['PERIOD', 'BNF_NAME']),
                         {'PERIOD', 'BNF_NAME'})

    def test_empty(self):
        self.assertEqual(self.requested_fields(None), set())


if __name__ == '__main__':
    unittest.main()
