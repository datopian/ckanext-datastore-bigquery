import sys
sys.path.insert(0, './src')
import os

import ckan_to_bigquery


table_name = '201401'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.bigquery_test_credentials.json'
os.environ['BIGQUERY_PROJECT_ID'] = 'bigquerytest-271707'

def fixture_create():
    create_table(..., )
    insert_data(blah)


def test_search():
    # assume there is a bigquery tables created
    results = ckan_to_bigquery.search(table_name)
    assert len(results) == 10
    first = results[0]
    expected = {
            'ACTUAL_COST': 6.59007,
            'ADDRESS_1': '-', 'ADDRESS_2': '-', 'ADDRESS_3': '-', 'ADDRESS_4': '-',
            'ADQUSAGE': 0.0, 'AREA_TEAM_CODE': '-', 'AREA_TEAM_NAME': 'UNIDENTIFIED',
            'BNF_CHAPTER_PLUS_CODE': '20: Dressings',
            'BNF_CHEMICAL_SUBSTANCE': '2003',
            'BNF_CODE': '20030100373',
            'BNF_DESCRIPTION': 'Softpore dressing 10cm x 20cm',
            'CHEMICAL_SUBSTANCE_BNF_DESCR': 'Wound Management & Other Dressings',
            'ITEMS': 2, 'NIC': 7.0, 'PCO_CODE': '-', 'PCO_NAME': 'UNIDENTIFIED',
            'POSTCODE': '-',
            'PRACTICE_CODE': '-', 'PRACTICE_NAME': 'UNIDENTIFIED DOCTORS',
            'QUANTITY': 10.0,
            'REGIONAL_OFFICE_CODE': '-', 'REGIONAL_OFFICE_NAME':
            'UNIDENTIFIED', 'TOTAL_QUANTITY': 20.0, 'UNIDENTIFIED': True,
            'YEAR_MONTH': 201401
        }
    assert first == expected

