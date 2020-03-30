import sys
sys.path.insert(0, './src')
import os

import ckan_to_bigquery as ckan2bq
from google.cloud.bigquery.schema import SchemaField


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '.bigquery_test_credentials.json'

project_id = 'bigquerytest-271707'
client = ckan2bq.Client(project_id)

table_name = '201401'
limit = 5
sort = 'REGIONAL_OFFICE_NAME'

def fixture_create():
    create_table(..., )
    insert_data(blah)


expected1 = {
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

expected2 = [
    {'YEAR_MONTH': 201401,
     'REGIONAL_OFFICE_NAME': 'EASTERN', 'REGIONAL_OFFICE_CODE': 'Y09',
     'AREA_TEAM_NAME': 'EAST OF ENGLAND', 'AREA_TEAM_CODE': 'Q35',
     'PCO_NAME': 'PETERBOROUGH', 'PCO_CODE': '5PN00',
     'PRACTICE_NAME': 'ALTERNATIVE MEDICAL SCHEME', 'PRACTICE_CODE': 'D81650',
     'ADDRESS_1': '', 'ADDRESS_2': '', 'ADDRESS_3': '', 'ADDRESS_4': '',
     'POSTCODE': '', 'BNF_CHEMICAL_SUBSTANCE': '0403040Y0',
     'CHEMICAL_SUBSTANCE_BNF_DESCR': 'Duloxetine hydrochloride', 'BNF_CODE': '0403040Y0AAABAB',
     'BNF_DESCRIPTION': 'Duloxetine 60mg gastro-resistant capsules',
     'BNF_CHAPTER_PLUS_CODE': '04: Central Nervous System', 'QUANTITY': 28.0, 'ITEMS': 1,
     'TOTAL_QUANTITY': 28.0, 'ADQUSAGE': 28.0, 'NIC': 27.72, 'ACTUAL_COST': 26.10909, 'UNIDENTIFIED': False
    },
    {'YEAR_MONTH': 201401,
     'REGIONAL_OFFICE_NAME': 'EASTERN', 'REGIONAL_OFFICE_CODE': 'Y09',
     'AREA_TEAM_NAME': 'EAST OF ENGLAND', 'AREA_TEAM_CODE': 'Q35',
     'PCO_NAME': 'PETERBOROUGH', 'PCO_CODE': '5PN00',
     'PRACTICE_NAME': 'ALTERNATIVE MEDICAL SCHEME', 'PRACTICE_CODE': 'D81650',
     'ADDRESS_1': '', 'ADDRESS_2': '', 'ADDRESS_3': '', 'ADDRESS_4': '',
     'POSTCODE': '', 'BNF_CHEMICAL_SUBSTANCE': '0401020K0',
     'CHEMICAL_SUBSTANCE_BNF_DESCR': 'Diazepam', 'BNF_CODE': '0401020K0AAAIAI',
     'BNF_DESCRIPTION': 'Diazepam 5mg tablets',
     'BNF_CHAPTER_PLUS_CODE': '04: Central Nervous System', 'QUANTITY': 84.0, 'ITEMS': 2,
     'TOTAL_QUANTITY': 168.0, 'ADQUSAGE': 84.0, 'NIC': 5.34, 'ACTUAL_COST': 5.05208, 'UNIDENTIFIED': False
     }
    ]

expected3 = {
            'ACTUAL_COST': 2.77082,
            'ADDRESS_1': '', 'ADDRESS_2': '', 'ADDRESS_3': '', 'ADDRESS_4': '',
            'ADQUSAGE': 11.2, 'AREA_TEAM_CODE': 'Q35', 'AREA_TEAM_NAME': 'EAST OF ENGLAND',
            'BNF_CHAPTER_PLUS_CODE': '04: Central Nervous System',
            'BNF_CHEMICAL_SUBSTANCE': '0401020P0',
            'BNF_CODE': '0401020P0AAABAB', 'BNF_DESCRIPTION': 'Lorazepam 1mg tablets',
            'CHEMICAL_SUBSTANCE_BNF_DESCR': 'Lorazepam', 'ITEMS': 1, 'NIC': 2.93,
            'PCO_CODE': '5PN00', 'PCO_NAME': 'PETERBOROUGH', 'POSTCODE': '',
            'PRACTICE_CODE': 'D81650', 'PRACTICE_NAME': 'ALTERNATIVE MEDICAL SCHEME',
            'QUANTITY': 28.0, 'REGIONAL_OFFICE_CODE': 'Y09', 'REGIONAL_OFFICE_NAME': 'EASTERN',
            'TOTAL_QUANTITY': 28.0, 'UNIDENTIFIED': False, 'YEAR_MONTH': 201401
        }

expected4 = [
    {'ACTUAL_COST': 3.27919, 'ADDRESS_1': 'ULVERSTON COMM.HEALTH CTR', 'ADDRESS_2': 'STANLEY STREET',
     'ADDRESS_3': 'ULVERSTON', 'ADDRESS_4': 'CUMBRIA', 'ADQUSAGE': 0.0,
     'AREA_TEAM_CODE': 'Q49', 'AREA_TEAM_NAME': 'CUMBRIA,NORTHUMB,TYNE & WEAR AREA',
     'BNF_CHAPTER_PLUS_CODE': '13: Skin', 'BNF_CHEMICAL_SUBSTANCE': '1302010U0',
     'BNF_CODE': '1302010U0BJAAAN', 'BNF_DESCRIPTION': 'E45 Itch Relief cream',
     'CHEMICAL_SUBSTANCE_BNF_DESCR': 'Urea', 'ITEMS': 1, 'NIC': 3.47, 'PCO_CODE': '01H00',
     'PCO_NAME': 'NORTH CUMBRIA CCG', 'POSTCODE': 'LA12 7BT', 'PRACTICE_CODE': 'A82068',
     'PRACTICE_NAME': 'ULVERSTON COMMUNITY HEALTH CENTRE', 'QUANTITY': 100.0, 'REGIONAL_OFFICE_CODE': 'Y54',
     'REGIONAL_OFFICE_NAME': 'NORTH OF ENGLAND', 'TOTAL_QUANTITY': 100.0, 'UNIDENTIFIED': False,
     'YEAR_MONTH': 201401
     },
    {'ACTUAL_COST': 52.19338, 'ADDRESS_1': 'COURT THORN SURGERY', 'ADDRESS_2': 'LOW HESKET',
     'ADDRESS_3': 'CARLISLE', 'ADDRESS_4': 'CUMBRIA', 'ADQUSAGE': 56.0,
     'AREA_TEAM_CODE': 'Q49', 'AREA_TEAM_NAME': 'CUMBRIA,NORTHUMB,TYNE & WEAR AREA',
     'BNF_CHAPTER_PLUS_CODE': '04: Central Nervous System', 'BNF_CHEMICAL_SUBSTANCE': '0403040Y0',
     'BNF_CODE': '0403040Y0AAABAB', 'BNF_DESCRIPTION': 'Duloxetine 60mg gastro-resistant capsules',
     'CHEMICAL_SUBSTANCE_BNF_DESCR': 'Duloxetine hydrochloride', 'ITEMS': 1, 'NIC': 55.44,
     'PCO_CODE': '01H00', 'PCO_NAME': 'NORTH CUMBRIA CCG', 'POSTCODE': 'CA4 0HP',
     'PRACTICE_CODE': 'A82631', 'PRACTICE_NAME': 'COURT THORN SURGERY', 'QUANTITY': 56.0,
     'REGIONAL_OFFICE_CODE': 'Y54', 'REGIONAL_OFFICE_NAME': 'NORTH OF ENGLAND', 'TOTAL_QUANTITY': 56.0,
     'UNIDENTIFIED': False, 'YEAR_MONTH': 201401
     }
    ]

expected5 = {'REGIONAL_OFFICE_NAME': 'UNIDENTIFIED'}

expected6 = {
            'PRACTICE_CODE': '-',
            'REGIONAL_OFFICE_NAME': 'UNIDENTIFIED'
            }

schema = [
            SchemaField('YEAR_MONTH', 'INTEGER', 'NULLABLE', None, ()),
            SchemaField('REGIONAL_OFFICE_NAME', 'STRING', 'NULLABLE', None, ()),
            SchemaField('REGIONAL_OFFICE_CODE', 'STRING', 'NULLABLE', None, ()),
            SchemaField('AREA_TEAM_NAME', 'STRING', 'NULLABLE', None, ()),
            SchemaField('AREA_TEAM_CODE', 'STRING', 'NULLABLE', None, ()),
            SchemaField('PCO_NAME', 'STRING', 'NULLABLE', None, ()),
            SchemaField('PCO_CODE', 'STRING', 'NULLABLE', None, ()),
            SchemaField('PRACTICE_NAME', 'STRING', 'NULLABLE', None, ()),
            SchemaField('PRACTICE_CODE', 'STRING', 'NULLABLE', None, ()),
            SchemaField('ADDRESS_1', 'STRING', 'NULLABLE', None, ()),
            SchemaField('ADDRESS_2', 'STRING', 'NULLABLE', None, ()),
            SchemaField('ADDRESS_3', 'STRING', 'NULLABLE', None, ()),
            SchemaField('ADDRESS_4', 'STRING', 'NULLABLE', None, ()),
            SchemaField('POSTCODE', 'STRING', 'NULLABLE', None, ()),
            SchemaField('BNF_CHEMICAL_SUBSTANCE', 'STRING', 'NULLABLE', None, ()),
            SchemaField('CHEMICAL_SUBSTANCE_BNF_DESCR', 'STRING', 'NULLABLE', None, ()),
            SchemaField('BNF_CODE', 'STRING', 'NULLABLE', None, ()),
            SchemaField('BNF_DESCRIPTION', 'STRING', 'NULLABLE', None, ()),
            SchemaField('BNF_CHAPTER_PLUS_CODE', 'STRING', 'NULLABLE', None, ()),
            SchemaField('QUANTITY', 'FLOAT', 'NULLABLE', None, ()),
            SchemaField('ITEMS', 'INTEGER', 'NULLABLE', None, ()),
            SchemaField('TOTAL_QUANTITY', 'FLOAT', 'NULLABLE', None, ()),
            SchemaField('ADQUSAGE', 'FLOAT', 'NULLABLE', None, ()),
            SchemaField('NIC', 'FLOAT', 'NULLABLE', None, ()),
            SchemaField('ACTUAL_COST', 'FLOAT', 'NULLABLE', None, ()),
            SchemaField('UNIDENTIFIED', 'BOOLEAN', 'NULLABLE', None, ()),
        ]

def test_search_raw():
    results = client.search_raw(table_name)
    assert len(results) == 10
    first = results[0]
    assert first == expected1

def test_search():
    out = client.search(table_name)
    assert out['result']['total'] == 10
    first = out['result']['records'][0]
    assert first == expected1

def test_search_with_limit():
    results = client.search_limit(table_name, limit)
    assert len(results) == 5

def test_search_sort():
    results = client.search_sort(table_name, sort)
    assert results == expected2

def test_resource():
    # resource name is in fact the table name
    resource_name = table_name
    results = client.search_raw(resource_name)
    assert len(results) == 10
    first = results[0]
    assert first == expected1

def test_filter():
    # the fields go in the WHERE clause
    field = "REGIONAL_OFFICE_NAME = 'EASTERN'"
    results = client.search_filter(table_name, field)
    assert len(results) == 10
    first = results[0]
    assert first == expected3

def test_filters():
    # the fields go in the WHERE clause
    field1 = "REGIONAL_OFFICE_NAME = 'EASTERN'"
    field2 = "PRACTICE_CODE = 'D81650'"
    results = client.search_filters(table_name, field1, field2)
    assert len(results) == 10
    first = results[0]
    assert first == expected3

# def test_free_text_search():
#     q = "'EASTERN'"
#     results = client.search_free_text(table_name, q)
#     assert len(results) == 0

def test_distinct():
    distinct = 'DISTINCT'
    results = client.search_distinct(table_name, distinct)
    assert len(results) == 2
    assert results == expected4

def test_field():
    field = "REGIONAL_OFFICE_NAME"
    results = client.search_field(table_name, field)
    assert len(results) == 10
    first = results[0]
    assert first == expected5

def test_fields():
    field1 = "REGIONAL_OFFICE_NAME"
    field2 = "PRACTICE_CODE"
    results = client.search_fields(table_name, field1, field2)
    assert len(results) == 10
    first = results[0]
    assert first == expected6

def test_total():
    results = client.search_raw(table_name)
    assert len(results) == 10

def test_bq_table_schema():
    result = client.table_schema(table_name)
    assert len(result) == 26
    first = result
    assert first == schema