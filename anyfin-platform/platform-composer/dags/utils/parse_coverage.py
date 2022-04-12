import json
import pandas as pd
from datetime import datetime
from os import path

datasets_to_include = ['core', 'dim', 'metrics', 'marketing', 'finance']

load_date = datetime.today().strftime('%Y-%m-%d')

dbt_home_dir = '/home/airflow/gcs/dags/anyfin-data-model/'
data_dir = '/home/airflow/gcs/data/'

if path.exists(f'{dbt_home_dir}coverage-test.json'):
    with open(f'{dbt_home_dir}coverage-test.json', 'r') as f:
        json_data_tests = json.load(f)

    if path.exists(f'{dbt_home_dir}coverage-doc.json'):
        with open(f'{dbt_home_dir}coverage-doc.json', 'r') as f:
            json_data_docs = json.load(f)

        test_data = []
        docs_data = []

        for node in json_data_tests['tables']:
            dataset_name, table_name = node['name'].split('.')
            if dataset_name not in datasets_to_include:
                continue
            test_data.append([load_date, dataset_name, table_name, node['covered']])
            # in case if we want to store column-based data instead of table-based
            # for column in node['columns']:
            #     test_data.append([load_datetime, dataset_name, table_name, column['name'], column['covered']])

        for node in json_data_docs['tables']:
            dataset_name, table_name = node['name'].split('.')
            if dataset_name not in datasets_to_include:
                continue
            docs_data.append([dataset_name, table_name, node['covered'], node['total']])

        data_to_insert = pd.DataFrame(test_data, columns=['execution_date', 'dataset_name', 'table_name', 'tests_covered'])
        data_to_insert_docs = pd.DataFrame(docs_data, columns=['dataset_name', 'table_name', 'docs_covered', 'columns_total'])

        data_to_insert = pd.merge(data_to_insert, data_to_insert_docs, on=['dataset_name', 'table_name'],
                                  how='left')
        data_to_insert.columns = ['execution_date', 'dataset_name', 'table_name', 'tests_covered', 'docs_covered',
                                  'columns_total']

        total_row = [datetime.today().strftime('%Y-%m-%d'), 'all', 'all', data_to_insert['tests_covered'].sum(),
                     data_to_insert['docs_covered'].sum(), data_to_insert['columns_total'].sum()]
        idx = data_to_insert.index.max() + 1
        data_to_insert.loc[idx] = total_row

        data_to_insert.to_csv(f'{data_dir}dbt-coverage-report.csv', index=False)
