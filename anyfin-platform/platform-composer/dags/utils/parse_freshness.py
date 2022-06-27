import json
from os import path
import pandas as pd
from datetime import datetime
import sys

data_dir = '/home/airflow/gcs/data/'

if len(sys.argv) == 2:
    load_date = datetime.strptime(sys.argv[1], '%Y-%m-%d')
    print('load date is', load_date)
    if path.exists(f'{data_dir}/sources.json'):
        with open(f'{data_dir}/sources.json', 'r') as f:
            data = json.load(f)

        freshness_fata = []

        for node in data['results']:
            dataset_name, model_name = node['unique_id'].split('.')[2:]
            status = node['status']
            max_timestamp_in_table = node['max_loaded_at']
            max_loaded_at_time_ago_in_s = node['max_loaded_at_time_ago_in_s']
            filter_condition = node['criteria']['filter']
            freshness_fata.append([load_date, dataset_name, model_name, status, max_timestamp_in_table, max_loaded_at_time_ago_in_s, filter_condition])

        data_to_insert = pd.DataFrame(freshness_fata, columns=['execution_date', 'dataset_name', 'model_name', 'status', 'max_timestamp_in_table',
                                                               'max_timestamp_time_ago_in_s', 'filter_condition'])

        print(f'Writing to {data_dir}source-freshness.csv...')
        data_to_insert.to_csv(f'{data_dir}source-freshness.csv', index=False)
else:
    raise ValueError(f"You didn't provide execution date. Args you provided {sys.argv}")