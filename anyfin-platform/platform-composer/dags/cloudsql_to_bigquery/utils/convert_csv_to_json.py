import io
import os
import csv
import sys
import json
import time
import pandas as pd
from google.cloud import storage
import argparse
import logging

DATABASE_NAME = ''
TABLES = {}
BUCKET_NAME = ''


def upload_json(df, count, table_name, bucket, start):
    filename = f'export-{count}.json'
    df.to_json(filename, orient="records", lines=True)
    with open(filename, 'r') as jsonf:
        blob = bucket.blob(f'json_extracts/{DATABASE_NAME}/{table_name}/{filename}')
        blob.upload_from_filename(filename)
    logging.info(f'{time.time() - start} seconds elapsed after {count + 1} export(s)')
    os.remove(filename)


def run(table_name, chunk_size):
    columns = [list(s.get('schema').keys()) for t, s in TABLES.items() if t == table_name][0]
    columns = [c.replace('\"', '') for c in columns]
    columns.append('_ingested_ts')

    for t, data in TABLES.items():
        if t == table_name:
            schema = data.get('schema')
    dtypes = {}
    array_fields = []
    for key in schema:
        if schema[key] == 'numeric':
            dtypes[key] = 'float64'
        else:
            dtypes[key] = 'object'
        if schema[key] == 'ARRAY':
            array_fields.append(key)
    start = time.time()

    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NAME)

    files_uploaded = 0
    def convert_string_to_array(x):
        try:
            return x.replace('{','').replace('}','').split(',')
        except AttributeError as error:
            return None

    csv_filepath = f'gs://{BUCKET_NAME}/pg_dumps/{DATABASE_NAME}_{table_name}_export.csv'
    for chunk in pd.read_csv(csv_filepath, chunksize=chunk_size, names=columns, dtype=dtypes):
        for field in array_fields:
            chunk[field] = chunk[field].apply( convert_string_to_array )
        upload_json(chunk, files_uploaded, table_name, bucket, start)
        files_uploaded += 1
    end = time.time()

    logging.info(f"{end - start} seconds elapsed")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--table_name",
        default=None
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        default=5000
    )
    parser.add_argument(
        "--database_name",
        type=str,
        default=None
    )
    parser.add_argument(
        "--bucket_name",
        type=str,
        default=None
    )
    args = parser.parse_args()

    # Initialize global variables
    DATABASE_NAME = args.database_name
    BUCKET_NAME = args.bucket_name
    with open(f'/home/airflow/{DATABASE_NAME}_schemas_state.json', 'r') as f:
        TABLES = json.loads(f.read())

    run(args.table_name, args.chunk_size)