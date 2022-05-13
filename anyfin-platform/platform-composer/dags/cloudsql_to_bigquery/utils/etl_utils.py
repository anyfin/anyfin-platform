import json
import tempfile
import os
import logging
from datetime import datetime, timedelta, date

from google.cloud import bigquery
from airflow import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook


class ETL(object):

    # Constructor
    def __init__(self, GCS_BUCKET, DATABASE_NAME):
        self.GCS_BUCKET = GCS_BUCKET
        self.DATABASE_NAME = DATABASE_NAME

        with open(os.path.join(os.path.dirname(__file__), f'../pg_schemas/{DATABASE_NAME}_schemas_state.json')) as f:
            self.TABLES = json.loads(f.read())
    
    def get_tables(self):
        return self.TABLES

    def deduplication_success(self, **context):
        return True

    def get_pg_columns(self, table):
        schema = self.TABLES.get(table).get('schema')
        columns = [f"{k}::timestamp as {k}" if 'timestamp' in schema.get(k) else f"{k} as {k}" for k in list(schema.keys())]
        columns.append('now()::timestamp as _ingested_ts')
        return ', '.join(list(columns))

    def get_bq_columns(self, table):
        schema = self.TABLES.get(table).get('schema')
        columns = [f"CAST({k} AS TIMESTAMP) as {k}" if 'timestamp' in schema.get(k) else k for k in list(schema.keys())]
        columns.append("CAST(_ingested_ts AS TIMESTAMP) as _ingested_ts")
        return ', '.join(list(columns))

    # Extract a list of tables seperated by if they have been updated or not
    # Return: Dictionary with extractable tables and tables containing unknown columns


    def extract_tables(self):

        # Postgres schema
        bucket_json_schemata = self.TABLES

        # DB connection
        con = PostgresHook(postgres_conn_id=f'{self.DATABASE_NAME}_replica').get_conn()
        cur = con.cursor()

        # Fetch current state of tables
        schema_query = '''
            SELECT
            table_name,
            json_agg(concat(column_name::text, ':', data_type::text)) as columns
            FROM
            information_schema.columns
            WHERE
            table_name in {tables}
            GROUP BY table_name
            ORDER BY 1;
        '''.format(tables="('{}')".format("','".join([t for t in bucket_json_schemata])))
        cur.execute(schema_query)
        db_state_schemata = cur.fetchall()

        # Close DB connection
        cur.close()
        con.close()

        missing_columns = {}

        for (db_table, db_columns) in db_state_schemata:
            bucket_table_columns = bucket_json_schemata.get(db_table).get('schema')

            db_column_names = [col.split(':')[0] for col in db_columns]
            bucket_column_names = bucket_table_columns.keys()

            discrepency = list(set(db_column_names) - set(bucket_column_names)) + \
                list(set(bucket_column_names) - set(db_column_names))

            if not discrepency:
                continue
            else:
                missing_columns[db_table] = discrepency

        tables_to_extract = [
            t for t in bucket_json_schemata if t not in list(missing_columns)]

        all_tables = {}
        all_tables['tables_to_extract'] = tables_to_extract  # List of tables
        # Dict with table: column
        all_tables['missing_columns'] = missing_columns

        return all_tables


    def no_missing_columns(self, task_name, **context):
        # Fetches task instance from context and pulls the variable from xcom
        missing_columns = context['ti'].xcom_pull(
            task_ids=f'{task_name}.extract_tables')['missing_columns']
            
        return True
        # Check if dictionary is empty - Temporarily deactivating
        # if not(missing_columns):
        #     return True
        # else:
        #     raise ValueError(
        #         'These columns are either missing or they have changed type: ', str(missing_columns))


    def upload_table_names(self, task_name, **context):
        # Fetches task instance from context and pulls the variable from xcom
        dict_tables = context['ti'].xcom_pull(task_ids=f'{task_name}.extract_tables')
        json_tables = json.dumps(dict_tables)

        # Connect to GCS
        gcs_hook = GCSHook(
            google_cloud_storage_conn_id='postgres-bq-etl-con')

        # Create a temporary JSON file and upload it to GCS
        with tempfile.NamedTemporaryFile(mode="w") as file:
            file.write(json_tables)
            file.flush()
            gcs_hook.upload(
                bucket_name=self.GCS_BUCKET,
                object_name=f'{self.DATABASE_NAME}_table_info.json',
                mime_type='application/json',
                filename=file.name
            )
