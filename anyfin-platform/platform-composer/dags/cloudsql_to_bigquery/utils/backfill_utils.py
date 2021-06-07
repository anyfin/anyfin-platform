import os, json, re
import tempfile
import logging
from google.cloud import bigquery

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow import AirflowException



class BACKFILL:

    # Constructor
    def __init__(self, GCS_BUCKET, DATABASE_NAME):
        self.GCS_BUCKET = GCS_BUCKET
        self.DATABASE_NAME = DATABASE_NAME

        with open(os.path.join(os.path.dirname(__file__), f'../pg_schemas/{DATABASE_NAME}_schemas_state.json')) as f:
            self.TABLES = json.loads(f.read())

        self.DIRECT_EXPORT_TABLES = [(table_name, content) for table_name, content in self.TABLES.items() if 'backfill_method' in content and content['backfill_method'] == 'direct_export']
        self.NESTED_EXPORT_TABLES = [(table_name, content) for table_name, content in self.TABLES.items() if 'backfill_method' in content and content['backfill_method'] == 'nested_export']
        
    def get_tables(self):
        return self.TABLES
    def get_export_tables(self):
        return self.DIRECT_EXPORT_TABLES + self.NESTED_EXPORT_TABLES
    def get_direct_export_tables(self):
        return self.DIRECT_EXPORT_TABLES
    def get_nested_export_tables(self):
        return self.NESTED_EXPORT_TABLES


    # Function to convert postgres schema types into BQ format
    def convert_type_to_bq(self, t):
        if t == 'timestamp with time zone':
            return 'TIMESTAMP'

        elif t == 'date':
            return 'DATE'

        elif t == 'numeric':
            return 'FLOAT'

        elif t == 'integer':
            return 'INTEGER'

        elif t == 'boolean':
            return 'BOOLEAN'
        else: 
            return 'STRING'

    def fetch_num_of_rows_postgres(self):
        db = PostgresHook(f'{self.DATABASE_NAME}_replica')
        query = []
        for table_name, _ in self.EXPORT_TABLES:
            query.append(f"SELECT '{table_name}', COUNT(*) FROM {table_name}")
        query = ' UNION ALL '.join(query)

        rowcounts = db.get_records(query)
        counts = {}
        for row in rowcounts:
            counts[row[0]] = row[1]
        return counts

    # Generate BQ schema object as json file and upload to GCS
    def generate_schema(self, **kwargs):
        schema = []
        for column, t in kwargs['content']['schema'].items():
            if column.startswith("\"") and column.endswith("\""):
                column = re.findall('"([^"]*)"', column)[0]
            if t == 'ARRAY':
                mode = 'REPEATED'
                t = 'STRING'
            else:
                mode = 'NULLABLE'
            schema.append( {"mode": mode, "name": column, "type": self.convert_type_to_bq(t)} )
        schema.append( {"mode": "NULLABLE", "name": "_ingested_ts", "type": "TIMESTAMP"} )

        # Connect to GCS
        gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id='postgres-bq-etl-con')

        filename = f'pg_dumps/{self.DATABASE_NAME}_' + kwargs['name'] + '_schema.json'		

        # Create a temporary JSON file and upload it to GCS
        with tempfile.NamedTemporaryFile(mode="w") as file:
            json.dump(schema, file)
            file.flush()
            gcs_hook.upload(
                bucket=self.GCS_BUCKET,
                object=filename,
                mime_type='application/json',
                filename=file.name
            )

    def fetch_bigquery_data(self, table_name, destination_table, **kwargs):
        postgres_results = kwargs['ti'].xcom_pull(task_ids='sanity_check_postgres')
        client = bigquery.Client()
        query_job = client.query(
            f"""
            SELECT
            COUNT(*) as num_of_rows
            FROM `anyfin.{destination_table}`"""
        )
        results = query_job.result()
        res = []
        for row in results:
            res.append(row.num_of_rows)
        logging.info(f"{postgres_results[table_name]} - Postgres rows for {table_name}")
        logging.info(f"{int(res[0])} - BQ backup rows for {table_name}")
        if postgres_results[table_name] * 0.999 <= int(res[0]):
            return True
        else:
            raise AirflowException("Number of rows in Postgres is higher than BQ")
