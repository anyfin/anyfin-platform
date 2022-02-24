from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from cloudsql_to_bigquery.utils.etl_utils import ETL

PROJECT_NAME = 'anyfin'
GCS_BUCKET = 'sql-to-bq-etl'
DATAFLOW_BUCKET = 'etl-dataflow-bucket'
DATABASE_NAME = 'assess'

ETL = ETL(GCS_BUCKET=GCS_BUCKET, DATABASE_NAME=DATABASE_NAME)

default_args = {
    'owner': 'ds-anyfin',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 8),
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': Variable.get('de_email', 'data-engineering@anyfin.com')
}

dag = DAG(
    f'{DATABASE_NAME}_postgres_bq_etl',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 3 * * *',
    max_active_runs=1,
    concurrency=3
)

dedup_tasks = []

for table in ETL.get_tables():
    load_raw = BigQueryOperator(
        task_id='load_raw_' + table,
        sql="""
        SELECT
            {}
        FROM
        EXTERNAL_QUERY("anyfin.eu.assess-replica",
            "SELECT {} FROM lookups WHERE ts::date='{}';");
        """.format(
            ETL.get_bq_columns(table),
            ETL.get_pg_columns(table),
            '{{ ds }}'
        ),
        destination_dataset_table=f"anyfin.{DATABASE_NAME}_staging.{table}_raw",
        use_legacy_sql=False,
        bigquery_conn_id='postgres-bq-etl-con',
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_NEVER',
        dag=dag
    )
