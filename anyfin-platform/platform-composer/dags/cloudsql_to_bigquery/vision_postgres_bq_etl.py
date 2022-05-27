from datetime import datetime, timedelta
from functools import partial

from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from cloudsql_to_bigquery.utils.etl_utils import ETL
from utils import slack_notification

PROJECT_NAME = "anyfin"
GCS_BUCKET = "sql-to-bq-etl"
DATAFLOW_BUCKET = "etl-dataflow-bucket"
DATABASE_NAME = "vision"
SLACK_CONNECTION = "slack_data_engineering"

ETL = ETL(GCS_BUCKET=GCS_BUCKET, DATABASE_NAME=DATABASE_NAME)

default_args = {
    "owner": "de-anyfin",
    "depends_on_past": False,
    "start_date": datetime(2022, 5, 25),
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": partial(
        slack_notification.task_fail_slack_alert, SLACK_CONNECTION
    ),
}

dag = DAG(
    f"{DATABASE_NAME}_postgres_bq_etl",
    default_args=default_args,
    catchup=False,
    schedule_interval="0 3 * * *",
    max_active_runs=1,
    concurrency=3,
)

dedup_tasks = []

for table in ETL.get_tables():
    load_raw = BigQueryOperator(
        task_id="load_raw_" + table,
        sql="""
        SELECT
            {}
        FROM
        EXTERNAL_QUERY("anyfin.eu.vision",
            "SELECT {} FROM {} WHERE created_at::date='{}';");
        """.format(
            ETL.get_bq_columns(table), table, ETL.get_pg_columns(table), "{{ ds }}"
        ),
        destination_dataset_table="""anyfin.{}.{}${}""".format(
            DATABASE_NAME, table, "{{ ds_nodash }}"
        ),
        use_legacy_sql=False,
        bigquery_conn_id="postgres-bq-etl-con",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_NEVER",
        dag=dag,
    )
