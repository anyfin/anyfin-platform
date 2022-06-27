from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from utils import slack_notification
from functools import partial

slack_connection = 'slack_data_engineering'

UTILS_DIR = '/home/airflow/gcs/dags/utils/'
DBT_HOME_DIR = '/home/airflow/gcs/dags/anyfin-data-model/'
DBT_SOURCE_FRESHNESS_TABLE = 'metadata.source_freshness_metadata'
PROJECT_NAME = 'anyfin'
SLACK_CONNECTION = 'slack_data_engineering'

today_partition = date.today().strftime('%Y%m%d')

default_args = {
    'owner': 'de-anyfin',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 14),
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
    'dagrun_timeout': timedelta(minutes=30),
    'on_failure_callback': partial(slack_notification.task_fail_slack_alert, SLACK_CONNECTION),
}

with DAG(
        dag_id='source_freshness',
        default_args=default_args,
        catchup=False,
        description='This DAG is used to check freshness of sources in sources.yml',
        schedule_interval='0 12 * * 1-6',
        max_active_runs=1
) as dag:

    dbt_source_freshness = BashOperator(
        task_id='source_freshness',
        bash_command=f'cd {DBT_HOME_DIR} && dbt source freshness -o {DBT_HOME_DIR}target/sources.json',
        retries=0,
    )

    parse_freshness = BashOperator(
        task_id='parse_freshness',
        bash_command=f'python3 {UTILS_DIR}parse_freshness.py',
        trigger_rule='all_done',
    )

    load_freshness_data = GCSToBigQueryOperator(
        task_id=f'load_freshness_data',
        bucket='europe-west1-platform-b746cda0-bucket',
        schema_object='data/dbt-source-freshness-metadata-schema.json',
        source_objects=['data/source-freshness.csv', ],
        time_partitioning={'type': 'DAY', 'field': 'execution_date'},
        source_format='CSV',
        create_disposition='CREATE_NEVER',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_jagged_rows=True,
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
        max_bad_records=10,
        destination_project_dataset_table=f'{PROJECT_NAME}.{DBT_SOURCE_FRESHNESS_TABLE}${today_partition}',
        bigquery_conn_id='postgres-bq-etl-con',
        google_cloud_storage_conn_id='postgres-bq-etl-con',
    )

dbt_source_freshness >> parse_freshness >> load_freshness_data
