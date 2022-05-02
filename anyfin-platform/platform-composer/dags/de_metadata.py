from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from utils import slack_notification
from functools import partial

SLACK_CONNECTION = 'slack_data_engineering'
QUERY = '''
select
    task_id,
    dag_id,
    run_id,
    start_date::timestamp with time zone::text as start_date,
    end_date::timestamp with time zone::text as end_date,
    duration,
    state,
    try_number,
    hostname,
    unixname,
    job_id,
    pool,
    queue,
    priority_weight,
    operator,
    queued_dttm,
    pid,
    max_tries,
    queued_by_job_id,
    external_executor_id,
    trigger_id,
    trigger_timeout
from task_instance 
where dag_id <> 'airflow_monitoring';
'''

default_args = {
    'owner': 'de-anyfin',
    'depends_on_past': False, 
    'retries': 0,
    'dagrun_timeout': timedelta(minutes=20),
    'on_failure_callback': partial(slack_notification.task_fail_slack_alert, SLACK_CONNECTION),
    'start_date': datetime(2022, 3, 9),
}

dag = DAG(
    dag_id="de_metadata", 
    default_args=default_args, 
    schedule_interval="0 1-17/2 * * *",  # Run this DAG every day every two hours between 1 and 17 
    max_active_runs=1,
    catchup=False
)

extract_airflow_db = PostgresToGCSOperator(
    task_id='extract_airflow_db',
    sql=QUERY,
    use_server_side_cursor=True,
    cursor_itersize=10000,
    bucket='sql-to-bq-etl',
    filename='metadata/airflow2_db_extract.json',
    postgres_conn_id='airflow_db',
    google_cloud_storage_conn_id='postgres-bq-etl-con',
    dag=dag
)

load_airflow_data_to_gcs = GCSToBigQueryOperator(
    task_id='load_airflow_data_to_gcs',
    bucket='sql-to-bq-etl',
    source_objects=['metadata/airflow2_db_extract.json', ],
    destination_project_dataset_table='anyfin:metadata.airflow2_db_metadata',
    time_partitioning={'type': 'DAY', 'field': 'start_date'},
    source_format='NEWLINE_DELIMITED_JSON',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id='postgres-bq-etl-con',
    google_cloud_storage_conn_id='postgres-bq-etl-con',
    dag=dag
)

extract_airflow_db >> load_airflow_data_to_gcs
