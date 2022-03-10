from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.mysql_to_gcs import MySqlToGoogleCloudStorageOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator

QUERY = '''
select 
    dag_id, 
    task_id, 
    convert(timestamp(execution_date), char) as execution_date, 
    convert(timestamp(start_date), char) as start_date, 
    convert(timestamp(end_date), char) as end_date, 
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
    pool_slots
from task_instance 
where dag_id <> 'airflow_monitoring';
'''

default_args = {
    'owner': 'ds-anyfin',
    'depends_on_past': False, 
    'retries': 0,
    'email_on_failure': True,
    'email': Variable.get('de_email', 'data-engineering@anyfin.com'),
    'start_date': datetime(2022, 3, 9),
}

dag = DAG(
    dag_id="de_metadata", 
    default_args=default_args, 
    schedule_interval="0 1 * * *",  # Run this DAG once per day
    max_active_runs=1,
    catchup=False
)

extract_airflow_db = MySqlToGoogleCloudStorageOperator(
    task_id='extract_airflow_db',
    sql=QUERY,
    bucket='sql-to-bq-etl',
    filename='metadata/airflow_db_extract.json',
    mysql_conn_id='airflow_db',
    provide_context=True,
    google_cloud_storage_conn_id='postgres-bq-etl-con',
    dag=dag
)

load_airflow_data_to_gcs = GoogleCloudStorageToBigQueryOperator(
    task_id='load_airflow_data_to_gcs',
    bucket='sql-to-bq-etl',
    source_objects=['metadata/airflow_db_extract.json', ],
    destination_project_dataset_table='anyfin:metadata.airflow_db_metadata',
    time_partitioning={'type': 'DAY', 'field': 'execution_date'},
    source_format='NEWLINE_DELIMITED_JSON',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    bigquery_conn_id='postgres-bq-etl-con',
    google_cloud_storage_conn_id='postgres-bq-etl-con',
    dag=dag
)

extract_airflow_db >> load_airflow_data_to_gcs