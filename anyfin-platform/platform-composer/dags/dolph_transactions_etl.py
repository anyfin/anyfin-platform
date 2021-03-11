from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.bash_operator import BashOperator

PROJECT_NAME = 'anyfin'
INSTANCE_NAME = 'anyfin-dolph-read-replica'
BUCKET_NAME = 'sql-bq-etl'
WEEKLY_EXTRACT = 'pg_dumps/transactions.csv'
SCHEMA_OBJECT = 'pg_dumps/dolph_transactions_schema.json'
DESTINATION_TABLE = 'dolph.transactions'

default_args = {
    'owner': 'ds-anyfin',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 8),
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'email_on_failure': False,
    'email_on_retry': False
}

dag = DAG(
    'dolph_transactions_etl',
    default_args=default_args,
    catchup=False,
    schedule_interval='@weekly',
    max_active_runs=1
)

export_transactions = BashOperator(
    task_id='export_transactions',
    bash_command="gcloud sql export csv  anyfin-dolph-read-replica --project=anyfin "
                 f"--offload --async gs://{BUCKET_NAME}/{WEEKLY_EXTRACT} "
                 "--database=postgres --query='select * from transactions;'",
    dag=dag
)

wait_operation = BashOperator(
    task_id='wait_operation',
    bash_command="""
    operation_id=$(gcloud beta sql operations --project=anyfin list --instance=anyfin-dolph-read-replica | grep EXPORT | grep RUNNING | awk '{print $1}'); 
    if [ -z "$operation_id" ]; 
    then echo ""; 
    else gcloud beta sql operations wait --project anyfin $operation_id --timeout=3600; 
    fi;""",
    dag=dag
)

bq_load = GoogleCloudStorageToBigQueryOperator(
    task_id='bq_load',
    bucket=BUCKET_NAME,
    schema_object=SCHEMA_OBJECT,
    source_objects=[WEEKLY_EXTRACT, ],
    time_partitioning={'type': 'DAY', 'field': 'created_at'},
    source_format='CSV',
    create_disposition='CREATE_NEVER',
    write_disposition='WRITE_TRUNCATE',
    allow_jagged_rows=True,
    ignore_unknown_values=True,
    max_bad_records=10,
    destination_project_dataset_table=DESTINATION_TABLE,
    bigquery_conn_id='postgres-bq-etl-con',
    google_cloud_storage_conn_id='postgres-bq-etl-con',
    dag=dag
)

export_transactions >> wait_operation >> bq_load
