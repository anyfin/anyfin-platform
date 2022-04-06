from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.operators.bash_operator import BashOperator
import json
import logging
from utils.dbtTaskFactory import dbtTaskFactory


DBT_DIR = '/home/airflow/gcs/dags/anyfin-data-model'
MODEL_TAG = 'thrice-daily'

default_args = {
    'owner': 'de-anyfin',
    'depends_on_past': False, 
    'retries': 0,
    #'email_on_failure': True,
    #'email': Variable.get('de_email', 'data-engineering@anyfin.com'),
    'start_date': datetime(2022, 4, 1),
}


dag = DAG(
    dag_id="bigquery_dbt_models", 
    default_args=default_args, 
    schedule_interval=None,  
    max_active_runs=1,
    catchup=False,
    concurrency=12
)


task_export_intercom_diff = BigQueryToGCSOperator(
    task_id="export_intercom_diff",
    source_project_dataset_table="anyfin.product.intercom_diff_sync",
    destination_cloud_storage_uris=[f"gs://anyfin-customer-facts/current-partition.json"],
    export_format='NEWLINE_DELIMITED_JSON',
    gcp_conn_id="bigquery-composer-connection",
    dag=dag
)

task_save_intercom_previous_sync = BigQueryToBigQueryOperator(
    task_id=f'save_intercom_previous_sync',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    source_project_dataset_tables=f'anyfin.product.intercom_sync',
    destination_project_dataset_table='anyfin.product.intercom_previous_sync',
    gcp_conn_id='bigquery-composer-connection',
    dag=dag
)

factory = dbtTaskFactory(DBT_DIR, dag, MODEL_TAG)

dbt_tasks = factory.generate_tasks_from_manifest()


task_save_intercom_previous_sync >> dbt_tasks["model.anyfin_bigquery.intercom_sync"] 
dbt_tasks["model.anyfin_bigquery.intercom_diff_sync"] >> task_export_intercom_diff
