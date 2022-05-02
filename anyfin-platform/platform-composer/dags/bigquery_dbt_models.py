from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator
from utils.DbtTaskFactory import DbtTaskFactory
from utils import slack_notification
from functools import partial


DBT_DIR = '/home/airflow/gcs/dags/anyfin-data-model'
MODEL_TAG = 'thrice-daily'
SLACK_CONNECTION = 'slack_data_engineering'

default_args = {
    'owner': 'de-anyfin',
    'depends_on_past': False, 
    'retries': 0,
    'on_failure_callback': partial(slack_notification.task_fail_slack_alert, SLACK_CONNECTION),
    'start_date': datetime(2022, 4, 1),
}


dag = DAG(
    dag_id="bigquery_dbt_models", 
    default_args=default_args, 
    schedule_interval=None,  
    max_active_runs=3,
    catchup=False,
    concurrency=12
)


task_export_intercom_diff = BigQueryToGCSOperator(
    task_id="export_intercom_diff",
    source_project_dataset_table="anyfin.product.intercom_diff_sync",
    destination_cloud_storage_uris=["gs://anyfin-customer-facts/current-partition.json"],
    export_format='NEWLINE_DELIMITED_JSON',
    gcp_conn_id="bigquery-composer-connection",
    dag=dag
)

task_save_intercom_previous_sync = BigQueryToBigQueryOperator(
    task_id='save_intercom_previous_sync',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    source_project_dataset_tables='anyfin.product.intercom_sync',
    destination_project_dataset_table='anyfin.product.intercom_previous_sync',
    gcp_conn_id='bigquery-composer-connection',
    dag=dag
)

factory = DbtTaskFactory(DBT_DIR, dag, MODEL_TAG)

dbt_tasks = factory.generate_tasks_from_manifest()


task_save_intercom_previous_sync >> dbt_tasks["model.anyfin_bigquery.intercom_sync"] 
dbt_tasks["model.anyfin_bigquery.intercom_diff_sync"] >> task_export_intercom_diff
