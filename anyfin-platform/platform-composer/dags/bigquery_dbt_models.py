from datetime import datetime, timedelta
from airflow.contrib.kubernetes import secret
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.operators.sensors import ExternalTaskSensor

DBT_IMAGE = "eu.gcr.io/anyfin-platform/dbt-image:latest"

default_args = {
    'owner': 'ds-anyfin',
    'depends_on_past': False, 
    'retries': 0,
    'email_on_failure': True,
    'email': Variable.get('de_email', 'data-engineering@anyfin.com'),
    'start_date': datetime(2020, 11, 22),
}


dag = DAG(
    dag_id="bigquery_dbt_models", 
    default_args=default_args, 
    schedule_interval="0 4,10,12 * * *",  # Run this DAG three times per day at the specified hours
    max_active_runs=1,
    catchup=False
)


task_sensor_main_bq_etl = ExternalTaskSensor (
    task_id='check_if_main_bq_etl_dedup_succeeded',
    external_dag_id='main_postgres_bq_etl',
    external_task_id='deduplication_success_confirmation',
    execution_delta=timedelta(hours=1),
    allowed_states=['success'],
    timeout=1800,                      # 1800 seconds = 30min of sensoring before retry is triggered
    retry_delay=timedelta(minutes=40), # Wait 40min before sensoring again
    retries=2,                         # Will sensor a total of retries + 1 times
    check_existance=True,
    dag=dag
)

task_sensor_dolph_bq_etl = ExternalTaskSensor (
    task_id='check_if_dolph_bq_etl_dedup_succeeded',
    external_dag_id='dolph_postgres_bq_etl',
    external_task_id='deduplication_success_confirmation',
    execution_delta=timedelta(hours=1),
    allowed_states=['success'],
    timeout=1800,                      # 1800 seconds = 30min of sensoring before retry is triggered
    retry_delay=timedelta(minutes=40), # Wait 40min before sensoring again
    retries=2,                         # Will sensor a total of retires + 1 times
    check_existance=True,
    dag=dag
)


secret_volume = secret.Secret(
    'volume',
    '/dbt/dbt/credentials/',  
    'dbt-service-account', 
    'service-account.json'
)


run_dbt = KubernetesPodOperator(
    image=DBT_IMAGE,
    namespace='default',
    task_id="run-models",
    name="run-models",
    cmds=["/bin/bash", "-c"],
    arguments=["gsutil -m rsync -r gs://anyfin-data-model/ /dbt/ && dbt run --models tag:thrice-daily --exclude tag:daily"],
    image_pull_policy='Always',
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
    secrets=[secret_volume]
)


now = datetime.now()
current_date = f"{now.year}{now.month}{now.day}"

task_export_latest_user_facts_partition_to_gcs = BigQueryToCloudStorageOperator(
    task_id="export_latest_user_facts_partition_to_gcs",
    source_project_dataset_table="anyfin.product.intercom_diff_sync",
    destination_cloud_storage_uris=[f"gs://anyfin-customer-facts/current-partition.json"],
    export_format='NEWLINE_DELIMITED_JSON',
    bigquery_conn_id="bigquery_default",
    dag=dag
)

task_save_intercom_previous_sync = BigQueryToBigQueryOperator(
    task_id=f'save_intercom_previous_sync',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    source_project_dataset_tables=f'anyfin.product.intercom_sync',
    destination_project_dataset_table='anyfin.product.intercom_previous_sync',
    bigquery_conn_id='postgres-bq-etl-con',
    dag=dag
)


[task_sensor_main_bq_etl, task_sensor_dolph_bq_etl] >> run_dbt
task_save_intercom_previous_sync >> run_dbt >> task_export_latest_user_facts_partition_to_gcs
