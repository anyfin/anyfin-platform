from datetime import datetime, timedelta
from airflow.contrib.kubernetes import secret
from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.operators.sensors import ExternalTaskSensor

DBT_IMAGE = "eu.gcr.io/anyfin-platform/dbt-image:latest"

default_args = {
    'owner': 'ds-anyfin',
    'depends_on_past': False, 
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': Variable.get('de_email', 'data-engineering@anyfin.com'),
    'start_date': datetime(2021, 6, 23),
}

dag = DAG(
    dag_id="bigquery_segment_dbt_models", 
    default_args=default_args, 
    schedule_interval="42 8,20 * * *",  # Run this DAG at 08:42 and 20:42 UTC This is after segment loads to BQ
    max_active_runs=1,
    catchup=False
)

secret_volume = secret.Secret(
    'volume',
    '/dbt/credentials/',  
    'dbt-service-account', 
    'service-account.json'
)


run_dbt = KubernetesPodOperator(
    image=DBT_IMAGE,
    namespace='default',
    task_id="run-models",
    name="run-models",
    cmds=["/bin/bash", "-c"],
    arguments=["gsutil -m rsync -r gs://anyfin-data-model/ /dbt/ && dbt run --models tag:segment"],
    image_pull_policy='Always',
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
    secrets=[secret_volume]
)