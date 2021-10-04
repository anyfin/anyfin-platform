from datetime import datetime, timedelta
from airflow.contrib.kubernetes import secret
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.operators.sensors import ExternalTaskSensor

DBT_IMAGE = "eu.gcr.io/anyfin-platform/dbt-image:latest"

default_args = {
    'owner': 'ds-anyfin',
    'depends_on_past': False, 
    'retries': 0,
    'email_on_failure': True,
    'email': Variable.get('de_email', 'data-engineering@anyfin.com'),
    'start_date': datetime(2020, 12, 14),
}

dag = DAG(
    dag_id="bigquery_daily_dbt_models", 
    default_args=default_args, 
    schedule_interval="0 4 * * *",  # Run this DAG once per day
    max_active_runs=1,
    catchup=False
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
    arguments=["gsutil -m rsync -r gs://anyfin-data-model/ /dbt/ && dbt run --models tag:daily"],
    image_pull_policy='Always',
    is_delete_operator_pod=True,
    get_logs=True,
    dag=dag,
    secrets=[secret_volume]
)