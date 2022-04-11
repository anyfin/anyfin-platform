from datetime import datetime
from airflow import DAG

from utils.DbtTaskFactory import DbtTaskFactory
from utils import slack_notification
from functools import partial

DBT_DIR = '/home/airflow/gcs/dags/anyfin-data-model'
MODEL_TAG = 'daily'
SLACK_CONNECTION = 'slack_data_engineering'

default_args = {
    'owner': 'de-anyfin',
    'depends_on_past': False, 
    'retries': 0,
    'on_failure_callback': partial(slack_notification.task_fail_slack_alert, SLACK_CONNECTION),
    'start_date': datetime(2022, 4, 1),
}

dag = DAG(
    dag_id="bigquery_daily_dbt_models", 
    default_args=default_args, 
    schedule_interval="0 4 * * *",  # Run this DAG once per day
    max_active_runs=1,
    catchup=False
)

factory = DbtTaskFactory(DBT_DIR, dag, MODEL_TAG)

dbt_tasks = factory.generate_tasks_from_manifest()