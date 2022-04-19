from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.operators.bash import BashOperator

from utils.DbtTaskFactory import DbtTaskFactory
from utils import slack_notification
from functools import partial

DBT_DIR = '/home/airflow/gcs/dags/anyfin-data-model'
MODEL_TAG = 'segment'
SLACK_CONNECTION = 'slack_data_engineering'

default_args = {
    'owner': 'de-anyfin',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'on_failure_callback': partial(slack_notification.task_fail_slack_alert, SLACK_CONNECTION),
    'start_date': datetime(2021, 6, 23),
}

dag = DAG(
    dag_id="bigquery_segment_dbt_models",
    default_args=default_args,
    schedule_interval="42 8,20 * * *",  # Run this DAG at 08:42 and 20:42 UTC This is after segment loads to BQ
    max_active_runs=1,
    catchup=False
)

delete_materialized_view = BigQueryDeleteTableOperator(
    task_id="delete_all_widgets_materialized_view",
    deletion_dataset_table=f"anyfin.pfm.all_widgets",
    bigquery_conn_id='bigquery_default',
    dag=dag
)

# Create materialized view here because dbt does not support it
create_materialized_view = BashOperator(
    task_id='create_all_widgets_materialized_view',
    # Executing 'bq' command requires Google Cloud SDK which comes
    # preinstalled in Cloud Composer.
    bash_command="bq query --use_legacy_sql=false "
                 "'CREATE MATERIALIZED VIEW  anyfin.pfm.all_widgets AS SELECT  "
                 "widget_id, "
                 "user_id, "
                 "widget_type, "
                 "MIN(IF(action=\"CREATED\", timestamp, null)) as created_at, "
                 "MAX(IF(action=\"DELETED\", timestamp, null)) as deleted_at "
                 "FROM `anyfin.tracking.widget_log` "
                 "GROUP BY 1, 2, 3'",
    dag=dag
)

factory = DbtTaskFactory(DBT_DIR, dag, MODEL_TAG, run_options=['--full-refresh'])

dbt_tasks = factory.generate_tasks_from_manifest()

dbt_tasks['model.anyfin_bigquery.widget_log'] >> delete_materialized_view >> create_materialized_view
