from datetime import datetime, timedelta
from airflow.contrib.kubernetes import secret
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteTableOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator

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

delete_materialized_view = BigQueryDeleteTableOperator(
    task_id="delete_materialized_view",
    deletion_dataset_table=f"anyfin.pfm.all_widgets",
    bigquery_conn_id='bigquery_default',
    dag=dag
)

create_materialized_view = BashOperator(
        task_id='create_materialized_view',
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

run_dbt >> delete_materialized_view >> create_materialized_view