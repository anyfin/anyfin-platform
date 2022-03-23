from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator

default_args = {
    'owner': 'ds-anyfin',
    'depends_on_past': False, 
    'retries': 0,
    'email_on_failure': True,
    'email': Variable.get('de_email', 'data-engineering@anyfin.com'),
    'start_date': datetime(2022, 3, 21),
}

dag = DAG(
    dag_id="risk-variable-library", 
    default_args=default_args, 
    schedule_interval="0 2 * * *",  # Run this DAG once per day
    max_active_runs=1,
    catchup=False
)

schufa_parser = DataflowStartFlexTemplateOperator(
    body={
        "launchParameter": {
            "containerSpecGcsPath": "gs://sql-to-bq-etl/flex_templates/flex_template_schufa_parser",
            "jobName": "schufaparser",
            "environment": {
                "enableStreamingEngine": "false"
            },
        }
    },
    task_id="schufa_parser",
    location="europe-west1",
    project_id="anyfin",
    gcp_conn_id="postgres-bq-etl-con",
    wait_until_finished=True,
    dag=dag
)

schufa_features = DataflowStartFlexTemplateOperator(
    body={
        "launchParameter": {
            "containerSpecGcsPath": "gs://sql-to-bq-etl/flex_templates/flex_template_schufa_features",
            "jobName": "schufafeatures",
            "environment": {
                "enableStreamingEngine": "false"
            },
        }
    },
    task_id="schufa_features",
    location="europe-west1",
    project_id="anyfin",
    gcp_conn_id="postgres-bq-etl-con",
    wait_until_finished=True,
    dag=dag
)

schufa_parser >> schufa_features