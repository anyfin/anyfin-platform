from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator

from utils import slack_notification
from functools import partial

GCS_BUCKET = "anyfin-rvl"
FLEX_TEMPLATES_DIR = "flex_templates"
SLACK_CONNECTION = 'slack_data_engineering'
PARAMS = {"maxWorkers": 8, "machineType": "n1-standard-2", "enableStreamingEngine": "false"}

default_args = {
    'owner': 'de-anyfin',
    'depends_on_past': False, 
    'retries': 0,
    'on_failure_callback': partial(slack_notification.task_fail_slack_alert, SLACK_CONNECTION),
    'start_date': datetime(2022, 3, 21),
}

dag = DAG(
    dag_id="risk-variable-library", 
    default_args=default_args, 
    schedule_interval="0 5 * * *",  # Run this DAG once per day
    max_active_runs=1,
    catchup=False
)

schufa = DataflowStartFlexTemplateOperator(
    body={
        "launchParameter": {
            "containerSpecGcsPath": f"gs://{GCS_BUCKET}/{FLEX_TEMPLATES_DIR}/flex_template_schufa",
            "jobName": "rvlschufa",
            "environment": PARAMS
        }
    },
    task_id="schufa",
    location="europe-west1",
    project_id="anyfin",
    gcp_conn_id="postgres-bq-etl-con",
    wait_until_finished=True,
    dag=dag
)

asiakastieto = DataflowStartFlexTemplateOperator(
    body={
        "launchParameter": {
            "containerSpecGcsPath": f"gs://{GCS_BUCKET}/{FLEX_TEMPLATES_DIR}/flex_template_asiakastieto",
            "jobName": "rvlasiakastieto",
            "environment": PARAMS
        }
    },
    task_id="asiakastieto",
    location="europe-west1",
    project_id="anyfin",
    gcp_conn_id="postgres-bq-etl-con",
    wait_until_finished=True,
    dag=dag
)

# # Temporarily commenting out until it's refactored
# crif_buergel = DataflowStartFlexTemplateOperator(
#     body={
#         "launchParameter": {
#             "containerSpecGcsPath": f"gs://{GCS_BUCKET}/{FLEX_TEMPLATES_DIR}/flex_template_crif_buergel",
#             "jobName": "rvlcrifbuergel",
#             "environment": PARAMS
#         }
#     },
#     task_id="crif_buergel",
#     location="europe-west1",
#     project_id="anyfin",
#     gcp_conn_id="postgres-bq-etl-con",
#     wait_until_finished=True,
#     dag=dag
# )

uc = DataflowStartFlexTemplateOperator(
    body={
        "launchParameter": {
            "containerSpecGcsPath": f"gs://{GCS_BUCKET}/{FLEX_TEMPLATES_DIR}/flex_template_uc",
            "jobName": "rvluc",
            "environment": PARAMS
        }
    },
    task_id="uc",
    location="europe-west1",
    project_id="anyfin",
    gcp_conn_id="postgres-bq-etl-con",
    wait_until_finished=True,
    dag=dag
)

internal_lookup = DataflowStartFlexTemplateOperator(
    body={
        "launchParameter": {
            "containerSpecGcsPath": f"gs://{GCS_BUCKET}/{FLEX_TEMPLATES_DIR}/flex_template_internal_lookup",
            "jobName": "rvlinternallookup",
            "environment": PARAMS
        }
    },
    task_id="internal_lookup",
    location="europe-west1",
    project_id="anyfin",
    gcp_conn_id="postgres-bq-etl-con",
    wait_until_finished=True,
    dag=dag
)

de_capacity = DataflowStartFlexTemplateOperator(
    body={
        "launchParameter": {
            "containerSpecGcsPath": f"gs://{GCS_BUCKET}/{FLEX_TEMPLATES_DIR}/flex_template_de_capacity",
            "jobName": "rvldecapacity",
            "environment": PARAMS
        }
    },
    task_id="de_capacity",
    location="europe-west1",
    project_id="anyfin",
    gcp_conn_id="postgres-bq-etl-con",
    wait_until_finished=True,
    dag=dag
)

fi_capacity = DataflowStartFlexTemplateOperator(
    body={
        "launchParameter": {
            "containerSpecGcsPath": f"gs://{GCS_BUCKET}/{FLEX_TEMPLATES_DIR}/flex_template_fi_capacity",
            "jobName": "rvlficapacity",
            "environment": PARAMS
        }
    },
    task_id="fi_capacity",
    location="europe-west1",
    project_id="anyfin",
    gcp_conn_id="postgres-bq-etl-con",
    wait_until_finished=True,
    dag=dag
)

se_capacity = DataflowStartFlexTemplateOperator(
    body={
        "launchParameter": {
            "containerSpecGcsPath": f"gs://{GCS_BUCKET}/{FLEX_TEMPLATES_DIR}/flex_template_se_capacity",
            "jobName": "rvlsecapacity",
            "environment": PARAMS
        }
    },
    task_id="se_capacity",
    location="europe-west1",
    project_id="anyfin",
    gcp_conn_id="postgres-bq-etl-con",
    wait_until_finished=True,
    dag=dag
)


internal_lookup >> [de_capacity, fi_capacity, se_capacity]
schufa >> de_capacity
asiakastieto >> fi_capacity
uc >> se_capacity