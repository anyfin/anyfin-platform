import logging
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.postgres_hook import PostgresHook
from utils import slack_notification
from functools import partial
from gdpr.report import PersonalDataReport

SLACK_CONNECTION = "slack_data_engineering"
GCS_BUCKET = "anyfin-platform-gdpr"
GCS_PATH = "reports"
QUERIES_PATH = os.path.join(os.path.dirname(__file__), "queries")


def fetch_pg_data(hook, query, cid, **kwargs):
    with open(os.path.join(QUERIES_PATH, query)) as f:
        sql = f.read().format(customer_id=cid)
        _results = hook.get_records(sql)
    return _results


def generate_report_document(**kwargs):
    dag_run = kwargs.get("dag_run")
    customer_id = dag_run.conf["customer_id"]
    results = {}
    gcs = GoogleCloudStorageHook(google_cloud_storage_conn_id="google_cloud_default")
    for db in os.listdir(QUERIES_PATH):
        hook = PostgresHook(f"{db}_replica")
        queries = os.listdir(f"{QUERIES_PATH}/{db}")
        for query in queries:

            results[f"{db}/{query.split('.')[0]}"] = fetch_pg_data(
                hook, f"{db}/{query}", cid=customer_id
            )
    report_tmp_file = PersonalDataReport(customer_id, results).generate()
    gcs.upload(GCS_BUCKET, f"{GCS_PATH}/{customer_id}.xlsx", report_tmp_file)
    logging.info(results)


default_args = {
    "owner": "de-anyfin",
    "depends_on_past": False,
    "start_date": datetime(2022, 5, 10),
    "on_failure_callback": partial(
        slack_notification.task_fail_slack_alert, SLACK_CONNECTION
    ),
}

dag = DAG(
    "gdpr_data_requests",
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    max_active_runs=3,
    concurrency=3,
)

generate_report = PythonOperator(
    task_id="generate_report", python_callable=generate_report_document, dag=dag
)
