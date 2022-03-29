from datetime import datetime, timedelta
from airflow.contrib.kubernetes import secret
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.operators.sensors import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
import json
import logging


DBT_DIR = '/home/airflow/gcs/dags/anyfin-data-model'
MODEL_TAG = 'thrice-daily'

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
    catchup=False,
    concurrency=100
)


def get_execution_date(execution_date, **kwargs):
    
    if execution_date.strftime("%H") == '04':
        logging.info('Timedelta 2h') # This log stays for debugging purposes
        return execution_date - timedelta(hours=2)
    else:
        logging.info('Timedelta 1h') # This log stays for debugging purposes
        return execution_date - timedelta(hours=1)


task_sensor_main_bq_etl =  ExternalTaskSensor (
    task_id='check_if_main_bq_etl_dedup_succeeded',
    external_dag_id='main_postgres_bq_etl',
    external_task_id='deduplication_success_confirmation',
    execution_date_fn=get_execution_date,
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

task_export_intercom_diff = BigQueryToCloudStorageOperator(
    task_id="export_intercom_diff",
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

def load_manifest():
    local_filepath = f"{DBT_DIR}/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)

    return data

def make_dbt_task(node, dbt_verb):
    """Returns an Airflow operator either run and test an individual model"""
    GLOBAL_CLI_FLAGS = "--no-write-json"
    model = node.split(".")[-1]

    if dbt_verb == "run":
        dbt_task = BashOperator(
            task_id=node, 
            bash_command=f"dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target prod --select {model}",
            dag=dag,
        )

    return dbt_task

data = load_manifest()

dbt_tasks = {}
nodes = {node:value for node, value in data['nodes'].items() 
         if node.split(".")[0] == "model" and MODEL_TAG in value["config"]["tags"]}


for node, node_info in nodes.items():
    if node_info["config"]["materialized"] == "ephemeral":
        continue
    dbt_tasks[node] = make_dbt_task(node, "run")


for node, node_info in nodes.items():
    if node_info["config"]["materialized"] == "ephemeral":
        continue

    # Set all model -> model dependencies
    dependencies = node_info["depends_on"]["nodes"]
    while dependencies:
        upstream_node = dependencies.pop()
        
        if not upstream_node in nodes:
            continue

        upstream_node_type = upstream_node.split(".")[0]
        if upstream_node_type != "model":
            continue
        
        if data["nodes"][upstream_node]["config"]["materialized"] == "ephemeral":
            dependencies += data["nodes"][upstream_node]["depends_on"]["nodes"]
        else:
            dbt_tasks[upstream_node] >> dbt_tasks[node]


#[task_sensor_main_bq_etl, task_sensor_dolph_bq_etl] >> run_dbt

task_save_intercom_previous_sync >> dbt_tasks["model.anyfin_bigquery.intercom_sync"] 
dbt_tasks["model.anyfin_bigquery.intercom_diff_sync"] >> task_export_intercom_diff
