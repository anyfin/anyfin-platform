from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.operators.bash_operator import BashOperator
import json
import logging


DBT_DIR = '/home/airflow/gcs/dags/anyfin-data-model'
MODEL_TAG = 'thrice-daily'

default_args = {
    'owner': 'de-anyfin',
    'depends_on_past': False, 
    'retries': 0,
    #'email_on_failure': True,
    #'email': Variable.get('de_email', 'data-engineering@anyfin.com'),
    'start_date': datetime(2020, 11, 22),
}


with DAG(
    dag_id="bigquery_dbt_models", 
    default_args=default_args, 
    schedule_interval=None,  
    max_active_runs=1,
    catchup=False,
    concurrency=6
) as dag:


    # task_export_intercom_diff = BigQueryToGCSOperator(
    #     task_id="export_intercom_diff",
    #     source_project_dataset_table="anyfin.product.intercom_diff_sync",
    #     destination_cloud_storage_uris=[f"gs://anyfin-customer-facts/current-partition.json"],
    #     export_format='NEWLINE_DELIMITED_JSON',
    #     gcp_conn_id="bigquery-composer-connection",
    #     dag=dag
    # )

    # task_save_intercom_previous_sync = BigQueryToBigQueryOperator(
    #     task_id=f'save_intercom_previous_sync',
    #     create_disposition='CREATE_IF_NEEDED',
    #     write_disposition='WRITE_TRUNCATE',
    #     source_project_dataset_tables=f'anyfin.product.intercom_sync',
    #     destination_project_dataset_table='anyfin.product.intercom_previous_sync',
    #     gcp_conn_id='bigquery-composer-connection',
    #     dag=dag
    # )

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
                bash_command=f"dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target prod --select {model}"
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
        dependencies = list(set(dependencies))
        while dependencies:
            upstream_node = dependencies.pop()
            
            if not upstream_node in nodes:
                continue

            upstream_node_type = upstream_node.split(".")[0]
            if upstream_node_type != "model":
                continue
            
            if nodes[upstream_node]["config"]["materialized"] == "ephemeral":
                dependencies += nodes[upstream_node]["depends_on"]["nodes"]
                dependencies = list(set(dependencies))
            else:
                dbt_tasks[upstream_node] >> dbt_tasks[node]


    #task_save_intercom_previous_sync >> dbt_tasks["model.anyfin_bigquery.intercom_sync"] 
    #dbt_tasks["model.anyfin_bigquery.intercom_diff_sync"] >> task_export_intercom_diff
