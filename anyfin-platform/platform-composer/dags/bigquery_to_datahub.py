from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonVirtualenvOperator

METADATA_RECIPES = {
        "bigquery_anyfin": "bigquery_anyfin_recipe.yaml", 
        "bigquery_segment": "bigquery_segment_recipe.yaml"
}

USAGE_RECIPE = "bigquery_usage_recipe.yaml"

default_args = {
    "owner": "ds-anyfin",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    'email': Variable.get('de_email', 'data-engineering@anyfin.com'),
}

dag = DAG(
    "datahub_metadata_ingestion",
    default_args=default_args,
    start_date=datetime(2021,9,12),
    catchup=False,
    schedule_interval="@daily"
)

def datahub_recipe(filename):
    import os
    from datahub.configuration.config_loader import load_config_file
    from datahub.ingestion.run.pipeline import Pipeline
    from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
    from tempfile import NamedTemporaryFile

    gcs = GoogleCloudStorageHook(google_cloud_storage_conn_id="google_cloud_storage_default")
    recipe = NamedTemporaryFile(suffix=".yaml")
    recipe.write(gcs.download("anyfin-data-model","datahub_recipes/"+filename)) 
    recipe.seek(os.SEEK_SET)        
    config = load_config_file(recipe.name)
    pipeline = Pipeline.create(config)
    pipeline.run()
    pipeline.raise_from_status()

ingest_tasks = []
for k, v in METADATA_RECIPES.items():
    ingest_task = PythonVirtualenvOperator(
        task_id=f"{k}_metadata_ingest",
        python_callable=datahub_recipe,
        requirements=['acryl-datahub[bigquery]'],
        op_args=[v],
        dag=dag
    )   
    ingest_tasks.append(ingest_task)

usage_ingest_task = PythonVirtualenvOperator(
    task_id="bigquery_usage_metadata_ingest",
    python_callable=datahub_recipe,
    op_args=[USAGE_RECIPE],
    requirements= ['acryl-datahub[bigquery-usage]'],
    dag=dag
)   

ingest_tasks >> usage_ingest_task
