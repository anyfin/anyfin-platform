import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration

from cloudsql_to_bigquery.utils.backfill_utils import BACKFILL
from cloudsql_to_bigquery.utils.db_info_utils import DATABASES_INFO
from utils import slack_notification
from functools import partial

PROJECT_NAME = 'anyfin'
GCS_BUCKET = 'sql-to-bq-etl'
DATAFLOW_BUCKET = 'etl-dataflow-bucket'
TEMPLATE_FILE = os.path.dirname(os.path.realpath(__file__)) + '/beam_utils/pg_bq_etl.py'
SETUP_FILE = os.path.dirname(os.path.realpath(__file__)) + '/beam_utils/setup.py'
DAG_PATH = os.path.dirname(os.path.realpath(__file__))
SLACK_CONNECTION = 'slack_data_engineering'

default_args = {
	'owner': 'de-anyfin',
	'depends_on_past': False,
	'start_date': datetime(2022, 4, 1),
	'retries': 1,
	'retry_delay': timedelta(minutes=30),
	'on_failure_callback': partial(slack_notification.task_fail_slack_alert, SLACK_CONNECTION),
}

with DAG(
	'postgres_bq_backfill',
	default_args=default_args,
	catchup=False,
	schedule_interval='0 13 * * SUN',
	max_active_runs=1,
	concurrency=12
) as dag:

	for DB in DATABASES_INFO:
		DATABASE_NAME, INSTANCE_NAME = DB['DATABASE_NAME'], DB['INSTANCE_NAME']
		DESTINATION_PROJECT = DB['DESTINATION_PROJECT']
		
		if DESTINATION_PROJECT == 'anyfin-staging':
			DESTINATION_DATASET = DATABASE_NAME.split('-')[0] + '_staging'  # Removes -staging from db name
		else:
			DESTINATION_DATASET = f'{DATABASE_NAME}_staging'
		
		backfill = BACKFILL(GCS_BUCKET=GCS_BUCKET, DATABASE_NAME=DATABASE_NAME)
		if backfill.get_beam_tables():
			# Operator to backfill tables with beam
			beam_backfill = BeamRunPythonPipelineOperator(
				task_id=f'{DATABASE_NAME}_beam_backfill',
				runner="DataflowRunner",
				py_file=TEMPLATE_FILE,
				py_options=[],
				py_requirements=[],
				py_interpreter='python3',
				py_system_site_packages=True,
				pipeline_options={
					"project": PROJECT_NAME,
					"worker_zone": 'europe-west1-b',
					"region": "europe-west1",
					"staging_location": f'gs://{DATAFLOW_BUCKET}/Staging/',
					"experiment": "use_beam_bq_sink",
					"date": '{{ds}}',
					"machine_type": "n1-standard-4",
					"num-workers": '1',
					"temp_location": f'gs://{DATAFLOW_BUCKET}/Temp/',
					"database_name": f"{DATABASE_NAME}",
					"destination_project": f"{DESTINATION_PROJECT}",
					"destination_dataset": f'{DESTINATION_DATASET}',
					"setup_file": SETUP_FILE,
					"poll_sleep": 30,
					"backfill": "true"
				},
				email_on_failure=True,
				dataflow_config=DataflowConfiguration(
					job_name=f'{DATABASE_NAME}-etl-backfill', 
					project_id=PROJECT_NAME, 
					location="europe-west1",
					gcp_conn_id="dataflow-etl-connection"
				)
			)