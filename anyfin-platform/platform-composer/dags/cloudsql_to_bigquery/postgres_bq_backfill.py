import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from DataFlowPython3Operator import DataFlowPython3Operator

from cloudsql_to_bigquery.utils.backfill_utils import BACKFILL


PROJECT_NAME = 'anyfin'
GCS_BUCKET = 'sql-to-bq-etl'
DATAFLOW_BUCKET = 'etl-dataflow-bucket'
DATABASE_NAMES = ['main', 'dolph', 'pfm', 'psd2', 'ddi', 'sendout', 'savings']
INSTANCE_NAMES = ['anyfin-main-replica', 'anyfin-dolph-read-replica', 'pfm-replica', 'psd2-replica', 'anyfin-ddi-service-replica', 'sendout-replica', 'savings-replica']
TEMPLATE_FILE = os.path.dirname(os.path.realpath(__file__)) + '/beam_utils/pg_bq_etl.py'
SETUP_FILE = os.path.dirname(os.path.realpath(__file__)) + '/beam_utils/setup.py'
DAG_PATH = os.path.dirname(os.path.realpath(__file__))

default_args = {
	'owner': 'ds-anyfin',
	'depends_on_past': False,
	'start_date': datetime(2020, 9, 8),
	'retries': 2,
	'retry_delay': timedelta(minutes=30),
	'email_on_failure': True,
	'email_on_retry': False,
	'email': Variable.get('de_email', 'data-engineering@anyfin.com')
}

dag = DAG(
	'postgres_bq_backfill',
	default_args=default_args,
	catchup=False,
	schedule_interval='0 0 * * SUN',
	max_active_runs=1,
	concurrency=3
)

for DATABASE_NAME, INSTANCE_NAME in zip(DATABASE_NAMES, INSTANCE_NAMES):
	
	backfill = BACKFILL(GCS_BUCKET=GCS_BUCKET, DATABASE_NAME=DATABASE_NAME)
	if backfill.get_beam_tables():
		# Operator to backfill tables with beam
		beam_backfill = DataFlowPython3Operator(
			task_id=f'{DATABASE_NAME}_beam_backfill',
			py_file=TEMPLATE_FILE,
			job_name=f'{DATABASE_NAME}-etl-backfill',
			provide_context=True,
			dataflow_default_options={
				"project": f'{PROJECT_NAME}',
				"worker_zone": 'europe-west1-b',
				"region": "europe-west1",
				"staging_location": f'gs://{DATAFLOW_BUCKET}/Staging/',
				"runner": "DataFlowRunner",
				"experiment": "use_beam_bq_sink",
				"date": '{{ds}}',
				"backfill": 'true',
				"machine_type": "n1-standard-4",
				"setup_file": SETUP_FILE,
				"temp_location": f'gs://{DATAFLOW_BUCKET}/Temp/',
				"database_name": f"{DATABASE_NAME}"
			},
			delegate_to="postgres-bq-etl@anyfin.iam.gserviceaccount.com",
			options={
				"num-workers": '1'
			},
			gcp_conn_id='postgres-bq-etl-con',
			poll_sleep=30,
			dag=dag
		)