import os
from datetime import datetime, timedelta
import psycopg2

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from DataFlowPython3Operator import DataFlowPython3Operator
from airflow.contrib.operators.gcp_compute_operator import GceInstanceStartOperator, GceInstanceStopOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.bash_operator import BashOperator

from cloudsql_to_bigquery.utils.backfill_utils import BACKFILL


PROJECT_NAME = 'anyfin'
GCS_BUCKET = 'sql-to-bq-etl'
DATABASE_NAMES = ['main', 'dolph', 'pfm', 'psd2']
INSTANCE_NAMES = ['anyfin-main-replica', 'anyfin-dolph-read-replica', 'pfm-replica', 'psd2-replica']
TEMPLATE_FILE = os.path.dirname(os.path.realpath(__file__)) + '/beam_utils/pg_bq_etl.py'
SETUP_FILE = os.path.dirname(os.path.realpath(__file__)) + '/beam_utils/setup.py'
DAG_PATH = os.path.dirname(os.path.realpath(__file__))

default_args = {
	'owner': 'ds-anyfin',
	'depends_on_past': False,
	'start_date': datetime(2020, 9, 8),
	'retries': 2,
	'retry_delay': timedelta(minutes=10),
	'email_on_failure': False,
	'email_on_retry': False,
	'email': Variable.get('de_email', 'data-engineering@anyfin.com')
}

dag = DAG(
	'postgres_bq_backfill',
	default_args=default_args,
	catchup=False,
	schedule_interval='0 2 * * SUN',
	max_active_runs=1,
	concurrency=3
)

for DATABASE_NAME, INSTANCE_NAME in zip(DATABASE_NAMES, INSTANCE_NAMES):
	
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
			"staging_location": f'gs://{GCS_BUCKET}/Staging/',
			"runner": "DataFlowRunner",
			"experiment": "use_beam_bq_sink",
			"date": '{{ds}}',
			"backfill": 'true',
			"machine_type": "n1-standard-4",
			"setup_file": SETUP_FILE,
			"temp_location": f'gs://{GCS_BUCKET}/Temp/',
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