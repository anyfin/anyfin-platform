"""
Should be deployed under the composer's dags directory, e.g. gs://europe-west1-production-com-369e4b80-bucket/dags
"""

import os
import json
from datetime import datetime, timedelta
import tempfile
import re
from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.postgres_hook import PostgresHook
from cloudsql_to_bigquery.utils.cloud_json_loader import cloud_json_loader
from DataFlowPython3Operator import DataFlowPython3Operator

from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.bash_operator import BashOperator


PROJECT_NAME = 'anyfin-platform'
INSTANCE_NAME = 'anyfin-main-replica'
BUCKET_NAME = 'sql-bq-etl'
TEMPLATE_FILE = os.path.dirname(os.path.realpath(__file__)) + '/beam_utils/main_etl.py'
SETUP_FILE = os.path.dirname(os.path.realpath(__file__)) + '/beam_utils/setup.py'
tables_to_backfill = []


with open(os.path.join(os.path.dirname(__file__), 'pg_schemas/main_schemas_state.json')) as f:
    TABLES = json.loads(f.read())


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
	'main_postgres_bq_backfill',
	default_args=default_args,
	catchup=False,
	schedule_interval='0 2 * * SUN',
	max_active_runs=1,
	concurrency=3
)


tables_to_backfill = [(table_name, content) for table_name, content in TABLES.items() if 'backfill_method' in content and content['backfill_method'] == 'export']


def convert_type_to_bq(t):
	if t == 'timestamp with time zone':
		return 'TIMESTAMP'

	elif t == 'date':
		return 'DATE'

	elif t == 'numeric':
		return 'FLOAT'

	elif t == 'integer':
		return 'INTEGER'

	elif t == 'boolean':
		return 'BOOLEAN'

	else: 
		return 'STRING'

def generate_schema(**kwargs):
	schema = []
	for column, t in kwargs['content']['schema'].items():
		if column.startswith("\"") and column.endswith("\""):
			column = re.findall('"([^"]*)"', column)[0]
		if t == 'ARRAY':
			mode = 'REPEATED'
			t = 'STRING'
		else:
			mode = 'NULLABLE'
		schema.append( {"mode": mode, "name": column, "type": convert_type_to_bq(t)} )
	schema.append( {"mode": "NULLABLE", "name": "_ingested_ts", "type": "TIMESTAMP"} )
	
	# Connect to GCS
	gcs_hook = GoogleCloudStorageHook(
		google_cloud_storage_conn_id='postgres-bq-etl-con')

	filename = 'pg_dumps/main_' + kwargs['name'] + '_schema.json'		

	# Create a temporary JSON file and upload it to GCS
	with tempfile.NamedTemporaryFile(mode="w") as file:
		json.dump(schema, file)
		file.flush()
		gcs_hook.upload(
			bucket=BUCKET_NAME,
			object=filename,
			mime_type='application/json',
			filename=file.name
		)

gen_schema_tasks = []
for table_name, content in tables_to_backfill:
	task_generate_schema_objects = PythonOperator(
		task_id='generate_schema_object_' + table_name,
		python_callable=generate_schema,
		op_kwargs={"name": table_name, "content": content},
		xcom_push=False,
		dag=dag
	)
	gen_schema_tasks.append(task_generate_schema_objects)


export_tasks = []
wait_tasks = []
for table_name, content in tables_to_backfill:
	columns = ", ".join(content['schema'].keys())

	task_export_table = BashOperator(
		task_id='export_' + table_name,
		bash_command="gcloud sql export csv anyfin-main-replica --project=anyfin "
					f"--offload --async gs://{BUCKET_NAME}/pg_dumps/main_{table_name}.csv "
					f"--database=main --query='select {columns}, now() as _ingested_ts from {table_name} limit 100;'",
		dag=dag
	)
	export_tasks.append(task_export_table)

	task_wait_operation = BashOperator(
		task_id='wait_operation_' + table_name,
		bash_command="""
			operation_id=$(gcloud beta sql operations --project=anyfin list --instance=anyfin-main-replica | grep EXPORT | grep RUNNING | awk '{print $1}'); 
			if [ -z "$operation_id" ]; 
			then echo ""; 
			else gcloud beta sql operations wait --project anyfin $operation_id --timeout=3600; 
			fi;""",
		dag=dag
	)
	wait_tasks.append(task_wait_operation)

bq_load_tasks = []
for table_name, content in tables_to_backfill:
	SCHEMA_OBJECT = 'pg_dumps/main_' + table_name + '_schema.json'
	DESTINATION_TABLE = 'main_staging.' + table_name + '_raw'
	SOURCE_OBJECT = 'pg_dumps/main_' + table_name + '.csv'

	if 'bq_partition_column' in content:
		PARTITION = {'type': 'DAY', 'field': content['bq_partition_column']}
	else:
		PARTITION = {}

	task_bq_load = GoogleCloudStorageToBigQueryOperator(
		task_id='load_' + table_name + '_into_bq',
		bucket=BUCKET_NAME,
		schema_object=SCHEMA_OBJECT,
		source_objects=[SOURCE_OBJECT, ],
		time_partitioning=PARTITION,
		source_format='CSV',
		create_disposition='CREATE_NEVER',
		write_disposition='WRITE_TRUNCATE',
		allow_jagged_rows=True,
		ignore_unknown_values=True,
		max_bad_records=10,
		destination_project_dataset_table=DESTINATION_TABLE,
		bigquery_conn_id='postgres-bq-etl-con',
		google_cloud_storage_conn_id='postgres-bq-etl-con',
		dag=dag
	)
	bq_load_tasks.append(task_bq_load)


beam_backfill = DataFlowPython3Operator(
	task_id='beam_backfill',
	py_file=TEMPLATE_FILE,
	job_name='main-etl-backfill',
	provide_context=True,
	dataflow_default_options={
		"project": 'anyfin',
		"worker_zone": 'europe-west1-b',
		"region": "europe-west1",
		"staging_location": f'gs://{BUCKET_NAME}/Staging/',
		"runner": "DataFlowRunner",
		"experiment": "use_beam_bq_sink",
		"date": '{{ds}}',
		"backfill": 'true',
		"machine_type": "n1-standard-4",
		"setup_file": SETUP_FILE,
		"temp_location": f'gs://{BUCKET_NAME}/Temp/'
	},
	delegate_to="postgres-bq-etl@anyfin.iam.gserviceaccount.com",
	options={
		"num-workers": '1'
	},
	gcp_conn_id='postgres-bq-etl-con',
	poll_sleep=30,
	dag=dag
)



for i in range(1,len(tables_to_backfill)):
	gen_schema_tasks[i-1] >> export_tasks[i-1] >> wait_tasks[i-1] >> bq_load_tasks[i-1]
	wait_tasks[i-1] >> export_tasks[i]

gen_schema_tasks[-1] >> export_tasks[-1] >> wait_tasks[-1] >> bq_load_tasks[-1]