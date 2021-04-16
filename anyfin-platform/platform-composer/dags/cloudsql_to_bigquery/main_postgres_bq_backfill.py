"""
Should be deployed under the composer's dags directory, e.g. gs://europe-west1-production-com-369e4b80-bucket/dags
"""

import os
import json
from datetime import datetime, timedelta
import tempfile
import re
import time
import pandas as pd
import logging
import paramiko
import psycopg2
from google.cloud import storage, bigquery
from airflow import DAG, AirflowException
from airflow.models import Variable
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator,BigQueryValueCheckOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.postgres_hook import PostgresHook
from cloudsql_to_bigquery.utils.cloud_json_loader import cloud_json_loader
from DataFlowPython3Operator import DataFlowPython3Operator
from airflow.contrib.operators.gcp_compute_operator import GceInstanceStartOperator, GceInstanceStopOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.bash_operator import BashOperator


PROJECT_NAME = 'anyfin-platform'
INSTANCE_NAME = 'anyfin-main-replica'
BUCKET_NAME = 'sql-bq-etl'
TEMPLATE_FILE = os.path.dirname(os.path.realpath(__file__)) + '/beam_utils/main_etl.py'
SETUP_FILE = os.path.dirname(os.path.realpath(__file__)) + '/beam_utils/setup.py'
DAG_PATH = os.path.dirname(os.path.realpath(__file__))
POSTGRES_CREDENTIALS = Variable.get("main_postgres_bq_secret", deserialize_json=True)
tables_to_backfill = []


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


with open(os.path.join(os.path.dirname(__file__), 'pg_schemas/main_schemas_state.json')) as f:
    TABLES = json.loads(f.read())


tables_to_backfill = [(table_name, content) for table_name, content in TABLES.items() if 'backfill_method' in content and content['backfill_method'] == 'export']

# Operator to backfill tables with beam
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

# Function to convert postgres schema types into BQ format
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

def fetch_num_of_rows_postgres(tables_info, **kwargs):
	db = PostgresHook('main_replica')
	query = []
	for table_name, info in tables_info:
		query.append(f"SELECT '{table_name}', COUNT(*) FROM {table_name}")
	query = ' UNION ALL '.join(query)

	rowcounts = db.get_records(query)
	counts = {}
	for row in rowcounts:
		counts[row[0]] = row[1]
	return counts

# Generate BQ schema object as json file and upload to GCS
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

def fetch_bigquery_data(table_name, destination_table, **kwargs):
	postgres_results = kwargs['ti'].xcom_pull(task_ids='sanity_check_postgres')
	client = bigquery.Client()
	query_job = client.query(
    f"""
    SELECT
      COUNT(*) as num_of_rows
    FROM `anyfin.{destination_table}`"""
    )
	results = query_job.result()
	res = []
	for row in results:
		res.append(row.num_of_rows)
	logging.info(f"{postgres_results[table_name]} - Postgres rows for {table_name}")
	logging.info(f"{int(res[0])} - BQ backup rows for {table_name}")
	if postgres_results[table_name] * 0.999 <= int(res[0]):
		return True
	else:
		raise AirflowException("Number of rows in Postgres is higher than BQ")

gce_instance_start_task = GceInstanceStartOperator(
	project_id='anyfin-platform',
	zone='europe-west1-b',
	resource_id='postgres-bq-backfill',
	task_id='gcp_compute_start',
	dag=dag
)

upload_convertion_script_to_instance_task = BashOperator(
	task_id='upload_convertion_script_to_instance',
	bash_command=f'gcloud compute scp --zone "europe-west1-b" --project "anyfin-platform" \
				  "{DAG_PATH}/pg_schemas/main_schemas_state.json" \
				  "{DAG_PATH}/utils/convert_csv_to_json.py" \
			      postgres-bq-backfill:/home/airflow/ ',
	dag=dag
)

gce_instance_stop_task = GceInstanceStopOperator(
	project_id='anyfin-platform',
	zone='europe-west1-b',
	resource_id='postgres-bq-backfill',
	task_id='gcp_compute_stop',
	trigger_rule=TriggerRule.ALL_DONE,
	dag=dag
)

postgres_check = PythonOperator(
	task_id='sanity_check_postgres',
	python_callable=fetch_num_of_rows_postgres,
	op_kwargs={'tables_info': tables_to_backfill},
	dag=dag
)

split_tasks = []
wait_tasks = []
prev_wait_task = ""


# def get_pass_value(table_name, **kwargs):
# 	return kwargs['ti'].xcom_pull(task_ids='sanity_check_postgres', key=table_name) * 0.0001

# Iterate over all tables to backfill and create tasks
for table_name, content in tables_to_backfill:

	# Delete old exported table
	task_delete_old_export = GoogleCloudStorageDeleteOperator(
		task_id=f'delete_old_{table_name}_export',
		bucket_name=BUCKET_NAME,
		objects=[f'pg_dumps/main_{table_name}.csv'],
		google_cloud_storage_conn_id='postgres-bq-etl-con',
		dag=dag
	)

	task_delete_old_json_extract = GoogleCloudStorageDeleteOperator(
		task_id=f'delete_old_json_{table_name}_extract',
		bucket_name=BUCKET_NAME,
		prefix=f'json_extracts/{table_name}/export-',
		google_cloud_storage_conn_id='postgres-bq-etl-con',
		dag=dag
	)

	# Export table
	columns = ", ".join(content['schema'].keys())
	task_export_table = BashOperator(
		task_id='export_' + table_name,
		bash_command="gcloud sql export csv anyfin-main-replica --project=anyfin "
					f"--offload --async gs://{BUCKET_NAME}/pg_dumps/main_{table_name}.csv "
					f"--database=main --query='select {columns}, now() as _ingested_ts from {table_name};'",
		dag=dag
	)

	# Wait for export to complete
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

	# Convert exported CSV to JSON
	submit_python_split_task = BashOperator(
    	task_id=f'csv_to_json_{table_name}',
 	   	bash_command=f'gcloud beta compute ssh --zone "europe-west1-b" "postgres-bq-backfill" --project "anyfin-platform" -- \
				sudo python3 /home/airflow/convert_csv_to_json.py --table_name={table_name} --chunk_size={content.get("chunksize")}',
    	dag=dag
	)

	split_tasks.append(submit_python_split_task)

	# Generate BQ schema used for loading JSON into BQ
	task_generate_schema_object = PythonOperator(
		task_id='generate_schema_object_' + table_name,
		python_callable=generate_schema,
		op_kwargs={"name": table_name, "content": content},
		xcom_push=False,
		dag=dag
	)

	# Load generated JSON files into BQ
	SCHEMA_OBJECT = 'pg_dumps/main_' + table_name + '_schema.json'
	DESTINATION_TABLE = 'main_staging.' + table_name + '_raw_backup'
	SOURCE_OBJECT = f'json_extracts/{table_name}/export-*.json'

	if 'bq_partition_column' in content:
		PARTITION = {'type': 'DAY', 'field': content['bq_partition_column']}
	else:
		PARTITION = {}
	
	bq_load_backup = GoogleCloudStorageToBigQueryOperator(
		task_id='backup_load_' + table_name + '_into_bq',
		bucket=BUCKET_NAME,
		schema_object=SCHEMA_OBJECT,
		source_objects=[SOURCE_OBJECT, ],
		time_partitioning=PARTITION,
		source_format='NEWLINE_DELIMITED_JSON',
		create_disposition='CREATE_IF_NEEDED',
		write_disposition='WRITE_TRUNCATE',
		allow_jagged_rows=True,
		ignore_unknown_values=True,
		max_bad_records=0,
		destination_project_dataset_table=DESTINATION_TABLE,
		bigquery_conn_id='postgres-bq-etl-con',
		google_cloud_storage_conn_id='postgres-bq-etl-con',
		dag=dag
	)

	sanity_check_bq = PythonOperator(
		task_id=f"check_{table_name}_against_postgres",
		python_callable=fetch_bigquery_data,
		op_kwargs = {'table_name': table_name, 'destination_table': DESTINATION_TABLE},
		provide_context=True,
		dag=dag
	)

	DESTINATION_TABLE = 'main_staging.' + table_name + '_raw'

	bq_load_final = GoogleCloudStorageToBigQueryOperator(
		task_id='final_load_' + table_name + '_into_bq',
		bucket=BUCKET_NAME,
		schema_object=SCHEMA_OBJECT,
		source_objects=[SOURCE_OBJECT, ],
		time_partitioning=PARTITION,
		source_format='NEWLINE_DELIMITED_JSON',
		create_disposition='CREATE_IF_NEEDED',
		write_disposition='WRITE_TRUNCATE',
		allow_jagged_rows=True,
		ignore_unknown_values=True,
		max_bad_records=0,
		destination_project_dataset_table=DESTINATION_TABLE,
		bigquery_conn_id='postgres-bq-etl-con',
		google_cloud_storage_conn_id='postgres-bq-etl-con',
		dag=dag
	)
	# Create dependencies

	task_delete_old_export >> task_delete_old_json_extract >> task_export_table >> task_wait_operation >> submit_python_split_task >> task_generate_schema_object >> bq_load_backup >> sanity_check_bq >> bq_load_final
	prev_wait_task >> task_export_table
	prev_wait_task = task_wait_operation

wait_tasks[0] >> gce_instance_start_task >> upload_convertion_script_to_instance_task >> split_tasks >> gce_instance_stop_task