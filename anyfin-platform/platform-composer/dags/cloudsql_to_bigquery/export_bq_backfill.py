import os
import psycopg2
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.contrib.operators.gcp_compute_operator import GceInstanceStartOperator, GceInstanceStopOperator
from airflow.contrib.operators.gcs_delete_operator import GoogleCloudStorageDeleteOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator

from cloudsql_to_bigquery.utils.backfill_utils import BACKFILL


PROJECT_NAME = 'anyfin'
GCS_BUCKET = 'sql-to-bq-etl'

DATABASES_INFO = [
					{'DATABASE_NAME': 'main', 'INSTANCE_NAME': 'anyfin-main-replica', 'DATABASE':'main'},
					{'DATABASE_NAME': 'dolph', 'INSTANCE_NAME': 'anyfin-dolph-read-replica', 'DATABASE':'postgres'},
					{'DATABASE_NAME': 'pfm', 'INSTANCE_NAME': 'pfm-replica', 'DATABASE':'postgres'},
					{'DATABASE_NAME': 'psd2', 'INSTANCE_NAME': 'psd2-replica', 'DATABASE':'postgres'},
					{'DATABASE_NAME': 'sendout', 'INSTANCE_NAME': 'sendout-replica', 'DATABASE':'postgres'},
					{'DATABASE_NAME': 'savings', 'INSTANCE_NAME': 'savings-replica', 'DATABASE':'postgres'},
				 ]

DAG_PATH = os.path.dirname(os.path.realpath(__file__))

default_args = {
	'owner': 'ds-anyfin',
	'depends_on_past': False,
	'start_date': datetime(2020, 9, 8),
	'retries': 2,
	'retry_delay': timedelta(minutes=10),
	'email_on_failure': True,
	'email_on_retry': False,
	'email': Variable.get('de_email', 'data-engineering@anyfin.com')
}

dag = DAG(
	f'export_postgres_bq_backfill',
	default_args=default_args,
	catchup=False,
	schedule_interval='0 13 * * SUN',
	max_active_runs=1,
	concurrency=2
)


gce_instance_start_task = GceInstanceStartOperator(
	project_id='anyfin-platform',
	zone='europe-west1-b',
	resource_id='postgres-bq-backfill',
	task_id='gcp_compute_start',
	trigger_rule=TriggerRule.ONE_SUCCESS,
	dag=dag
)

upload_convertion_script_to_instance_task = BashOperator(
	task_id='upload_convertion_script_to_instance',
	bash_command=f'gcloud compute scp --zone "europe-west1-b" --project "anyfin-platform" \
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



for DB in DATABASES_INFO:
	DATABASE_NAME = DB['DATABASE_NAME']
	INSTANCE_NAME = DB['INSTANCE_NAME']
	DATABASE = DB['DATABASE']
	backfill = BACKFILL(GCS_BUCKET=GCS_BUCKET, DATABASE_NAME=DATABASE_NAME)
	sanity_check_tables = [table for table, content in backfill.get_export_tables() if not ('ignore_sanity_check' in content and content['ignore_sanity_check'] == True)]
	daily_tables = [table for table, content in backfill.get_export_tables() if not ('ignore_daily' in content and content['ignore_daily'] == True)]

	if backfill.get_nested_export_tables():
		# Task to upload schema to compute instance
		task_upload_schema_to_instance = BashOperator(
			task_id=f'upload_{DATABASE_NAME}_schema_to_instance',
			bash_command=f'gcloud compute scp --zone "europe-west1-b" --project "anyfin-platform" \
						  "{DAG_PATH}/pg_schemas/{DATABASE_NAME}_schemas_state.json" \
						  postgres-bq-backfill:/home/airflow/ ',
			dag=dag
		)
	
	if sanity_check_tables:
		postgres_check = PythonOperator(
			task_id=f'{DATABASE_NAME}_sanity_check_postgres',
			python_callable=backfill.fetch_num_of_rows_postgres,
			dag=dag
		)

	split_tasks = []

	for table_name, content in backfill.get_beam_export_tables():
		raw = '_raw'
		backup = '_backup'
		beam_backfill_job = DataflowTemplateOperator(
			task_id=f"postgres-beam-backfill-{table_name}",
			template=f"gs://sql-to-bq-etl/beam_templates/postgres-backfill-{DATABASE_NAME}-{table_name}",
			dataflow_default_options= {
				'project': 'anyfin',
				'region': 'europe-west1',
				'numWorkers': '4',
				'maxWorkers': '4',
				'machineType': 'n1-standard-2'
			},
			parameters={
				"destinationTable": f"anyfin:{DATABASE_NAME}_staging.{table_name}{raw}{backup}",
				"currentDate": datetime.today().strftime('%Y-%m-%d')
			},
			gcp_conn_id='postgres-bq-etl-con',
			region='europe-west1',
			dag=dag,
		)

		check_against_prod = BigQueryCheckOperator(
			task_id=f'check_{DATABASE_NAME}_{table_name}_against_prod',
			sql=f'''
			with
			backup as (SELECT DATE(created_at) dt, count( *) cnt FROM `anyfin.{DATABASE_NAME}_staging.{table_name}{raw}{backup}` group by 1),
			prod as (SELECT DATE(created_at) dt, count(*) cnt FROM `anyfin.{DATABASE_NAME}.{table_name}` group by 1),
			final_check as (select  p.dt, p.cnt - b.cnt diff from prod p left join backup b on p.dt=b.dt)
			select count(*)=0 from final_check where (diff<>0 or diff is null) and dt < CURRENT_DATE()
			''',
			dag=dag
		)

		load_from_backup = BigQueryToBigQueryOperator(
			task_id=f'load_{DATABASE_NAME}_{table_name}_from_backup',
			create_disposition='CREATE_NEVER',
			write_disposition='WRITE_TRUNCATE',
			source_project_dataset_tables=f'{DATABASE_NAME}_staging.{table_name}{raw}{backup}',
			destination_project_dataset_table=f'{DATABASE_NAME}_staging.{table_name}{raw}',
			bigquery_conn_id='postgres-bq-etl-con',
			dag=dag
		)

		beam_backfill_job >> check_against_prod >> load_from_backup


	# Iterate over all tables to backfill and create tasks
	for table_name, content in backfill.get_export_tables():
		nested = True if content['backfill_method'] == 'nested_export' else False

		# Delete old exported table (in the future change to DELETE IF EXISTS)
		task_delete_old_export = GoogleCloudStorageDeleteOperator(
			task_id=f'delete_old_{DATABASE_NAME}_{table_name}_export',
			bucket_name=GCS_BUCKET,
			objects=[f'pg_dumps/{DATABASE_NAME}_{table_name}_export.csv'],
			google_cloud_storage_conn_id='postgres-bq-etl-con',
			dag=dag
		)

		if nested:
			task_delete_old_json_extract = GoogleCloudStorageDeleteOperator(
				task_id=f'delete_old_json_{DATABASE_NAME}_{table_name}_extract',
				bucket_name=GCS_BUCKET,
				prefix=f'json_extracts/{DATABASE_NAME}/{table_name}/export-',
				google_cloud_storage_conn_id='postgres-bq-etl-con',
				dag=dag
			)

		# Export table
		columns = ", ".join(content['schema'].keys())
		task_export_table = BashOperator(
			task_id=f'export_{DATABASE_NAME}_{table_name}',
			bash_command=f"gcloud sql export csv  {INSTANCE_NAME} --project={PROJECT_NAME} --billing-project={PROJECT_NAME} " # add --log-http  for debugging
						f"--offload --async gs://{GCS_BUCKET}/pg_dumps/{DATABASE_NAME}_{table_name}_export.csv "
						f"--database={DATABASE} --query='select {columns}, now() as _ingested_ts from {table_name};'"
						" && "
						"sleep 30"
						" && "
						f"operation_id=$(gcloud beta sql operations --project={PROJECT_NAME} list --instance={INSTANCE_NAME} "
						"| grep EXPORT | grep RUNNING | awk '{print $1}'); "
						"if [ -z '$operation_id' ]; "
						"then echo ""; "
						f"else gcloud beta sql operations wait --project {PROJECT_NAME} $operation_id --timeout=28800; "
						"fi;",
			pool=f'{DATABASE_NAME}_export_tasks',
			dag=dag
		)

		# Generate BQ schema used for loading data into BQ
		task_generate_schema_object = PythonOperator(
			task_id=f'generate_schema_object_{DATABASE_NAME}_{table_name}',
			python_callable=backfill.generate_schema,
			op_kwargs={"name": table_name, "content": content},
			xcom_push=False,
			dag=dag
		)

		if 'bq_partition_column' in content:
			PARTITION = {'type': 'DAY', 'field': content['bq_partition_column']}
		else:
			PARTITION = {}

		if nested:
			# Convert exported CSV to JSON
			submit_python_split_task = BashOperator(
				task_id=f'csv_to_json_{DATABASE_NAME}_{table_name}',
				bash_command=f'gcloud beta compute ssh --zone "europe-west1-b" "postgres-bq-backfill" --project "anyfin-platform" -- \
						sudo python3 /home/airflow/convert_csv_to_json.py --table_name={table_name} --chunk_size={content.get("chunksize")} \
						--database_name={DATABASE_NAME} --bucket_name={GCS_BUCKET}',
				dag=dag
			)
			split_tasks.append(submit_python_split_task)

			SOURCE_OBJECT = f'json_extracts/{DATABASE_NAME}/{table_name}/export-*.json'
			SOURCE_FORMAT = 'NEWLINE_DELIMITED_JSON'
		else:
			SOURCE_OBJECT = f'pg_dumps/{DATABASE_NAME}_{table_name}_export.csv'
			SOURCE_FORMAT = 'CSV'

		SCHEMA_OBJECT = f'pg_dumps/{DATABASE_NAME}_{table_name}_schema.json'

		daily_load = True if table_name in daily_tables else False
		sanity_check = True if table_name in sanity_check_tables else False

		staging = '_staging' if daily_load or sanity_check else ''
		raw = '_raw' if daily_load else ''
		backup = '_backup' if sanity_check else ''

		DESTINATION_TABLE = f'{DATABASE_NAME}{staging}.{table_name}{raw}{backup}'

		# Load export CSV/JSON into BQ
		bq_load_backup = GoogleCloudStorageToBigQueryOperator(
			task_id=f'backup_load_{DATABASE_NAME}_{table_name}_into_bq',
			bucket=GCS_BUCKET,
			schema_object=SCHEMA_OBJECT,
			source_objects=[SOURCE_OBJECT, ],
			time_partitioning=PARTITION,
			source_format=SOURCE_FORMAT,
			create_disposition='CREATE_NEVER',
			write_disposition='WRITE_TRUNCATE',
			allow_jagged_rows=True,
			allow_quoted_newlines=True,
			ignore_unknown_values=True,
			max_bad_records=10,
			destination_project_dataset_table=DESTINATION_TABLE,
			bigquery_conn_id='postgres-bq-etl-con',
			google_cloud_storage_conn_id='postgres-bq-etl-con',
			dag=dag
		)

		if sanity_check:
			sanity_check_bq = PythonOperator(
				task_id=f"check_{DATABASE_NAME}_{table_name}_against_postgres",
				python_callable=backfill.fetch_bigquery_data,
				op_kwargs = {'table_name': table_name, 'destination_table': DESTINATION_TABLE},
				provide_context=True,
				dag=dag
			)
			if daily_load: # If daily load - ETL dedup will transfer from staging to prod
				DESTINATION_TABLE = f'{DATABASE_NAME}{staging}.{table_name}{raw}'
			else:
				DESTINATION_TABLE = f'{DATABASE_NAME}.{table_name}' # Transfer directly to prod since there is no dedup here

			bq_load_final = BigQueryToBigQueryOperator(
				task_id=f'final_load_{DATABASE_NAME}_{table_name}_into_bq',
				create_disposition='CREATE_NEVER',
				write_disposition='WRITE_TRUNCATE',
				source_project_dataset_tables=f'{DATABASE_NAME}{staging}.{table_name}{raw}{backup}',
				destination_project_dataset_table=DESTINATION_TABLE,
				bigquery_conn_id='postgres-bq-etl-con',
				dag=dag
			)


		# Create dependencies
		task_delete_old_export >> task_export_table >> task_generate_schema_object >> bq_load_backup
		if table_name in sanity_check_tables:
			postgres_check >> sanity_check_bq
			bq_load_backup >> sanity_check_bq >> bq_load_final
		if nested:
			task_delete_old_json_extract >> task_export_table
			task_export_table >> submit_python_split_task >> bq_load_backup
			task_export_table >> gce_instance_start_task
			upload_convertion_script_to_instance_task >> task_upload_schema_to_instance >> submit_python_split_task >> gce_instance_stop_task
			

gce_instance_start_task >> upload_convertion_script_to_instance_task