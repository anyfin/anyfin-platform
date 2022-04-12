import tempfile
import requests
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from utils import slack_notification
from functools import partial

BUCKET = 'intercom-to-bq-etl'
FILE_NAME = 'intercom_messages.csv'
SCHEMA_OBJECT = 'intercom_messages_schema.json'
SLACK_CONNECTION = 'slack_data_engineering'

API_KEY = Variable.get('intercom_api_export_messages_secret')

default_args = {
	'owner': 'de-anyfin',
	'depends_on_past': False,
	'start_date': datetime(2020, 9, 8),
	'retries': 2,
	'retry_delay': timedelta(minutes=10),
	'on_failure_callback': partial(slack_notification.task_fail_slack_alert, SLACK_CONNECTION),
}

dag = DAG(
	'intercom_messages_to_bigquery',
	default_args=default_args,
	catchup=False,
	schedule_interval='0 0 * * *',
	max_active_runs=1,
	concurrency=3
)


def request_download_id(start_date):
	""" Send request for download id.

	Args:
		start_date (string): Fetches data generated after this date
	Returns: 
		download_id (int): Used to fetch download url
	"""

	headers = {
		'Authorization': f'Bearer {API_KEY}',
		'Accept': 'application/json',
		'Content-Type': 'application/json',
		'Intercom-Version': 'Unstable'
	}

	start_time = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
	current_timestamp = int(time.time())
	data = f'{{"created_at_after": {start_time},"created_at_before": {current_timestamp}}}'

	response = requests.post('https://api.intercom.io/export/messages/data', headers=headers, data=data)
	download_id = response.json()['job_identifier']

	return download_id

def get_dowload_link(**context):
	""" Use dowload id to get the actual download url that will be used to start downloading the data.

	This function will continuously ping intercom every minute while the download url is being prepared.
	When status is complete the loop ends and the download url is returned.

	Args:
		**context: Fetches the download id from the context of the previous task
	Returns: 
		Pushes download url to context for next task
	"""

	download_id = context['ti'].xcom_pull(task_ids='request_download_id')
	pending = True
	attempt = 1
	while(pending):
		# Sleep
		attempt += 1
		time.sleep(60)

		# Check status
		headers = {
			'Authorization': f'Bearer {API_KEY}',
			'Accept': 'application/json',
			'Intercom-Version': 'Unstable'
		}

		response = requests.get('https://api.intercom.io/export/messages/data/'+download_id, headers=headers)
		response = response.json()
		print(response)
		if response['status'] == 'complete':
			download_url = response['download_url']
			pending = False
			return download_url
		if attempt > 120:
			raise Exception("The request timed out! Job status never became complete after 60*120seconds so raising this error") 

def download(**context): 
	""" Send octet-stream request to a download_url to start downloading the data.

	This function will download data from specified url, store it as a temp file and upload it to GCS

	Args:
		**context: Fetches the download url from the context of the previous task
	Returns: 
		Nothing
	"""

	download_url = context['ti'].xcom_pull(task_ids='get_download_url')
	headers = {
			'Authorization': f'Bearer {API_KEY}',
			'Accept': 'application/octet-stream',
			'Intercom-Version': 'Unstable'
	}

	response = requests.get(download_url, headers=headers, stream=True)
	
	# Connect to GCS
	gcs_hook = GCSHook(gcp_conn_id='bigquery-composer-connection')

	# Write file response to temp file and upload it to GCS
	with tempfile.NamedTemporaryFile(mode="wb", suffix='.csv') as f:
		for chunk in response.iter_content(chunk_size=128):
			if chunk: # filter out keep-alive new chunks
				f.write(chunk)
		f.flush()
		gcs_hook.upload(
				bucket_name=BUCKET,
				object_name=FILE_NAME,
				mime_type='application/csv',
				filename=f.name
			)


task_request_download_id = PythonOperator(
	task_id='request_download_id',
	python_callable=request_download_id,
	op_kwargs={'start_date': '2021-09-01'}, # Should be 2021-09-01 for all data
	do_xcom_push=True,
	dag=dag
)

task_get_download_url = PythonOperator(
	task_id='get_download_url',
	python_callable=get_dowload_link,
	provide_context=True,
	do_xcom_push=True,
	dag=dag
)

task_download_and_upload_to_gcs = PythonOperator(
	task_id='download_and_upload_to_gcs',
	python_callable=download,
	provide_context=True,
	do_xcom_push=True,
	dag=dag
)

task_upload_from_gcs_to_bigquery = GCSToBigQueryOperator(
			task_id='upload_from_gcs_to_bigquery',
			bucket=BUCKET,
			schema_object='intercom_messages_schema.json',
			source_objects=[FILE_NAME, ],
			#time_partitioning=PARTITION,
			source_format='CSV',
			create_disposition='CREATE_IF_NEEDED',
			write_disposition='WRITE_TRUNCATE',
			allow_jagged_rows=True,
			allow_quoted_newlines=True,
			ignore_unknown_values=True,
			max_bad_records=5,
			destination_project_dataset_table='anyfin.product.intercom_message_export',
			bigquery_conn_id='bigquery-composer-connection',
			google_cloud_storage_conn_id='bigquery-composer-connection',
			dag=dag
		)

task_request_download_id >> task_get_download_url >> task_download_and_upload_to_gcs >> task_upload_from_gcs_to_bigquery
