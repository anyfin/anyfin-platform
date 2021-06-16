import os
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.models import Variable
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.operators.dummy_operator import DummyOperator
from DataFlowPython3Operator import DataFlowPython3Operator

from cloudsql_to_bigquery.utils.etl_utils import ETL

PROJECT_NAME = 'anyfin'
GCS_BUCKET = 'sql-to-bq-etl'
DATAFLOW_BUCKET = 'etl-dataflow-bucket'
DATABASE_NAME = 'pfm'

TEMPLATE_FILE = os.path.dirname(os.path.realpath(__file__)) + '/beam_utils/pg_bq_etl.py'
SETUP_FILE = os.path.dirname(os.path.realpath(__file__)) + '/beam_utils/setup.py'

ETL = ETL(GCS_BUCKET=GCS_BUCKET, DATABASE_NAME=DATABASE_NAME)

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
    f'{DATABASE_NAME}_postgres_bq_etl',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 2,9,11 * * *',
    max_active_runs=1,
    concurrency=3
)


task_extract_tables = PythonOperator(
    task_id='extract_tables',
    python_callable=ETL.extract_tables,
    xcom_push=True,
    dag=dag
)

task_no_missing_columns = PythonOperator(
    task_id='no_missing_columns',
    python_callable=ETL.no_missing_columns,
    provide_context=True,
    retries=0,
    dag=dag
)

task_upload_result_to_gcs = PythonOperator(
    task_id='upload_result_to_gcs',
    python_callable=ETL.upload_table_names,
    provide_context=True,
    retries=2,
    dag=dag
)

extract_from_cloudsql = DataFlowPython3Operator(
    task_id='extract_from_cloudsql',
    py_file=TEMPLATE_FILE,
    job_name=f'{DATABASE_NAME}-etl',
    provide_context=True,
    dataflow_default_options={
        "project": PROJECT_NAME,
        "worker_zone": 'europe-west1-b',
        "region": "europe-west1",
        "staging_location": f'gs://{DATAFLOW_BUCKET}/Staging/',
        "runner": "DataFlowRunner",
        "experiment": "use_beam_bq_sink",
        "date": '{{ds}}',
        "machine_type": "n1-standard-4",
        "setup_file": SETUP_FILE,
        "temp_location": f'gs://{DATAFLOW_BUCKET}/Temp/',
        "database_name": f"{DATABASE_NAME}"
    },
    delegate_to="postgres-bq-etl@anyfin.iam.gserviceaccount.com",
    options={
        "num-workers": '1'
    },
    email_on_failure=True,
    gcp_conn_id='postgres-bq-etl-con',
    poll_sleep=30,
    dag=dag
)

first_daily_run = BranchPythonOperator(
    task_id='first_daily_run',
    provide_context=True,
    python_callable=ETL.check_if_first_daily_run,
    dag=dag
)

postgres_status = PythonOperator(
    task_id='postgres_status',
    provide_context=True,
    python_callable=ETL.fetch_postgres_rowcount,
    dag=dag
)


bq_status = PythonOperator(
    task_id='bq_status',
    provide_context=True,
    python_callable=ETL.fetch_bigquery_rowcount,
    dag=dag
)


check_postgres_against_bq = PythonOperator(
    task_id='check_postgres_against_bq',
    provide_context=True,
    python_callable=ETL.bq_pg_comparison,
    email_on_failure=True,
    dag=dag
)

no_check = DummyOperator(
    task_id='no_check',
    dag=dag
)

dedup_tasks = []
for table in ETL.get_tables(): 
    dedup = BigQueryOperator(    
        task_id='deduplicate_' + table,
        sql=f"""
            with temp as (
                select 
                    id, 
                    max(_ingested_ts) as max_ingested_ts 
                from anyfin.{DATABASE_NAME}_staging.{table}_raw group by 1
            )
            select 
                t.* 
            from temp join 
                anyfin.{DATABASE_NAME}_staging.{table}_raw t on temp.id= t.id and temp.max_ingested_ts=t._ingested_ts""",
        destination_dataset_table=f"anyfin.{DATABASE_NAME}.{table}",
        cluster_fields=['id'],
        time_partitioning={'field': 'created_at'},
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='bigquery_default',
        create_disposition='CREATE_IF_NEEDED',
        dag=dag
    )
    dedup_tasks.append(dedup)



deduplication_success_confirmation = PythonOperator(
    task_id='deduplication_success_confirmation',
    python_callable=ETL.deduplication_success,
    provide_context=True,
    email_on_failure=True,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)


task_extract_tables >> task_no_missing_columns

task_extract_tables >> task_upload_result_to_gcs

extract_from_cloudsql >> first_daily_run

first_daily_run >> postgres_status >> bq_status >> check_postgres_against_bq
first_daily_run >> no_check

task_upload_result_to_gcs >> extract_from_cloudsql >> dedup_tasks

dedup_tasks >> deduplication_success_confirmation
