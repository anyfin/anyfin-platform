import os
import json
from datetime import datetime, timedelta, date
import tempfile
import logging
from google.cloud import bigquery
from airflow import DAG, AirflowException
from airflow.models import Variable
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from DataFlowPython3Operator import DataFlowPython3Operator

PROJECT_NAME = 'anyfin'

GCS_BUCKET = 'sql-to-bq-etl'

TEMPLATE_FILE = os.path.dirname(os.path.realpath(__file__)) + '/beam_utils/main_etl.py'
SETUP_FILE = os.path.dirname(os.path.realpath(__file__)) + '/beam_utils/setup.py'
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
    'main_postgres_bq_etl',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 2,9,11 * * *',
    max_active_runs=1,
    concurrency=3
)


def deduplication_success(**context):
    return True

# Extract a list of tables seperated by if they have been updated or not
# Return: Dictionary with extractable tables and tables containing unknown columns


def extract_tables(bucket_json_schemata=TABLES):
    # DB connection
    con = PostgresHook(postgres_conn_id='main_replica').get_conn()
    cur = con.cursor()

    # Fetch current state of tables
    schema_query = '''
        SELECT
           table_name,
           json_agg(concat(column_name::text, ':', data_type::text)) as columns
        FROM
           information_schema.columns
        WHERE
           table_name in {tables}
        GROUP BY table_name
        ORDER BY 1;
    '''.format(tables="('{}')".format("','".join([t for t in bucket_json_schemata])))
    cur.execute(schema_query)
    db_state_schemata = cur.fetchall()

    # Close DB connection
    cur.close()
    con.close()

    missing_columns = {}

    for (db_table, db_columns) in db_state_schemata:
        bucket_table_columns = bucket_json_schemata.get(db_table).get('schema')

        db_column_names = [col.split(':')[0] for col in db_columns]
        bucket_column_names = bucket_table_columns.keys()

        discrepency = list(set(db_column_names) - set(bucket_column_names)) + \
            list(set(bucket_column_names) - set(db_column_names))

        if not discrepency:
            continue
        else:
            missing_columns[db_table] = discrepency

    tables_to_extract = [
        t for t in bucket_json_schemata if t not in list(missing_columns)]

    all_tables = {}
    all_tables['tables_to_extract'] = tables_to_extract  # List of tables
    # Dict with table: column
    all_tables['missing_columns'] = missing_columns

    return all_tables


def no_missing_columns(**context):

    # Fetches task instance from context and pulls the variable from xcom
    missing_columns = context['ti'].xcom_pull(
        task_ids='extract_tables')['missing_columns']

    # Check if dictionary is empty
    if not(missing_columns):
        return True
    else:
        raise ValueError(
            'These columns are either missing or they have changed type: ', str(missing_columns))


def upload_table_names(**context):
    # Fetches task instance from context and pulls the variable from xcom
    dict_tables = context['ti'].xcom_pull(task_ids='extract_tables')
    json_tables = json.dumps(dict_tables)

    # Connect to GCS
    gcs_hook = GoogleCloudStorageHook(
        google_cloud_storage_conn_id='postgres-bq-etl-con')

    # Create a temporary JSON file and upload it to GCS
    with tempfile.NamedTemporaryFile(mode="w") as file:
        file.write(json_tables)
        file.flush()
        gcs_hook.upload(
            bucket=GCS_BUCKET,
            object='main_table_info.json',
            mime_type='application/json',
            filename=file.name
        )


task_extract_tables = PythonOperator(
    task_id='extract_tables',
    python_callable=extract_tables,
    xcom_push=True,
    dag=dag
)

task_no_missing_columns = PythonOperator(
    task_id='no_missing_columns',
    python_callable=no_missing_columns,
    provide_context=True,
    retries=0,
    dag=dag
)

task_upload_result_to_gcs = PythonOperator(
    task_id='upload_result_to_gcs',
    python_callable=upload_table_names,
    provide_context=True,
    retries=2,
    dag=dag
)

extract_from_cloudsql = DataFlowPython3Operator(
    task_id='extract_from_cloudsql',
    py_file=TEMPLATE_FILE,
    job_name='main-etl',
    provide_context=True,
    dataflow_default_options={
        "project": PROJECT_NAME,
        "worker_zone": 'europe-west1-b',
        "region": "europe-west1",
        "staging_location": f'gs://{GCS_BUCKET}/Staging/',
        "runner": "DataFlowRunner",
        "experiment": "use_beam_bq_sink",
        "date": '{{ds}}',
        "machine_type": "n1-standard-4",
        "setup_file": SETUP_FILE,
        "temp_location": f'gs://{GCS_BUCKET}/Temp/'
    },
    delegate_to="postgres-bq-etl@anyfin.iam.gserviceaccount.com",
    options={
        "num-workers": '1'
    },
    gcp_conn_id='postgres-bq-etl-con',
    poll_sleep=30,
    dag=dag
)

def check_if_first_daily_run(ds, **kwargs):
    today = date.today()
    today = today.strftime("%Y-%m-%d")
    if today != ds:
        return 'postgres_status'
    else:
        return 'no_check'


first_daily_run = BranchPythonOperator(
    task_id='first_daily_run',
    provide_context=True,
    python_callable=check_if_first_daily_run,
    dag=dag
)

def fetch_postgres_rowcount(tables, ds, **kwargs):
    db = PostgresHook('main_replica')
    query = []
    for table_name in tables:
        query.append(f"SELECT '{table_name}', COUNT(id) FROM {table_name} where created_at::date = '{ds}'")
    query = ' UNION ALL '.join(query)
    
    rowcounts = db.get_records(query)
    counts = {}
    for row in rowcounts:
        counts[row[0]] = row[1]
    return counts

postgres_status = PythonOperator(
    task_id='postgres_status',
    provide_context=True,
    op_kwargs={'tables': TABLES},
    python_callable=fetch_postgres_rowcount,
    dag=dag
)

def fetch_bigquery_rowcount(tables, ds, **kwargs):
    query = []
    for table_name in tables:
        query.append(f"SELECT '{table_name}' table_name, COUNT(DISTINCT id) num_of_unique_rows FROM `anyfin.main_staging.{table_name}_raw` where DATE(created_at) = '{ds}'")
    query = ' UNION ALL '.join(query)
    client = bigquery.Client()
    query_job = client.query(query)
    results = query_job.result()
    counts = {}
    for row in results:
        counts[row.table_name] = row.num_of_unique_rows
    return counts

bq_status = PythonOperator(
    task_id='bq_status',
    provide_context=True,
    op_kwargs={'tables': TABLES},
    python_callable=fetch_bigquery_rowcount,
    dag=dag
)

def bq_pg_comparison(tables, **kwargs):
    postgres_results = kwargs['ti'].xcom_pull(task_ids='postgres_status')
    bq_results = kwargs['ti'].xcom_pull(task_ids='bq_status')

    discrepancies = {}

    for table_name in tables:
        logging.info(f"Table: {table_name}: Postgres - {postgres_results[table_name]} || BQ - {bq_results[table_name]}")
        if postgres_results[table_name] != bq_results[table_name]:
            discrepancies[table_name] = {'postgres': postgres_results[table_name], 'bq': bq_results[table_name]}
    
    if not discrepancies:
        return True
    else:
        raise AirflowException(f"Discrepancies found in tables: {' ,'.join(discrepancies.keys())}")

check_postgres_against_bq = PythonOperator(
    task_id='check_postgres_against_bq',
    provide_context=True,
    op_kwargs={'tables': TABLES},
    python_callable=bq_pg_comparison,
    email_on_failure=True,
    dag=dag
)

no_check = DummyOperator(
    task_id='no_check',
    dag=dag
)

dedup_tasks = []
for table in TABLES:
    if table == 'assessments':
        dedup = BigQueryOperator(
            task_id='deduplicate_' + table,
            sql=f"""
                with temp as (
                    SELECT 
                        id, 
                        max(_ingested_ts) as max_ingested_ts 
                    FROM anyfin.main_staging.assessments_raw group by 1
                )
                SELECT 
                    t.*,
                    json_extract(main_policy,  '$.data.Rdm' )  as `rdm`,
                    json_extract(main_policy,  '$.data.Kalp' )  as `kalp`,
                    json_extract(main_policy,  '$.data.Limit' )  as `limit`,
                    json_extract(main_policy,  '$.data.Pricing' )  as `pricing`,
                    json_extract(main_policy,  '$.data.Request' )  as `request_schema`,
                    json_extract(main_policy,  '$.data.request' )  as `request`,
                    json_extract(main_policy,  '$.data.Capacity' )  as `capacity`,
                    json_extract(main_policy,  '$.data.Customer' )  as `customer`,
                    json_extract(main_policy,  '$.data.Response' )  as `response_schema`,
                    json_extract(main_policy,  '$.data.UCLookup' )  as `uc_lookup`,
                    json_extract(main_policy,  '$.data.response' )  as `response`,
                    json_extract(main_policy,  '$.data.PepScreen' )  as `pep_screen`,
                    json_extract(main_policy,  '$.data.Rejection' )  as `rejection`,
                    json_extract(main_policy,  '$.data.decisions' )  as `decisions`,
                    json_extract(main_policy,  '$.data.ReverseLookup' )  as `reverselookup`,
                    json_extract(main_policy,  '$.data.CurrentPricing' )  as `current_pricing`,
                    json_extract(main_policy,  '$.data.InternalLookup' )  as `internal_lookup`,
                    json_extract(main_policy,  '$.data.SanctionScreen' )  as `sanction_screen`,
                    json_extract(main_policy,  '$.data.CustomerMatcher' )  as `customer_matcher`,
                    json_extract(main_policy,  '$.data.SuggestedPricing' )  as `suggested_pricing`,
                    json_extract(main_policy,  '$.data.NewCustomerPolicy' )  as `new_customer_policy`,
                    json_extract(main_policy,  '$.data.NewCustomerScoring' )  as `new_customer_scoring`,
                    json_extract(main_policy,  '$.data.ReturningCustomerScoring' )  as `returning_customer_scoring`,
                    json_extract(main_policy,  '$.data.ReturningCustomerPolicy' )  as `returning_customer_policy`,
                    json_extract(main_policy,  '$.data.PrimaryCreditCriteria' )  as `primary_credit_criteria`,
                    json_extract(main_policy,  '$.data.labelling_reject_reasons' )  as `labelling_reject_reasons`,
                    json_extract(main_policy,  '$.data.labelling_warning_reasons' )  as `labelling_warning_reasons`,
                    json_extract(main_policy,  '$.data.labelling_auto_reject_reasons' )  as `labelling_auto_reject_reasons`,
                    json_extract(main_policy,  '$.data.AsiakastietoLookup' )  as `asiakastieto_lookup`,
                    json_extract(main_policy,  '$.data.AsiakastietoCcisLookup' )  as `asiakastieto_ccis_lookup`,
                    json_extract(main_policy,  '$.data.KalpNew' )  as `kalp_new`
                    from temp join 
                    anyfin.main_staging.{table}_raw t on temp.id= t.id and temp.max_ingested_ts=t._ingested_ts
            """,
            destination_dataset_table=f"anyfin.main.{table}",
            cluster_fields=['id'],
            time_partitioning={'field': 'created_at'},
            use_legacy_sql=False,
            write_disposition='WRITE_TRUNCATE',
            bigquery_conn_id='bigquery_default',
            create_disposition='CREATE_IF_NEEDED',
            dag=dag
        )
        dedup_tasks.append(dedup)        

    else: 
        dedup = BigQueryOperator(    
            task_id='deduplicate_' + table,
            sql=f"""
                with temp as (
                    select 
                        id, 
                        max(_ingested_ts) as max_ingested_ts 
                    from anyfin.main_staging.{table}_raw group by 1
                )
                select 
                    t.* 
                from temp join 
                    anyfin.main_staging.{table}_raw t on temp.id= t.id and temp.max_ingested_ts=t._ingested_ts""",
            destination_dataset_table=f"anyfin.main.{table}",
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
    python_callable=deduplication_success,
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
