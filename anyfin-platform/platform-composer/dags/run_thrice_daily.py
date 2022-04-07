from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta, date
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from airflow.utils.state import State
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

slack_connection = 'slack_data_engineering'

UTILS_DIR = '/home/airflow/gcs/dags/utils/'
DBT_HOME_DIR = '/home/airflow/gcs/dags/anyfin-data-model/'
DBT_COVERAGE_TABLE = 'metadata.dbt_coverage_metadata'
PROJECT_NAME = 'anyfin'

today_partition = date.today().strftime('%Y%m%d')

default_args = {
    'owner': 'de-anyfin',
    'depends_on_past': False,
    'start_date': datetime(2022, 4, 1),
    'retries': 0,  # Do not retry as this will rerun all dbt models and ETLs again
    'retry_delay': timedelta(minutes=10),
    #'email_on_failure': True,
    #'email_on_retry': False,
    #'email': Variable.get('de_email', 'data-engineering@anyfin.com')
}

with DAG(
    dag_id='run_thrice_daily', 
    default_args=default_args, 
    catchup=False,
    description='This DAG is used to orcehstrate all of our DAGs that run thrice-daily',
    schedule_interval='0 2,6,11 * * *',
    max_active_runs=1
) as dag:


    run_ETL = TriggerDagRunOperator(
        task_id='run-postgres_bq_etl',
        trigger_dag_id='postgres_bq_etl',
        execution_date='{{ ds }}',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=[State.SUCCESS, State.FAILED],
    )

    run_dbt = TriggerDagRunOperator(
        task_id='run-bigquery_dbt_models',
        trigger_dag_id='bigquery_dbt_models',
        execution_date='{{ ds }}',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=[State.SUCCESS, State.FAILED],
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_HOME_DIR} && dbt test',
        retries=0,
    )

    compute_coverage = BashOperator(
        task_id='compute_coverage',
        bash_command=f'cd {DBT_HOME_DIR} && dbt-coverage compute test --cov-report coverage-test.json && dbt-coverage '
                     f'compute doc --cov-report coverage-doc.json && python3 {UTILS_DIR}parse_coverage.py',
    )

    load_coverage_data = GCSToBigQueryOperator(
        task_id=f'load_data',
        bucket='europe-west1-platform-b746cda0-bucket',
        schema_object='data/dbt-coverage-schema.json',
        source_objects=['data/dbt-coverage-report.csv', ],
        time_partitioning={'type': 'DAY', 'field': 'execution_date'},
        source_format='CSV',
        create_disposition='CREATE_NEVER',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        allow_jagged_rows=True,
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
        max_bad_records=10,
        destination_project_dataset_table=f'{PROJECT_NAME}.{DBT_COVERAGE_TABLE}${today_partition}',
        bigquery_conn_id='postgres-bq-etl-con',
        google_cloud_storage_conn_id='postgres-bq-etl-con',
    )

compute_coverage >> load_coverage_data
run_ETL >> run_dbt >> [dbt_test, compute_coverage]
