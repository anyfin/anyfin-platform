from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator

from cloudsql_to_bigquery.utils.etl_utils import ETL

PROJECT_NAME = 'anyfin'
GCS_BUCKET = 'sql-to-bq-etl'
DATAFLOW_BUCKET = 'etl-dataflow-bucket'
DATABASE_NAME = 'assess'

ETL = ETL(GCS_BUCKET=GCS_BUCKET, DATABASE_NAME=DATABASE_NAME)

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
    f'{DATABASE_NAME}_postgres_bq_etl',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 3,9,11 * * *',
    max_active_runs=1,
    concurrency=3
)

dedup_tasks = []

for table in ETL.get_tables():
    load_raw = BigQueryOperator(
        task_id='load_raw_' + table,
        sql="""
        SELECT
            {}
        FROM
        EXTERNAL_QUERY("anyfin.eu.assess-replica",
            "SELECT {} FROM lookups WHERE ts::date>='{}';");
        """.format(
            ETL.get_bq_columns(table),
            ETL.get_pg_columns(table),
            '{{ ds }}'
        ),
        destination_dataset_table=f"anyfin.{DATABASE_NAME}_staging.{table}_raw",
        use_legacy_sql=False,
        bigquery_conn_id='postgres-bq-etl-con',
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_NEVER',
        dag=dag
    )

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
        destination_dataset_table=f"anyfin.{DATABASE_NAME}_staging.{table}",
        cluster_fields=['id'],
        time_partitioning={'field': 'ts'},
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='postgres-bq-etl-con',
        create_disposition='CREATE_IF_NEEDED',
        dag=dag
    )
    load_raw >> dedup

    dedup_tasks.append(dedup)

deduplication_success_confirmation = PythonOperator(
    task_id='deduplication_success_confirmation',
    python_callable=ETL.deduplication_success,
    provide_context=True,
    email_on_failure=True,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

dedup_tasks >> deduplication_success_confirmation