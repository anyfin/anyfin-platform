from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import State


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
    #schedule_interval='0 2,9,11 * * *',
    schedule_interval=None,
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

run_ETL >> run_dbt