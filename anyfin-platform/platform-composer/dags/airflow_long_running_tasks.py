from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.sql import SQLValueCheckOperator

from utils import slack_notification
from functools import partial

SLACK_CONNECTION = 'slack_data_engineering'
QUERY = '''
select
    count(task_id) as num_of_tasks
from task_instance 
where dag_id <> 'airflow_monitoring'
and duration/60 > CASE WHEN task_id = 'export_dolph_transactions' THEN 600 ELSE 300 END
and start_date::timestamp with time zone > CURRENT_DATE - 1;
'''

default_args = {
    'owner': 'de-anyfin',
    'depends_on_past': False, 
    'retries': 0,
    'dagrun_timeout': timedelta(minutes=20),
    'on_failure_callback': partial(slack_notification.task_fail_slack_alert, SLACK_CONNECTION),
    'start_date': datetime(2022, 6, 6),
}

dag = DAG(
    dag_id="airflow_long_running_tasks", 
    default_args=default_args, 
    schedule_interval="30 */2 * * *",  # Run this DAG every two hour at minute 30 
    max_active_runs=1,
    catchup=False
)

check_tasks = SQLValueCheckOperator(
    task_id="check_tasks",
    depends_on_past=True,
    conn_id="airflow_db", 
    sql=QUERY,
    pass_value=0,
    dag=dag
)