from airflow import DAG
from airflow.operators.bash import BashOperator


dag = DAG(
    "dummy_dag",
    default_args={},
    catchup=False,
    schedule_interval=None
)

run_this = BashOperator(
    task_id='dummy_task',
    bash_command='echo 1',
)