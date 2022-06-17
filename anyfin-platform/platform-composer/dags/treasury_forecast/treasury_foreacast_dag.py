from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator

# from utils import slack_notification
# from functools import partial

# PythonVirtualenvOperator doesn't accept a path for a requirements.txt file
# It can only take a list of dependencies as input 
with open("dags/treasury_forecast/model/requirements.txt", "r") as req:
    requirements = req.readlines()

default_args = {
    "owner": "de-anyfin",
    "depends_on_past": False,
    "retries": 0,
    "dagrun_timeout": timedelta(minutes=20),
    # "on_failure_callback": partial(
    #     slack_notification.task_fail_slack_alert, SLACK_CONNECTION
    # ),
    "start_date": datetime(2022, 6, 15),
}


def run_model():
    import sys
    import logging
    # PythonVirtualenvOperator creates a completely isolated environment where the dags 
    # folder isn't in the PYTHONPATH and has to be inserted the following way
    sys.path.insert(0, "/opt/airflow/dags")
    from treasury_forecast.model.AnyfinPortfolioForecast import AnyfinPortfolioForecast
    from treasury_forecast.model.AnyfinSPVModel import AnyfinSPVModel
    from treasury_forecast.model.Settings import Settings

    logging.info("Load model settings")
    settings = Settings(
        load_new_data=False,
        generate_new_settings_template=False,
        spv_name="anyfin-finance-1",
    )

    logging.info("Portfolio forecast")
    fc = AnyfinPortfolioForecast(settings)
    # fc.generate_baseline_fc()

    logging.info("SPV forecast")
    spv = AnyfinSPVModel(settings, fc)
    # spv.generate_baseline_fc()

    logging.info("Forecast finished")


dag = DAG(
    dag_id="treasury_forecast",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    max_active_runs=1,
    catchup=False,
)

PythonVirtualenvOperator(
    python_callable=run_model,
    task_id="run_model",
    requirements=requirements,
    system_site_packages=False,
    provide_context=True,
    dag=dag,
)
