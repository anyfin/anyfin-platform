import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

# from utils import slack_notification
# from functools import partial

# PythonVirtualenvOperator doesn't accept a path for a requirements.txt file
# It can only take a list of dependencies as input 
with open("/home/airflow/gcs/dags/treasury_forecast/model/requirements.txt", "r") as req:
    requirements = req.readlines()

gcp = GoogleBaseHook(gcp_conn_id="postgres-bq-etl-con")
cred_dict = json.loads(gcp._get_field('keyfile_dict'))

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


def run_model(credentials_dict):
    import sys
    import logging

    from oauth2client.service_account import ServiceAccountCredentials

    # PythonVirtualenvOperator creates a completely isolated environment where the dags
    # folder isn't in the PYTHONPATH and has to be inserted the following way
    # sys.path.insert(0, "/opt/airflow/dags")
    sys.path.insert(0, "/home/airflow/gcs/dags/")

    from treasury_forecast.model.AnyfinPortfolioForecast import AnyfinPortfolioForecast
    from treasury_forecast.model.Settings import Settings
    from treasury_forecast.utils import download_settings_file, upload_csv_to_gdrive

    # Do we want to store them as Airflow Variables to not store them open, or it's fine?
    # id of the folder in gdrive where we want to export csv files
    gdrive_folder_id = ''
    settings_sheet_url = ''
    data_storage_path = '/home/airflow/gcs/data/treasury_forecast/'

    scopes_drive = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/drive.appdata',
                    'https://www.googleapis.com/auth/drive.file', 'https://www.googleapis.com/auth/drive.metadata']
    scopes_sheets = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.file',
                     'https://www.googleapis.com/auth/drive.readonly']

    logging.info('Downloading settings file')
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scopes=scopes_sheets)
    download_settings_file(creds, settings_sheet_url, data_storage_path)
    logging.info('Settings file downloaded')

    logging.info("Load model settings")
    settings = Settings()
    logging.info("Settings file loaded")

    logging.info("Generate complete forecast")
    fc = AnyfinPortfolioForecast(settings)
    fc.generate_complete_forecast()
    logging.info("Forecast finished")

    logging.info("Loading results to Google Drive")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scopes=scopes_drive)
    upload_csv_to_gdrive(creds, gdrive_folder_id, local_path_to_files=f'{data_storage_path}data/')
    logging.info("CSV files are loaded to Google Drive")


dag = DAG(
    dag_id="treasury_forecast",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    max_active_runs=1,
    catchup=False,
)

PythonVirtualenvOperator(
    python_callable=run_model,
    op_kwargs={
        'credentials_dict': cred_dict
    },
    task_id="run_model",
    requirements=requirements,
    system_site_packages=False,
    provide_context=True,
    dag=dag,
)
