import json

import holidays
import requests
from lxml import etree
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator

from utils import slack_notification
from functools import partial

SLACK_CONNECTION = 'slack_data_engineering'

default_args = {
    'owner': 'de-anyfin',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 30),
    'retries': 0,
    'on_failure_callback': partial(slack_notification.task_fail_slack_alert, SLACK_CONNECTION),
}


def modify_xml(ds):
    tree = etree.parse('/home/airflow/gcs/dags/utils/euribor_message.xml')
    root = tree.getroot()
    root.find('.//datefrom').text = ds
    root.find('.//dateto').text = ds

    tree.write('/home/airflow/gcs/dags/utils/euribor_message.xml')


def parse_values(resp):
    tree = etree.fromstring(bytes(resp.text, encoding='utf8'))
    try:
        date = tree.find('.//date').text
        value = float(tree.find('.//value').text)
        return date, value
    except Exception as e:
        print(f"Wasn't able to parse value. Error: {e}")
        return None


def generate_pubsub_message(date, value, index_type):
    return json.dumps({
        "dateFrom": f"{date}",
        "countryCode": "FI",
        "type": f"{index_type}",
        "value": value
    })


def str_to_bytes(text):
    return bytes(text, 'utf-8')


def choose_branch(result):
    if result == -1:
        return 'notify_fetch_nothing'
    elif type(result) == str:
        return 'publish_to_pubsub'
    else:
        return 'do_nothing'


def fetch_eurobor3m_and_prepare_pubsub_msg(ds):
    fetch_date = datetime.strptime(ds, '%Y-%m-%d')
    # checking if date is not a holiday and not a weekend because these are not banking days
    # if fetch_date not in holidays.SE() and fetch_date.weekday() not in [5, 6]:
    if fetch_date.weekday() not in [5, 6] and fetch_date not in holidays.SE():
        modify_xml(fetch_date.strftime('%Y-%m-%d'))
        url = 'http://swea.riksbank.se/sweaWS/services/SweaWebServiceHttpSoap12Endpoint'
        headers = {'Content-Type': 'application/soap+xml;charset=utf-8;action=urn:getInterestAndExchangeRates'}
        with open('/home/airflow/gcs/dags/utils/euribor_message.xml', 'rb') as f:
            data = f.read()
        response = requests.post(url, headers=headers, data=data)
        val = parse_values(response)
        if val is None:
            print('Fetched nothing!')
            return -1
        date, value = val[0], val[1]
        if date != ds:
            print(f'Incorrect date. Expected {ds} parsed {date}')
            return -1
        return generate_pubsub_message(date, value, 'EURIBOR_3M')
    else:
        print('Not a banking day. Skip fetching')
        return 0


dag = DAG('euribor_index',
          default_args=default_args,
          catchup=False,
          max_active_runs=1,
          schedule_interval='5 7 * * *',
          render_template_as_native_obj=True,
          user_defined_macros={'message_data': str_to_bytes}
          )

fetch_rates = PythonOperator(
    task_id='fetch_rates',
    python_callable=fetch_eurobor3m_and_prepare_pubsub_msg,
    provide_context=True,
    dag=dag
)
choose_branch = BranchPythonOperator(
    task_id='check_branch',
    python_callable=choose_branch,
    op_kwargs={'result': fetch_rates.output},
    dag=dag,
)

notify_fetch_nothing = PythonOperator(
    task_id='notify_fetch_nothing',
    python_callable=slack_notification.failed_to_fetch_eurobor,
    provide_context=True,
    op_kwargs={'slack_connection': SLACK_CONNECTION},
    dag=dag,
)

do_nothing = DummyOperator(
    task_id='do_nothing',
    dag=dag
)

post_message_to_pubsub = PubSubPublishMessageOperator(
    task_id='publish_to_pubsub',
    project_id='anyfin',
    topic='floating-interest.created',
    gcp_conn_id='bigquery-composer-connection',
    messages=[
        {'data': "{{ message_data(task_instance.xcom_pull(task_ids='fetch_rates', key='return_value')) }}"}
    ],
    dag=dag,
)

fetch_rates >> choose_branch >> [notify_fetch_nothing, post_message_to_pubsub, do_nothing]
