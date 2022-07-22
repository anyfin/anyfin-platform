import json

from bs4 import BeautifulSoup
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
WEEKENDS_INDEXES = [5, 6]
EURIBOR_RATES_URL = 'https://www.euribor-rates.eu/en/current-euribor-rates/'
EXPECTED_ROW_TITLE = 'Euribor 3 months'

default_args = {
    'owner': 'de-anyfin',
    'depends_on_past': False,
    'start_date': datetime(2022, 7, 11),
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
    if fetch_date.weekday() not in WEEKENDS_INDEXES:
        page = requests.get(EURIBOR_RATES_URL).text
        soup = BeautifulSoup(page, parser='html', features="lxml")
        table = soup.find('table', {"class": 'table table-striped'})

        data = []

        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all('a')
            cols.extend(row.find_all('td') or row.find_all('th'))
            cols = [ele.text.strip() for ele in cols]
            data.append([ele for ele in cols if ele])
        if data[0][0] == fetch_date.strftime('%-m/%-d/%Y') and data[3][0] == EXPECTED_ROW_TITLE:
            print(data[0][0], data[3][1])
            date = datetime.strptime(data[0][0], '%m/%d/%Y').strftime('%Y-%m-%d')
            value = data[3][1].split(' ')[0]
            return generate_pubsub_message(date, value, 'EURIBOR_3M')
        else:
            print(f'Fetched nothing! Either there is no value for required date or name {EXPECTED_ROW_TITLE} is changed')
            return -1
    else:
        print('Not a banking day!')
        return 0


dag = DAG('euribor_index',
          default_args=default_args,
          catchup=False,
          max_active_runs=1,
          schedule_interval='5 11 * * *',
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
