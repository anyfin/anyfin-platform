import requests
from datetime import datetime, timedelta
from airflow import DAG
from lxml import etree
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

from utils import slack_notification
from functools import partial

#  list of all returning currencies is here https://www.riksbank.se/en-gb/statistics/search-interest--exchange-rates/web-services/series-for-web-services/

SLACK_CONNECTION = 'slack_data_engineering'

default_args = {
    'owner': 'de-anyfin',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'schedule_interval': '0 0 1 * *',
    'on_failure_callback': partial(slack_notification.task_fail_slack_alert, SLACK_CONNECTION),
}


def modify_xml(fetch_date):
    if fetch_date.month < 1 or fetch_date.month > 12:
        raise ValueError(f'Incorrect month number {fetch_date.month}')
    tree = etree.parse('/home/airflow/gcs/dags/utils/message.xml')

    root = tree.getroot()
    year_tag = root.find('.//year')
    month_tag = root.find('.//month')

    year_tag.text = str(fetch_date.year)
    month_tag.text = str(fetch_date.month)

    tree.write('/home/airflow/gcs/dags/utils/message.xml')


def get_insert_query(response, fetch_date):
    tree = etree.fromstring(bytes(response.text, encoding='utf8'))

    error_namespace = {'SOAP-ENV': 'http://www.w3.org/2003/05/soap-envelope'}
    if tree.xpath('.//SOAP-ENV:Fault', namespaces=error_namespace):
        raise ValueError(
            f"Failed to fetch rates. Server returned error: {tree.xpath('.//SOAP-ENV:Text', namespaces=error_namespace)[0].text}")

    series = tree.xpath('.//series')
    date = fetch_date.replace(day=1).strftime('%Y-%m-%d')
    output = []
    for item in series:
        currency = item.find('.//seriesname').text
        if currency not in ['USD', 'NOK', 'EUR']:
            continue
        unit = float(item.find('.//unit').text)
        value = float(item.find('.//ultimo').text)
        one_unit_value = value / unit
        output.append((date, currency, one_unit_value))
    if output:
        values = ', '.join(map(str, output))
        query = f'CREATE TEMP TABLE cr_temp(exchange_month DATE, currency_code STRING, value NUMERIC); ' \
                f'INSERT INTO cr_temp VALUES { values };' \
                f'MERGE dim.currency_rates_dim T \
                    USING cr_temp S \
                    ON T.exchange_month = S.exchange_month AND T.currency_code = S.currency_code \
                    WHEN MATCHED THEN \
                      UPDATE SET value = S.value \
                    WHEN NOT MATCHED THEN \
                      INSERT (exchange_month, currency_code, value) VALUES(exchange_month, currency_code, value);'
        return query
    else:
        print('No data from API. Skipping all tasks')
        raise AirflowSkipException


def fetch_rates_and_prepare_query(ds):
    fetch_date = datetime.strptime(ds, '%Y-%m-%d')
    modify_xml(fetch_date)
    url = 'http://swea.riksbank.se/sweaWS/services/SweaWebServiceHttpSoap12Endpoint'
    headers = {'Content-Type': 'application/soap+xml;charset=utf-8;action=urn:getMonthlyAverageExchangeRates'}
    with open('/home/airflow/gcs/dags/utils/message.xml', 'rb') as f:
        data = f.read()

    response = requests.post(url, headers=headers, data=data)
    query = get_insert_query(response, fetch_date)
    return query


dag = DAG('currency_rates',
          default_args=default_args,
          catchup=False,
          max_active_runs=1,
          )

fetch_rates_and_prepare_query = PythonOperator(
    task_id='fetch_rates_and_prepare_query',
    python_callable=fetch_rates_and_prepare_query,
    do_xcom_push=True,
    dag=dag,
)

insert_to_bq = BigQueryExecuteQueryOperator(
    task_id='insert_rates',
    gcp_conn_id='bigquery-composer-connection',
    sql="{{ ti.xcom_pull(task_ids='fetch_rates_and_prepare_query') }}",
    use_legacy_sql=False,
    dag=dag,
)

fetch_rates_and_prepare_query >> insert_to_bq
