import json
import requests
import logging
from google.cloud import bigquery
from six import string_types
from six.moves.urllib.parse import urlencode, urlunparse
from airflow import DAG, macros, models
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.exceptions import AirflowFailException

ADVERTISER_ID = 6955479302334906369 
ACCESS_TOKEN = models.Variable.get('tiktok_api_secret')

schema = [
    bigquery.SchemaField("date", "TIMESTAMP", mode="NULLABLE"),
    bigquery.SchemaField("ad_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("country_code", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("currency_code", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ad_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("adgroup_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("adgroup_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("campaign_id", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("campaign_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("spend", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("reach", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("impressions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("clicks", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_play_actions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_watched_2s", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_watched_6s", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("average_video_play", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("video_views_p25", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_views_p50", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_views_p75", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_views_p100", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("profile_visits", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("likes", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("comments", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("shares", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("follows", "INTEGER", mode="NULLABLE")
]

default_args = {
    'owner': 'growth',
    'depends_on_past': True,
    'start_date': datetime(2021,4,29),
    'retries': 3,
    'retry_delay': timedelta(minutes=4),
    'email_on_failure': True,
    'email': models.Variable.get('growth_email'),
    'email_on_retry': False
}

dag = DAG('marketing_cost_tiktok_ads', 
    default_args = default_args, 
    catchup = True,
    schedule_interval='0 03 * * *',
    max_active_runs = 1)

def build_url(path, query=""):
    """
    Build request URL
    :param path: Request path
    :param query: Querystring
    :return: Request URL
    """
    scheme, netloc = "https", "ads.tiktok.com"
    return urlunparse((scheme, netloc, path, "", query, ""))

def get_ad_report(ds, **kwargs):
    PATH = "/open_api/v1.2/reports/integrated/get/"

    #metric description in docs https://ads.tiktok.com/marketing_api/docs?id=1685764234508290
    metrics_list = ["ad_name","adgroup_id","adgroup_name","campaign_id","campaign_name","spend","reach","impressions","clicks","video_play_actions","video_watched_2s","video_watched_6s","average_video_play","video_views_p25","video_views_p50","video_views_p75","video_views_p100","profile_visits","likes","comments","shares","follows"] 
    metrics = json.dumps(metrics_list)
    dimensions_list = ["stat_time_day","ad_id"]
    dimensions = json.dumps(dimensions_list)
    advertiser_id = ADVERTISER_ID
    data_level = 'AUCTION_AD' #eventually RESERVATION_ADs also?
    start_date = ds
    end_date = ds
    page_size = 100
    service_type = "AUCTION"
    lifetime = False #set to true for backfills
    report_type = "BASIC"
    page = 1

    my_args = "{\"metrics\": %s, \"data_level\": \"%s\", \"end_date\": \"%s\", \"page_size\": \"%s\", \"start_date\": \"%s\", \"advertiser_id\": \"%s\", \"service_type\": \"%s\", \"lifetime\": \"%s\", \"report_type\": \"%s\", \"page\": \"%s\", \"dimensions\": %s}" % (metrics, data_level, end_date, page_size, start_date, advertiser_id, service_type, lifetime, report_type, page, dimensions)
    args = json.loads(my_args)
    logging.info("Sending request with the following args: ", my_args)

    query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in args.items()})
    url = build_url(PATH, query_string)
    headers = {
        "Access-Token": ACCESS_TOKEN
    }

    response = requests.get(url, headers=headers).json()

    if response['message'] == 'OK':
        ads = response['data']['list']
        rows = [] 
        columns = ['date', 'ad_id', 'country_code', 'currency_code'] + metrics_list
        for ad in ads:
            if float(ad["metrics"].get("spend")) > 0.0:
                dims = [ad["dimensions"].get(dim) for dim in dimensions_list]
                metr = [ad["metrics"].get(met) for met in metrics_list]
                vals = dims + ['SE', 'SEK'] + metr
                record = dict(zip(columns,vals))
                rows.append(record)
                
        #Streaming insert to Bigquery since the volume is very low. Might consider bulk loads later on if volume increases
        if len(rows) > 0:
            table = 'anyfin.marketing.daily_tiktok_ads'
            hook = BigQueryHook('postgres-bq-etl-con')
            client = bigquery.Client(project = 'anyfin', credentials = hook._get_credentials())
            client.query(f"DELETE FROM `{table}` WHERE date = '{ds}'")
            ins = client.insert_rows(table=table, rows=rows, selected_fields=schema)
            if ins == []:
                logging.info(f"New rows for date {ds} inserted to table - {table}")
            else: 
                raise AirflowFailException("No rows were inserted due to these errors: ", ins)

        else:
            logging.info(f"Looks like there was no spend on tiktok for date {ds}")
    else:
        raise AirflowFailException(f"The http request was not successful: {response['message']}")


ingest_ad_report = PythonOperator(
    task_id = 'ingest_daily_ad_report',
    python_callable = get_ad_report,
    provide_context = True,
    dag = dag
)
