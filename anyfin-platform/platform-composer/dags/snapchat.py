import requests
import pytz
import logging
import json
from google.cloud import bigquery
from datetime import datetime, timedelta
from airflow import DAG, models
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.exceptions import AirflowFailException

from utils import slack_notification
from functools import partial

SLACK_CONNECTION = 'slack_data_engineering'
ad_account_id = '62e180ea-884d-4383-815f-9d7624d5f31f'
bq_schema = [
    bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("ad_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ad_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("ad_squad_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("creative_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("campaign_id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("campaign_name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("country_code", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("spend", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("currency_code", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("impressions", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("swipes", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("swipe_up_percent", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("view_time_millis", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("screen_time_millis", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("video_views", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("android_installs", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("ios_installs", "INTEGER", mode="NULLABLE"),
    bigquery.SchemaField("uniques", "INTEGER", mode="NULLABLE")
]

default_args = {
    'owner': 'de-anyfin',
    'depends_on_past': True,
    'start_date': datetime(2022,3,27),
    'retries': 3,
    'retry_delay': timedelta(minutes=4),
    'on_failure_callback': partial(slack_notification.task_fail_slack_alert, SLACK_CONNECTION),
}

dag = DAG('marketing_cost_snapchat', 
    default_args = default_args, 
    catchup = True,
    schedule_interval='0 03 * * *',
    max_active_runs = 1)

def get_access_token(secrets):
    url = 'https://accounts.snapchat.com/login/oauth2/access_token' 
    try:
        r = requests.post(url, data=secrets)
        r.raise_for_status()
        token = r.json()['access_token']
        return token
    except requests.exceptions.RequestException as e:
        raise AirflowFailException(e)

def fetch_data(url, headers, payload={'limit' : 100}):
    try:
        r = requests.get(url, headers=headers, params=payload)
        r.raise_for_status()
        return r.json()    
    except requests.exceptions.RequestException as e:
        raise AirflowFailException(e)

def parse_request(ds, **kwargs):
    secrets = json.loads(models.Variable.get('snapchat_secrets'))
    access_token = get_access_token(secrets)
    headers = {'content-type': 'application/json','Authorization': 'Bearer ' + access_token}
    campaigns_url = f'https://adsapi.snapchat.com/v1/adaccounts/{ad_account_id}/campaigns'
    c_data = fetch_data(campaigns_url, headers)
    
    rows = []
    for c in c_data['campaigns']:
        campaign = c.get('campaign', {})
        campaign_id = campaign.get('id')
        campaign_name = campaign.get('name')

        ads_url = f'https://adsapi.snapchat.com/v1/campaigns/{campaign_id}/ads'
        a_data = fetch_data(ads_url, headers)

        for a in a_data['ads']:
            ad = a.get('ad', {}) 
            ad_id = ad.get('id')
            #Get midnight in Europe/Stockholm timezone to use in api call                
            start_time = kwargs['execution_date'].astimezone(pytz.timezone("Europe/Stockholm")).replace(hour=0)
            end_time = (kwargs['execution_date'] + timedelta(days=1)).astimezone(pytz.timezone("Europe/Stockholm")).replace(hour=0)
            
            payload = {
                'granularity': 'DAY',
                'fields': 'impressions,swipes,view_time_millis,screen_time_millis,spend,video_views,android_installs,ios_installs,swipe_up_percent,uniques',
                'start_time': str(start_time),
                'end_time': str(end_time),
                'omit_empty': True,
                'conversion_source_types': 'web,app',
                'swipe_up_attribution_window': '1_DAY',
                'view_attribution_window': '1_DAY'
            }
            stats_url = f'https://adsapi.snapchat.com/v1/ads/{ad_id}/stats'
            stats_data = fetch_data(stats_url, headers=headers, payload=payload)
            all_stats = stats_data['timeseries_stats'][0]['timeseries_stat'] #contains list of stats for 1 or multiple days + common ids and ts for all days
            stats_data = all_stats['timeseries'] 
            if len(stats_data) > 0: 
                for s in stats_data:
                    if s['stats'].get('spend', 0.00) > 0:
                        date = start_time.strftime('%Y-%m-%d')
                        row = [
                            date,
                            ad_id, 
                            ad.get('name'), 
                            ad.get('ad_squad_id', None),
                            ad.get('creative_id', None),                            
                            campaign_id,
                            campaign_name, 
                            campaign_name.split('_')[0].upper(), #assuming that campaigns follow country_* naming convention
                            s['stats'].get('spend', 0.00)/1000000.00*1.25, # converting microcurrency and adding VAT
                            'EUR',
                            s['stats'].get('impressions', 0),
                            s['stats'].get('swipes', 0),
                            s['stats'].get('swipe_up_percent', None),
                            s['stats'].get('view_time_millis', 0),
                            s['stats'].get('screen_time_millis', 0),
                            s['stats'].get('video_views', 0),
                            s['stats'].get('android_installs', 0),
                            s['stats'].get('ios_installs', 0),
                            s['stats'].get('uniques', 0)
                        ]
                        rows.append(row)

    if len(rows) > 0:
        table = 'anyfin.marketing.daily_snapchat_ads'
        hook = BigQueryHook('postgres-bq-etl-con')
        client = bigquery.Client(project = 'anyfin', credentials = hook._get_credentials())
        client.query(f"DELETE FROM `{table}` WHERE date = '{ds}'") 
        ins = client.insert_rows(table=table, rows=rows, selected_fields=bq_schema)
        if ins == []:
            logging.info(f"New rows for date {ds} inserted to table - {table}")
        else: 
            raise AirflowFailException("No rows were inserted due to these errors: ", ins)
    else:
        logging.info(f"Looks like there was no spend on snapchat for date {ds}")

ingest_ad_report = PythonOperator(
    task_id = 'ingest_daily_ad_report',
    python_callable = parse_request,
    provide_context = True,
    dag = dag
)
