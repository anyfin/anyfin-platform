import requests
import json
import os
import logging
from google.cloud import bigquery
from tempfile import NamedTemporaryFile
from airflow import DAG, macros, models
from datetime import datetime, timedelta, timezone
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

ORG_ID = '1555680'

default_args = {
    'owner': 'growth',
    'depends_on_past': True,
    'start_date': datetime(2020,8,28),
    'retries': 3,
    'retry_delay': timedelta(minutes=4),
    'email_on_failure': True,
    'email': models.Variable.get('growth_email'),
    'email_on_retry': False
}

dag = DAG('marketing_apple_ads', 
    default_args = default_args, 
    catchup = True,
    schedule_interval='0 03 * * *',
    max_active_runs = 1)

def download_campaigns_report(ds, **kwargs):
    """
    Function to fetch metrics for all campaigns within an organization. 
    """
    #authenticating by downloading key and pem files from gcs
    gs = GoogleCloudStorageHook(google_cloud_storage_conn_id='google_cloud_default')
    apple_pem = NamedTemporaryFile(suffix='.pem')
    apple_pem.write(gs.download('metrics-pipeline', 'apple_search_ads/anyfin.pem'))
    apple_pem.seek(os.SEEK_SET)
    apple_key = NamedTemporaryFile(suffix='.key')
    apple_key.write(gs.download('metrics-pipeline', 'apple_search_ads/anyfin.key'))
    apple_key.seek(os.SEEK_SET)
    APPLE_CERT = (apple_pem.name, apple_key.name)

    url = "https://api.searchads.apple.com/api/v2/reports/campaigns"
    request_body = create_report(ds)
    headers = {"Authorization": f"orgId={ORG_ID}"}
    response = requests.post(url, cert=APPLE_CERT, json=request_body, headers=headers)
    response.raise_for_status()
    response.encoding = "utf-8"
    
    apple_pem.close()
    apple_key.close()
    
    all_campaigns = response.json()['data']['reportingDataResponse']['row']
    rows = []

    for campaign in all_campaigns:
        #Not interested in rows with 0 spend
        if campaign['granularity'][0]['localSpend'].get('amount',0)!=0:
            row = {
                'date':campaign['granularity'][0].get('date', None),
                'campaign_id':campaign['metadata'].get('campaignId'),
                'ad_channel_type':campaign['metadata'].get('adChannelTyp', None),
                'app_dict':json.dumps(campaign['metadata'].get('app', {})),
                'campaign_name':campaign['metadata'].get('campaignName', None),
                'campaign_status':campaign['metadata'].get('campaignStatus', None),
                'countries':','.join(campaign['metadata'].get('countriesOrRegions', None)),
                'deleted':campaign['metadata'].get('deleted', False),
                'display_status':campaign['metadata'].get('displayStatus', None),
                'modification_time':campaign['metadata'].get('modificationTime', None),
                'serving_status':campaign['metadata'].get('servingStatus', None),
                'total_budget':campaign['metadata']['totalBudget'].get('amount',0),
                'total_remaining_budget':campaign['metadata'].get('totalRemainingBudget', {}).get('amount', 0),
                'impressions':campaign['granularity'][0].get('impressions', 0),
                'local_spend':campaign['granularity'][0]['localSpend'].get('amount',0),
                'currency_code':campaign['granularity'][0]['localSpend'].get('currency',None),
                'new_downloads':campaign['granularity'][0].get('newDownloads', 0),
                'redownloads':campaign['granularity'][0].get('redownloads', 0),
                'taps':campaign['granularity'][0].get('taps',0)
            }
            rows.append(row)
    
    #Streaming insert to Bigquery since the volume is very low (~0-5 rows per day). Might consider bulk loads later on if volume increases
    table = 'anyfin-segment.apple_search_ads.daily_campaigns'
    hook = BigQueryHook.get_hook('bigquery_segment')
    client = bigquery.Client(project = 'anyfin-segment', credentials = hook._get_credentials())
    client.query(f"DELETE FROM `{table}` WHERE date = '{ds}'")
    ins = client.insert_rows(client.get_table(table), rows)

    if len(ins) > 0:
        logging.info(f"New rows for date {ds} inserted to table - {table}")
    else:
        logging.info("No new rows inserted")


def create_report(ds):
    #ORTZ is timezone of apple search account ie Europe/Stockholm CET/CEST
    campaign_report = \
        {
            "startTime": ds,  
            "endTime": ds, 
            "granularity": 'DAILY',
            "selector": {
                "orderBy": [
                    {
                        "field": "countryCode",
                        "sortOrder": "ASCENDING"
                    }
                ],
                "conditions": [
                    {
                        "field": "deleted",
                        "operator": "IN",
                        "values": ["false", "true"]
                    }
                ],
                "pagination": {
                    "offset": 0,
                    "limit": 1000
                }
            },
            "groupBy": [
                "countryCode"
            ],
            "timeZone": 'ORTZ',
            "returnRecordsWithNoMetrics": False,
            "returnRowTotals": False,
            "returnGrandTotals": False
        }
    return campaign_report

ingest_campaign_report = PythonOperator(
    task_id = 'ingest_campaigns_report',
    python_callable = download_campaigns_report,
    provide_context = True,
    dag = dag
)
