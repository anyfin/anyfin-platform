from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator
from airflow.models import BaseOperator


class GoogleCloudStorageDeleteOperator(BaseOperator):

    template_fields = ('bucket_name', 'prefix', 'objects')

    @apply_defaults
    def __init__(self,
                 bucket_name,
                 objects=None,
                 prefix=None,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args, **kwargs):
        self.bucket_name = bucket_name
        self.objects = objects
        self.prefix = prefix
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

        assert objects is not None or prefix is not None

        super(GoogleCloudStorageDeleteOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to
        )

        if self.objects:
            objects = self.objects
        else:
            objects = hook.list(bucket=self.bucket_name,
                                prefix=self.prefix)

        self.log.info("Deleting %s objects from %s",
                      len(objects), self.bucket_name)
        for object_name in objects:
            hook.delete(bucket=self.bucket_name,
                        object=object_name)


default_args = {
    'owner': 'ds-anyfin',
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'email': Variable.get('ds_email'),
    'start_date': datetime(2020, 8, 28),
    'depends_on_past': False
}

BUCKET = 'prices-fluctuation'
DAILY_EXTRACTED_FILENAME = 'items_prices/{{ ds_nodash }}_daily_export.json'
DESTINATION_TABLE = 'purchase_history.items_prices${{ ds_nodash }}'
SCHEMA_OBJECT = 'items_prices/schema/items_prices_schema.json'

EXTRACT_SQL = '''
with temp as(
    select 
        distinct
        session_id,
		jsonb_array_elements_text(jsonb_array_elements(data->'transactionsAndOrders')->'transaction'->'traits')::json->>'dueDate' as due_date, 
        jsonb_array_elements_text(jsonb_array_elements(data->'transactionsAndOrders')->'order'->'lineItems')::json->>'productUrl' as product_url,
        jsonb_array_elements_text(jsonb_array_elements(data->'transactionsAndOrders')->'order'->'lineItems')::json->>'quantity' as quantity, 
        jsonb_array_elements_text(jsonb_array_elements(data->'transactionsAndOrders')->'order'->'lineItems')::json->'totalAmount'->>'amount' as amount,
        jsonb_array_elements(data->'transactionsAndOrders')->'order'->'merchant'->>'category' as merchant_category,
        jsonb_array_elements(data->'transactionsAndOrders')->'order'->'merchant'->>'displayName' as merchant,
		jsonb_array_elements_text(jsonb_array_elements(data->'transactionsAndOrders')->'transaction'->'traits')::json->>'status' as status,
        jsonb_array_elements(data->'transactionsAndOrders')->'order'->>'capturedAt' as capturedAt
    from snapshots where label = 'klarnaTransactionsAndOrders'
)
select
    distinct
    t.merchant,
    d.id, 
    d.created_at::timestamp::DATE::text as purchase_date,
    t.product_url,
    t.quantity,
    t.amount::numeric / 100 as amount,
    t.merchant_category,
    t.due_date::timestamp::DATE::text as due_date,
    t.status
from temp t inner join ddi_sessions d on d.id = t.session_id
where product_url is not null
and d.created_at::date = '{}';
'''

SUMMARY_SQL = '''
CREATE TEMP FUNCTION res_sum(a ANY TYPE) AS (
        ARRAY(SELECT STRUCT(name, SUM(count) as count, ROUND(SUM(amount),2) as amount) FROM UNNEST(a) GROUP BY name)
    );
CREATE TEMP FUNCTION top_1(a ANY TYPE, metric ANY TYPE) AS (
       (SELECT AS STRUCT st.name, st.amount, st.count from unnest(a) st ORDER BY metric DESC limit 1)
    );
WITH
  detailed_spent_per_session AS (
  SELECT
    a.customer_id,
    s.id,
    p.merchant,
    s.provider,
    p.merchant_category,
    COUNT(distinct s.id) as count,
    SUM(amount) as amount
  FROM
    `anyfin.purchase_history.items_prices` p
  LEFT JOIN
    `anyfin.main.ddi_sessions` s
  ON
    p.id = s.id
  LEFT JOIN
    `anyfin.main.applications` a
  ON
    a.ddi_session_id=s.id
  LEFT JOIN `anyfin.main.external_statements` e ON e.id = a.external_statement_id
  WHERE COALESCE(a.reject_reason, '') != 'duplicate' AND a.customer_id is not null
  GROUP BY 1,2,3,4,5),
  spent_with_breakdown AS (
  SELECT
    customer_id,
    res_sum(ARRAY_AGG(STRUCT(merchant as name, count, amount))) merchant,
    res_sum(ARRAY_AGG(STRUCT(provider as name, count, amount))) provider,
    res_sum(ARRAY_AGG(STRUCT(merchant_category as name, count, amount))) merchant_category,
    SUM(count) AS total_session_count,
    ROUND(SUM(amount),2) AS total_session_amount,
  FROM detailed_spent_per_session
  GROUP BY 1)
  SELECT
    customer_id as id,
    merchant,
    provider,
    merchant_category,
    top_1(merchant, 'amount') as top_merchant_by_amount,
    top_1(provider, 'amount') as top_provider_by_amount,
    top_1(merchant_category, 'amount') as top_merchant_category_by_amount,
    top_1(merchant, 'count') as top_merchant_by_sessions,
    top_1(provider, 'count') as top_provider_by_sessions,
    top_1(merchant_category, 'count') as top_merchant_category_by_sessions
  FROM spent_with_breakdown
'''

with DAG('purchase_history',
         default_args=default_args,
         catchup=False,
         schedule_interval='@daily',
         max_active_runs=1) as dag:

    extract_daily_gcs = PostgresToGoogleCloudStorageOperator(
        task_id='extract_daily_gcs',
        sql=EXTRACT_SQL.format('{{ ds }}'),
        bucket=BUCKET,
        google_cloud_storage_conn_id='postgres-bq-etl-con',
        postgres_conn_id='ddi_replica',
        filename=DAILY_EXTRACTED_FILENAME
    )

    load_daily_bq = GoogleCloudStorageToBigQueryOperator(
        task_id='load_daily_bq',
        bucket=BUCKET,
        source_objects=[DAILY_EXTRACTED_FILENAME, ],
        destination_project_dataset_table=DESTINATION_TABLE,
        schema_object=SCHEMA_OBJECT,
        time_partitioning={'type': 'DAY', 'field': 'purchase_date'},
        source_format='NEWLINE_DELIMITED_JSON',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='postgres-bq-etl-con',
        google_cloud_storage_conn_id='postgres-bq-etl-con',
        dag=dag
    )

    customer_summary = BigQueryOperator(
        task_id='customer_summary',
        sql=SUMMARY_SQL,
        destination_dataset_table="anyfin.purchase_history.customer_purchase_overview",
        cluster_fields=["id"],
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        bigquery_conn_id='bigquery_default',
        create_disposition='CREATE_IF_NEEDED',
        dag=dag
    )

    remove_daily_gcs = GoogleCloudStorageDeleteOperator(
        task_id='remove_daily_gcs',
        bucket_name=BUCKET,
        objects=[DAILY_EXTRACTED_FILENAME, ],
        google_cloud_storage_conn_id='postgres-bq-etl-con',
    )

extract_daily_gcs \
 >> load_daily_bq \
 >> customer_summary \
 >> remove_daily_gcs
 