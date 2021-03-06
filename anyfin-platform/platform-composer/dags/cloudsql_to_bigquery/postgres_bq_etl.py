import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration

# Magic
from airflow.utils.task_group import TaskGroup
from airflow.utils.weight_rule import WeightRule

from cloudsql_to_bigquery.utils.etl_utils import ETL
from cloudsql_to_bigquery.utils.db_info_utils import DATABASES_INFO

from utils import slack_notification
from functools import partial


PROJECT_NAME = 'anyfin'
GCS_BUCKET = 'sql-to-bq-etl'
DATAFLOW_BUCKET = 'etl-dataflow-bucket'
SLACK_CONNECTION = 'slack_data_engineering'

TEMPLATE_FILE = os.path.dirname(os.path.realpath(__file__)) + '/beam_utils/pg_bq_etl.py'
SETUP_FILE = os.path.dirname(os.path.realpath(__file__)) + '/beam_utils/setup.py'


default_args = {
    'owner': 'de-anyfin',
    'depends_on_past': False,
    'start_date': datetime(2020, 9, 8),
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'on_failure_callback': partial(slack_notification.task_fail_slack_alert, SLACK_CONNECTION),
    'orientation': "TB"
}

with DAG(
    dag_id='postgres_bq_etl',
    default_args=default_args,
    catchup=False,
    schedule_interval=None,
    max_active_runs=3,
    concurrency=12
) as dag:

    etl_groups = {}
    dedup_subgroups = {}
    for DB in DATABASES_INFO:
        DATABASE_NAME = DB['DATABASE_NAME']
        INSTANCE_NAME = DB['INSTANCE_NAME']
        DATABASE = DB['DATABASE']
        DESTINATION_PROJECT = DB['DESTINATION_PROJECT']
        if DESTINATION_PROJECT == 'anyfin-staging':
            DESTINATION_DATASET = DATABASE_NAME.split('-')[0] + '_staging'  # Removes -staging from db name
        else:
            DESTINATION_DATASET = f'{DATABASE_NAME}_staging'

        # MAKES SURE MAIN RUNS BEFORE THE OTHER DAGS
        if DATABASE_NAME == 'main':
            PRIORITY=2
        else:
            PRIORITY=1
        
        # Skipping assess db as Babis has a fancy new ETL for that one :)
        if DATABASE_NAME == 'assess':
            continue

        etl = ETL(GCS_BUCKET=GCS_BUCKET, DATABASE_NAME=DATABASE_NAME)

        g_id = f'{DATABASE_NAME}_etl'
        with TaskGroup(group_id=g_id) as tg:
            task_extract_tables = PythonOperator(
                task_id='extract_tables',
                python_callable=etl.extract_tables,
                do_xcom_push=True,
                trigger_rule=TriggerRule.ALL_DONE,
                priority_weight=PRIORITY,
                weight_rule=WeightRule.ABSOLUTE
            )

            task_no_missing_columns = PythonOperator(
                task_id='no_missing_columns',
                python_callable=etl.no_missing_columns,
                provide_context=True,
                op_kwargs={'task_name': g_id},
                retries=0,
                priority_weight=PRIORITY,
                weight_rule=WeightRule.ABSOLUTE
            )

            task_upload_result_to_gcs = PythonOperator(
                task_id='upload_result_to_gcs',
                python_callable=etl.upload_table_names,
                provide_context=True,
                op_kwargs={'task_name': g_id},
                retries=2,
                priority_weight=PRIORITY,
                weight_rule=WeightRule.ABSOLUTE
            )

            run_dataflow_pipeline = BeamRunPythonPipelineOperator(
                task_id=f"run_{DATABASE_NAME}_dataflow_pipeline",
                runner="DataflowRunner",
                py_file=TEMPLATE_FILE,
                py_options=[],
                py_requirements=[],
                py_interpreter='python3',
                py_system_site_packages=True,
                pipeline_options={
                    "project": PROJECT_NAME,
                    "worker_zone": 'europe-west1-b',
                    "region": "europe-west1",
                    "staging_location": f'gs://{DATAFLOW_BUCKET}/Staging/',
                    "experiment": "use_beam_bq_sink",
                    "date": '{{ds}}',
                    "machine_type": "n1-standard-4",
                    "num-workers": '1',
                    "temp_location": f'gs://{DATAFLOW_BUCKET}/Temp/',
                    "database_name": f"{DATABASE_NAME}",
					"destination_project": f"{DESTINATION_PROJECT}",
                    "destination_dataset": f"{DESTINATION_DATASET}",
                    "setup_file": SETUP_FILE,
                    "poll_sleep": 30,
                },
                email_on_failure=True,
                dataflow_config=DataflowConfiguration(
                    job_name=f'{DATABASE_NAME}-etl', 
                    project_id=PROJECT_NAME, 
                    location="europe-west1",
                    gcp_conn_id="dataflow-etl-connection"
                ),
                priority_weight=PRIORITY,
                weight_rule=WeightRule.ABSOLUTE
            )

            dedup_tasks = []
            dedup_g_id = 'deduplicate'
            with TaskGroup(group_id=dedup_g_id) as dedup_tg:
                for table in etl.get_tables():
                    
                    # If ignore daily is true we dont want to deduplicate
                    if etl.get_tables().get(table).get('ignore_daily'):
                        continue
                    
                     # If the table is gdpr_sensitive we don't want to dedup
                    if etl.get_tables().get(table).get('gdpr_sensitive'):
                        continue

                    DEDUP_DESTINATION_DATASET = DESTINATION_DATASET.split('_')[0] # Removes _staging
                    destination_table = table if '.' not in table else table.split('.')[1]
                    
                    if DATABASE_NAME == 'main' and table == 'assessments':
                        dedup = BigQueryExecuteQueryOperator(
                            task_id=table,
                            sql=f"""
                                with temp as (
                                    SELECT 
                                        id, 
                                        max(_ingested_ts) as max_ingested_ts 
                                    FROM {DESTINATION_PROJECT}.{DESTINATION_DATASET}.assessments_raw group by 1
                                )
                                SELECT 
                                    t.*,
                                    json_extract(main_policy,  '$.data.Rdm' )  as `rdm`,
                                    json_extract(main_policy,  '$.data.Kalp' )  as `kalp`,
                                    json_extract(main_policy,  '$.data.Limit' )  as `limit`,
                                    json_extract(main_policy,  '$.data.Pricing' )  as `pricing`,
                                    json_extract(main_policy,  '$.data.Request' )  as `request_schema`,
                                    json_extract(main_policy,  '$.data.request' )  as `request`,
                                    json_extract(main_policy,  '$.data.Capacity' )  as `capacity`,
                                    json_extract(main_policy,  '$.data.Customer' )  as `customer`,
                                    json_extract(main_policy,  '$.data.Response' )  as `response_schema`,
                                    json_extract(main_policy,  '$.data.UCLookup' )  as `uc_lookup`,
                                    json_extract(main_policy,  '$.data.response' )  as `response`,
                                    json_extract(main_policy,  '$.data.PepScreen' )  as `pep_screen`,
                                    json_extract(main_policy,  '$.data.Rejection' )  as `rejection`,
                                    json_extract(main_policy,  '$.data.decisions' )  as `decisions`,
                                    json_extract(main_policy,  '$.data.ReverseLookup' )  as `reverselookup`,
                                    json_extract(main_policy,  '$.data.CurrentPricing' )  as `current_pricing`,
                                    json_extract(main_policy,  '$.data.InternalLookup' )  as `internal_lookup`,
                                    json_extract(main_policy,  '$.data.SanctionScreen' )  as `sanction_screen`,
                                    json_extract(main_policy,  '$.data.CustomerMatcher' )  as `customer_matcher`,
                                    json_extract(main_policy,  '$.data.SuggestedPricing' )  as `suggested_pricing`,
                                    json_extract(main_policy,  '$.data.NewCustomerPolicy' )  as `new_customer_policy`,
                                    json_extract(main_policy,  '$.data.NewCustomerScoring' )  as `new_customer_scoring`,
                                    json_extract(main_policy,  '$.data.ReturningCustomerScoring' )  as `returning_customer_scoring`,
                                    json_extract(main_policy,  '$.data.Scoring')  as `scoring`,
                                    json_extract(main_policy,  '$.data.ReturningCustomerPolicy' )  as `returning_customer_policy`,
                                    json_extract(main_policy,  '$.data.PrimaryCreditCriteria' )  as `primary_credit_criteria`,
                                    json_extract(main_policy,  '$.data.labelling_reject_reasons' )  as `labelling_reject_reasons`,
                                    json_extract(main_policy,  '$.data.labelling_warning_reasons' )  as `labelling_warning_reasons`,
                                    json_extract(main_policy,  '$.data.labelling_auto_reject_reasons' )  as `labelling_auto_reject_reasons`,
                                    json_extract(main_policy,  '$.data.AsiakastietoLookup' )  as `asiakastieto_lookup`,
                                    json_extract(main_policy,  '$.data.AsiakastietoCcisLookup' )  as `asiakastieto_ccis_lookup`,
                                    json_extract(main_policy,  '$.data.KalpNew' )  as `kalp_new`,
                                    json_extract(main_policy,  '$.data.CustomerPolicy' )  as `customer_policy`,
                                    json_extract(main_policy,  '$.data.CustomerMatcherSchufaId' )  as `customer_matcher_schufa_id`,
                                    json_extract(main_policy,  '$.data.SCLookup' )  as `sc_lookup`,
                                    json_extract(main_policy,  '$.data.CRIFBuergelLookup' )  as `crif_buergel_lookup`,
                                    CAST(json_extract_scalar(main_policy,  '$.data.InternalLookup.exposure' ) as NUMERIC) as `exposure`,
                                    CAST(json_extract_scalar(main_policy,  '$.data.request.loanBalance' ) as NUMERIC) as `loan_balance`,
                                    CAST(json_extract_scalar(main_policy,  '$.data.Customer.returning' ) as bool)  as `is_returning`,
                                    CAST(json_extract_scalar(main_policy, '$.data.UCLookup.credit_history[0].credit_used_blanco') as int64) as uc_credit_used_blanco,
                                    CAST(json_extract_scalar(main_policy, '$.data.UCLookup.credit_history[0].credit_used_account') as int64) as uc_credit_used_account,
                                    CAST(json_extract_scalar(main_policy, '$.data.UCLookup.credit_history[0].credit_used_instalment') as int64) as uc_credit_used_instalment,
                                    COALESCE(SAFE.TIMESTAMP(json_extract_scalar(main_policy, '$.data.UCLookup._ts')), 
                                            SAFE.PARSE_TIMESTAMP("%a, %d %b %Y %X %Z", json_extract_scalar(main_policy, '$.data.UCLookup._ts'))) as uc_created_at,
                                    json_extract_string_array(main_policy,  '$.data.response.reasons' )  as `response_reasons`,
                                    json_extract(main_policy,'$.data.InternalLookup.customer_provided_income_gross')  as `customer_provided_income_gross`,
                                    json_extract_scalar(json_extract(main_policy,'$.data.Pricing'), '$.new.monthlyPayment')  as `new_monthly_payment`,
                                    json_extract_scalar(json_extract(main_policy,'$.data.Pricing'), '$.old.monthlyPayment')  as `old_monthly_payment`,
                                    json_extract_scalar(main_policy,'$.response.scorecard_version') as scorecard_version,
                                    CAST(json_extract_scalar(main_policy,  '$.data.InternalLookup._id' ) as INT64) as `internal_lookup_id`,
                                    COALESCE(json_extract(main_policy, '$.data.Limit.limit'), json_extract(main_policy, '$.data.Limit.suggested_limit')) as suggested_limit,
                                    COALESCE(json_extract_scalar(main_policy, '$.data.Limit.limit_source'),  json_extract_scalar(main_policy, '$.data.Limit.customer_type')) as customer_type,
                                    json_extract(main_policy,  '$.data.AsiakastietoLookup._id' )  as `asiakastieto_lookup_id`,
                                    json_extract(main_policy,  '$.data.AsiakastietoCcisLookup._id' )  as `asiakastieto_ccis_lookup_id`,
                                    json_extract(main_policy,  '$.data.SCLookup._id' )  as `schufa_lookup_id`,
                                    json_extract(main_policy,  '$.data.CRIFBuergelLookup._id' )  as `crif_buergel_lookup_id`
                                    from temp join 
                                    {DESTINATION_PROJECT}.{DESTINATION_DATASET}.{table}_raw t on temp.id= t.id and temp.max_ingested_ts=t._ingested_ts
                            """,
                            destination_dataset_table=f"{DESTINATION_PROJECT}.{DEDUP_DESTINATION_DATASET}.{table}",
                            cluster_fields=['id'],
                            time_partitioning={'field': 'created_at'},
                            use_legacy_sql=False,
                            write_disposition='WRITE_TRUNCATE',
                            gcp_conn_id='bigquery-composer-connection',
                            create_disposition='CREATE_IF_NEEDED',
                            retries= 3,
                            retry_delay=timedelta(seconds=30),
                            priority_weight=PRIORITY,
                            weight_rule=WeightRule.ABSOLUTE,
                            trigger_rule=TriggerRule.ALL_DONE
                        )
                        dedup_tasks.append(dedup)

                    else:
                        cluster_field = ['id'] if 'id' in etl.get_tables().get(table).get('schema').keys() else []
                        dedup = BigQueryExecuteQueryOperator(    
                        task_id=destination_table,
                        sql=f"""
                            with temp as (
                                select 
                                    id, 
                                    max(_ingested_ts) as max_ingested_ts 
                                from {DESTINATION_PROJECT}.{DESTINATION_DATASET}.{destination_table}_raw group by 1
                            )
                            select 
                                t.* 
                            from temp join 
                                {DESTINATION_PROJECT}.{DESTINATION_DATASET}.{destination_table}_raw t on temp.id= t.id and temp.max_ingested_ts=t._ingested_ts""",
                        destination_dataset_table=f"{DESTINATION_PROJECT}.{DEDUP_DESTINATION_DATASET}.{destination_table}",
                        cluster_fields=['id'],
                        time_partitioning={'field': 'created_at'},
                        use_legacy_sql=False,
                        write_disposition='WRITE_TRUNCATE',
                        gcp_conn_id='bigquery-composer-connection',
                        create_disposition='CREATE_IF_NEEDED',
                        retries= 3,
                        retry_delay=timedelta(seconds=30),
                        priority_weight=PRIORITY,
                        weight_rule=WeightRule.ABSOLUTE,
                        trigger_rule=TriggerRule.ALL_DONE
                        )
                        dedup_tasks.append(dedup)



                deduplication_success_confirmation = PythonOperator(
                    task_id='success_confirmation',
                    python_callable=etl.deduplication_success,
                    provide_context=True,
                    email_on_failure=True,
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                    priority_weight=PRIORITY,
                    weight_rule=WeightRule.ABSOLUTE
                )
            dedup_subgroups[DATABASE_NAME] = dedup_tg


            task_extract_tables >> task_no_missing_columns

            task_extract_tables >> task_upload_result_to_gcs

            task_upload_result_to_gcs >> run_dataflow_pipeline >> dedup_tasks

            dedup_tasks >> deduplication_success_confirmation
        etl_groups[g_id] = tg

    #etl_groups['main_etl'] >> [etl_tg for tg_name, etl_tg in etl_groups.items() if tg_name != 'main_etl']