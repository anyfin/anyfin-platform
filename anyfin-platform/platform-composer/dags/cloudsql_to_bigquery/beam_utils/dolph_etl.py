"""
Should be deployed under the composer's dags/cloudsql_to_bigquery directory
e.g.
gs://europe-west1-production-com-369e4b80-bucket/dags/cloudsql_to_bigquery
"""

import argparse
from datetime import datetime, timedelta
import logging
import json
import os
import apache_beam as beam
from airflow.models import Variable
from apache_beam.options.pipeline_options import PipelineOptions

PROJECT_NAME = 'anyfin'
GCS_BUCKET = 'sql-to-bq-etl'
POSTGRES_CREDENTIALS = Variable.get("dolph_postgres_bq_secret", deserialize_json=True)


with open(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'pg_schemas/dolph_schemas_state.json'))) as f:
    TABLES = json.loads(f.read())

# Class for loading Postgres data in Dataflow
class QueryPostgresFn(beam.DoFn):

    def __init__(self, **server_config):
        self.config = server_config

    def process(self, query):
        import psycopg2
        con = psycopg2.connect(**self.config)
        cur = con.cursor()
        cur.execute(query)
        return cur.fetchall()


# Parsing Postgres timestamps into BQ format
class ParseTimestampsFn(beam.DoFn):

    def __init__(self, schema):
        self.schema = schema

    def process(self, row):
        from datetime import datetime, date, time
        row = list(row)

        for i, x in enumerate(row):
            if isinstance(x, datetime):
                row[i] = str(x.isoformat())
            if isinstance(x, dict):
                row[i] = json.dumps(x)
            if isinstance(x, bytes):
                # on python 3 base64-encoded bytes are decoded to strings
                # before being sent to BigQuery
                row[i] = x.decode('utf-8')
            if isinstance(x, (date, time)):
                row[i] = str(x)
        row = dict(zip(self.schema, row))
        row['_ingested_ts'] = str(datetime.now().isoformat())
        yield row


# Automatically update the beam pipeline with more tables
def update_pipeline(table, pipeline, backfill, start_date, end_date):
    schema = [list(s.get('schema')) for t, s in TABLES.items() if t == table][0]
    ts_column = [s.get('ts_column') for t, s in TABLES.items() if t == table][0]
    bq_partition_column = [s.get('bq_partition_column') for t, s in TABLES.items() if t == table][0]

    if backfill:
        where_clause = f" where {ts_column} >= {start_date} and {ts_column} < {end_date} " if ts_column else ""
        write_disposition = beam.io.BigQueryDisposition.WRITE_TRUNCATE
        partitioning = {'timePartitioning': {'type': 'DAY', 'field': f'{bq_partition_column}'}} if ts_column else {}
    else:
        where_clause = f" where {ts_column} >= {start_date}  " if ts_column else ""
        write_disposition = beam.io.BigQueryDisposition.WRITE_APPEND if ts_column else beam.io.BigQueryDisposition.WRITE_TRUNCATE
        partitioning = {'timePartitioning': {'type': 'DAY', 'field': f'{bq_partition_column}'}} if ts_column else {}

    print(f'select {", ".join(schema)} from {table}{where_clause}')

    return (
        pipeline
        | f'Create query {table}' >> beam.Create([f'select {", ".join(schema)} from {table}{where_clause}'])
        | f'Fetch data from DB - {table}' >> beam.ParDo(QueryPostgresFn(host=POSTGRES_CREDENTIALS.get('host'),
                                                                        user=POSTGRES_CREDENTIALS.get('user'),
                                                                        dbname=POSTGRES_CREDENTIALS.get('dbname'),
                                                                        password=POSTGRES_CREDENTIALS.get('password')))
        # | f'Reshuffle data - {table}' >> beam.Reshuffle()
        | f'Parsing timestamps - {table}' >> beam.ParDo(ParseTimestampsFn(schema))
        | f'Write to BQ {table}' >> beam.io.gcp.bigquery.WriteToBigQuery(table=table + "_raw",
                                                                         dataset='dolph_staging',
                                                                         additional_bq_parameters=partitioning,
                                                                         method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                                                                         project=PROJECT_NAME,
                                                                         create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                                                                         write_disposition=write_disposition)
    )


def run(extract_tables, backfill, start_date, end_date, pipeline_args=None):
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=False
    )
    with beam.Pipeline(options=pipeline_options) as pipeline:
        for table in extract_tables:
            p = update_pipeline(table, pipeline, backfill, start_date, end_date)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--extract_tables",
        default=['transactions'],
    )
    parser.add_argument(
        "--backfill",
        default=False
    )
    parser.add_argument(
        "--num_of_days",
        default=0
    )
    parser.add_argument(
        "--date",
        default='1970-01-01',
    )
    known_args, pipeline_args = parser.parse_known_args()

    beginning_date = datetime.strptime(known_args.date, '%Y-%m-%d')
    incremental_start_date = beginning_date.strftime("'%Y-%m-%d %H:%M:%S.000000+00'")
    incremental_end_date = (beginning_date + timedelta(days=1)).strftime("'%Y-%m-%d %H:%M:%S.000000+00'")

    # Backfill, remember to run this with DirectRunner
    if known_args.backfill and known_args.backfill.lower() == 'true' and known_args.date and known_args.num_of_days:

        start_date = beginning_date.strftime("'%Y-%m-%d %H:%M:%S.000000+00'")
        end_date = (beginning_date + timedelta(days=int(known_args.num_of_days))).strftime("'%Y-%m-%d %H:%M:%S.000000+00'")

        known_args.start_date = start_date
        known_args.end_date = end_date

        run(
            known_args.extract_tables,
            known_args.backfill,
            known_args.start_date,
            known_args.end_date,
            pipeline_args
        )
    else:
        run(
            ['users', 'tink_integrations', 'providers', 'accounts', 'recurrings',
             'cash_advance_applications', 'payouts', 'payins'],
            False,
            incremental_start_date,
            incremental_end_date,
            pipeline_args
        )
