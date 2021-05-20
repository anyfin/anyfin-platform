import argparse
from datetime import datetime, timedelta
import logging
import json
import os
import apache_beam as beam
from airflow.models import Variable
from apache_beam.options.pipeline_options import PipelineOptions

DATABASE_NAME = ''
PROJECT_NAME = 'anyfin'
POSTGRES_CREDENTIALS = ''
TABLES = {}

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


# Parsing columns (pg data types to BQ friendly format)
class ParseColumnsFn(beam.DoFn):

    def __init__(self, schema):
        self.schema = schema

    def process(self, row):
        from datetime import datetime, date, time
        row = list(row)

        for i, x in enumerate(row):
            if isinstance(x, datetime):
                row[i] = str(x.isoformat())
            if isinstance(x, list) and all(isinstance(el, dict) for el in x) and list(self.schema.values())[i] != "ARRAY":
                row[i] = str(", ".join(json.dumps(el) for el in x))
            if isinstance(x, dict):
                row[i] = json.dumps(x)
            if list(self.schema.values())[i] == "ARRAY":
                row[i] = [str(el) for el in row[i]] if isinstance(row[i], list) else row[i]
            if isinstance(x, bytes):
                # on python 3 base64-encoded bytes are decoded to strings
                # before being sent to BigQuery
                row[i] = x.decode('utf-8')
            if isinstance(x, (date, time)):
                row[i] = str(x)

        schema = [s.replace('"', '') if s == '"from"' else s for s in self.schema]
        # schema = [s.replace('"', '') for s in self.schema]  ## More general approach
        row = dict(zip(schema, row))
        row['_ingested_ts'] = str(datetime.now().isoformat())
        yield row


# Automatically update the beam pipeline with more tables
def update_pipeline(table, pipeline, start_date, backfill):
    schema = [dict(s.get('schema')) for t, s in TABLES.items() if t == table][0]
    ts_column = [s.get('ts_column') for t, s in TABLES.items() if t == table][0]
    bq_partition_column = [s.get('bq_partition_column') for t, s in TABLES.items() if t == table][0]
    where_clause = ""

    if backfill:
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
        | f'Parsing columns - {table}' >> beam.ParDo(ParseColumnsFn(schema))
        | f'Write to BQ {table}' >> beam.io.gcp.bigquery.WriteToBigQuery(table=table + "_raw",
                                                                         dataset=f'{DATABASE_NAME}_staging',
                                                                         additional_bq_parameters=partitioning,
                                                                         method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                                                                         project=PROJECT_NAME,
                                                                         create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                                                                         write_disposition=write_disposition)
    )


def run(backfill, start_date, pipeline_args=None):
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=False
    )
    TABLE_NAMES = list(TABLES.keys())
    with beam.Pipeline(options=pipeline_options) as pipeline:
        if backfill:
            load_tables = [table for table in TABLE_NAMES if TABLES.get(table).get('backfill_method') == 'beam_backfill']
        else:
            load_tables = TABLE_NAMES

        for table in load_tables:
            if not TABLES.get(table).get('ignore_daily'):
                p = update_pipeline(table, pipeline, start_date, backfill)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--backfill",
        default=False
    )
    parser.add_argument(
        "--database_name",
        default=None
    )
    parser.add_argument(
        "--date",
        default='1970-01-01',
    )
    known_args, pipeline_args = parser.parse_known_args()

    beginning_date = datetime.strptime(known_args.date, '%Y-%m-%d')
    incremental_start_date = beginning_date.strftime("'%Y-%m-%d %H:%M:%S.000000+00'")

    # Initialize global variables
    DATABASE_NAME = known_args.database_name
    POSTGRES_CREDENTIALS = Variable.get(f"{DATABASE_NAME}_postgres_bq_secret", deserialize_json=True)
    with open(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', f'pg_schemas/{DATABASE_NAME}_schemas_state.json'))) as f:
        TABLES = json.loads(f.read())


    # Backfill, remember to run this with DirectRunner
    if known_args.backfill and known_args.backfill.lower() == 'true':
        # start_date = beginning_date.strftime("'%Y-%m-%d %H:%M:%S.000000+00'")
        # known_args.start_date = start_date

        run(
            True,
            known_args.date,
            pipeline_args
        )
    else:
        run(
            False,
            incremental_start_date,
            pipeline_args
        )