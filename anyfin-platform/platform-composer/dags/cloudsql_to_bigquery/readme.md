# Welcome to the **future** of Anyfin ETL!

## How to setup a new ETL

- Create DAG file
    - Duplicate one of the [database_name]_postgres_bq_etl.py dags and name it accordingly
    - Edit the global variable **DATABASE_NAME** to the name of the postgres database you want to transfer tables from
    - Deploy this file in the Platform Composer dags/cloudsql_to_bigquery folder in GCP
- Create PG Schema file
    - Follow the format of the other [database_name]_schemas_state.json files
        ```python
        {
            "table_name": {
                # Required
                "schema": {
                    "field": "type", 
                    "field": "type",  
                },
                # Optional
                "ts_column": "updated_at",           # Fetch daily data based on the ts_column - Remove row to fetch all data every day
                "bq_partition_column": "created_at", # The column to partition on in BigQuery
                "backfill_method": "beam_backfill",    # Valid options ["direct_export", "nested_export", "beam_backfill"]
                "chunk_size": "50000",               # Keep this row if backfill_method = "Export"
                "ignore_sanity_check": true,         # Add this to remove a table from the sanity check
                "ignore_daily": true                 # Add this to exclude table from loading daily data
            },
            {
            "table_name": ....

        }
        ```
    - To fetch the Postgres schema for a table use: 
        ```SQL 
        SELECT
            table_name,
            string_agg(
                '"' || column_name::text ||  '":"' || data_type::text || '"', ', 
                '
                ) as columns
        FROM  information_schema.columns
        WHERE table_name = [table name string]
        GROUP BY table_name;
        ```
    - Using the format in step one, create a json object for each table and save the file as [database_name]_schemas_state.json under pg_schemas folder
    - Deploy in the Platform Composer dags/cloudsql_to_bigquery/pg_schemas in GCP

- Create BQ Schema using Terraform
    - Inside BQ Create a dataset called [database_name]_staging under the appropriate GCP project (Make sure to set data location to EU)
    - Navigate to your local platform-composer/dags/cloudsql_to_bigquery folder
    - Run `mkdir bq_schemas/[database_name]` 
    - Run `python3 pg_schemas/json-schema-to-terraform.py [database_name]` This creates a terraform based on the PG schema and puts it in the bq_schema folder
    - Navigate to bq_schemas/[database_name] and run `terraform init` and then `terraform apply`


- Setup Connections and Credentials
    - Setup a port for the DB in the proxy at: `composer-cloudsql/platform_cloudsql_proxy.yaml`
        - Deploy using: `kubectl apply -f platform_cloudsql_proxy.yaml`
    - Create a connection in Airflow called [database_name]_replica
        - Set host to: `cloudsql-proxy.default.svc.cluster.local` and the port to the one you just set up in the proxy
    - Create an Airflow Variable called [database_name]_postgres_bq_secret and use the following value format
        ```JSON
        {
            "host": "[private_ip]",
            "dbname": "",
            "user":  "",
            "password": ""
        } 
        ```
- Additional
    - If you have added any tables to the beam_backfill you need to add the database details in the postgres_bq_backfill.py file
    - If you have added any tables to the export_backfill you need to add db details to the export_bq_backfil.py file in the same manner

### You should now be able to run the pipeline
