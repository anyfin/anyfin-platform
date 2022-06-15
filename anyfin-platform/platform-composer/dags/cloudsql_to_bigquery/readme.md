# Welcome to the **future** of Anyfin ETL!

## How to setup a new ETL

- Add the new database name, with additional info, to the list in the file utils/db_info_utils.py

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

- Setup Connections and Credentials
    - Setup a port for the DB in the proxy at: `composer-cloudsql/platform_cloudsql_proxy.yaml`
        - Connect to cluster using `gcloud container clusters get-credentials europe-west1-platform-b746cda0-gke --region europe-west1 --project anyfin-platform`
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
- Create a PR, when you merge it everything will be deployed
- Before running the pipeline, make sure to create the two datasets in BigQuery - called [database_name]_staging [database_name]. **Set the data location to EU**

### You should now be able to run the pipeline
