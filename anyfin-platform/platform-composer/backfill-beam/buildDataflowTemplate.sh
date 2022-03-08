#!/bin/bash
# Run below with arguments, under the /backfill-beam directory 
# $1 - main.json    | This will be the credentials file containing: {"username": <>, "password": <> ,"location": <>}
#                     for the DB. location is the DB URI, i.e. "jdbc:postgresql://<url>:<port>/<db>"
# $2 - main         | This will be the database for which we need to build the templates. Information is fetched from
#                     the respective _schemas_state.json file (e.g. for main that will be main_schemas_state.json)
# example command
# sh buildDataflowTemplate.sh main.json main
CREDENTIALS=$1
DB=$2
START_DATE=$3

# TABLES - An array with all the tables to create templates for
# The format of each entry should be:
# database name (e.g. main)
# table name (e.g. transactions)
# querying by offset or created_at timestamp (offset will be default here)
# step size, which will be the number of rows to fetch with every query if offset is used (50000 will be default). 
#                                                               If we don't query by offset any value will be fine
# (optional) start date - By default, if we query by timestamp, first date to query for will be the 2017-10-26. 
# entry examples:
# "main|cycles|offset|100000|created_at"
# "main|transactions|timestamp|1|_"

# NOTE: To run the script below make sure you have installed jq command-line JSON processor in your linux/macOS environment

echo "Fetching DB configuration."
TABLES=()
schemas_file="../dags/cloudsql_to_bigquery/pg_schemas/${DB}_schemas_state.json"
while read table;
do
    backfill_method=$(cat ${schemas_file} | jq ".${table}.backfill_method")
    if [[ $backfill_method == "\"beam_export\"" ]]; then 
        query_by=$(cat ${schemas_file} | jq ".${table}.query_by")
        step_size=$(cat ${schemas_file} | jq ".${table}.step_size")
        ordered_by=$(cat ${schemas_file} | jq ".${table}.ordered_by")
        table_entry="$DB|$table|$query_by|$step_size|$ordered_by"
        table_entry_cleared=$(echo $table_entry | tr -d \")
        TABLES+=("$table_entry_cleared")
    fi
done < <(cat ${schemas_file} | jq 'keys[]')
echo "DB configuration fetched. Building the templates."

for ENTRY in "${TABLES[@]}"
do
    echo $ENTRY
    DB="$(echo $ENTRY | cut -d '|' -f 1)"
    TABLE_NAME="$(echo $ENTRY | cut -d '|' -f 2)"
    QUERY_BY="$(echo $ENTRY | cut -d '|' -f 3)"
    STEP_SIZE="$(echo $ENTRY | cut -d '|' -f 4)"
    ORDERED_BY="$(echo $ENTRY | cut -d '|' -f 5)"
    if [ ! -z "$START_DATE" ]; then START_DATE="--startDate=$START_DATE"; fi
    echo $START_DATE
	mvn compile exec:java \
    -Dexec.mainClass=org.anyfin.ReadJdbc \
    -Dexec.args="--project=anyfin \
                --tempLocation=gs://sql-to-bq-etl/beam_backfill/Temp/ \
                --stagingLocation=gs://sql-to-bq-etl/beam_backfill/Staging/ \
                --templateLocation=gs://sql-to-bq-etl/beam_templates/postgres-backfill-$DB-$TABLE_NAME \
                --sourceTable=$TABLE_NAME \
                --stepSize=$STEP_SIZE \
                --orderedBy=$ORDERED_BY \
                --queryBy=$QUERY_BY \
                --dbName=$DB \
                --gcpTempLocation=gs://sql-to-bq-etl/beam_backfill/Temp/ \
                --runner=DataflowRunner \
                --credentialsFile=$CREDENTIALS \
                --region=europe-west1 $START_DATE" \
    -Pdataflow-runner
done