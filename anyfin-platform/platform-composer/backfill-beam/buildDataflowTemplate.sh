# Run below with arguments, under the /backfill-beam directory 
# $1 - main.json    | This will be the credentials file containing: {"username": <>, "password": <> ,"location": <>}
#                     for the DB. location is the DB URI, i.e. "jdbc:postgresql://<url>:<port>/<db>"
# example command
# sh buildDataflowTemplate.sh main.json
CREDENTIALS=$1

# TABLES - An array with all the tables to create templates for
# The format of each entry should be:
# database name (e.g. main)
# table name (e.g. transactions)
# querying by offset or created_at timestamp (offset will be default here)
# step size, which will be the number of rows to fetch with every query if offset is used (50000 will be default). 
#                                                               If we don't query by offset any value will be fine
# (optional) start date - By default, if we query by timestamp, first date to query for will be the 2017-10-26. 
# entry examples:
# "main|cycles|offset|100000"
# "main|transactions|timestamp|1|2017-10-10"
TABLES=(
    "main|messages|offset|100000"
    "main|cycles|offset|500000"
    "main|assessments|offset|5000"
    "main|transactions|timestamp|1"
)

for ENTRY in "${TABLES[@]}"
do
    DB="$(echo $ENTRY | cut -d '|' -f 1)"
    TABLE_NAME="$(echo $ENTRY | cut -d '|' -f 2)"
    QUERY_BY="$(echo $ENTRY | cut -d '|' -f 3)"
    STEP_SIZE="$(echo $ENTRY | cut -d '|' -f 4)"
    START_DATE="$(echo $ENTRY | cut -d '|' -f 5)"
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
                --queryBy=$QUERY_BY \
                --gcpTempLocation=gs://sql-to-bq-etl/beam_backfill/Temp/ \
                --runner=DataflowRunner \
                --credentialsFile=$CREDENTIALS \
                --region=europe-west1 $START_DATE" \
    -Pdataflow-runner
done