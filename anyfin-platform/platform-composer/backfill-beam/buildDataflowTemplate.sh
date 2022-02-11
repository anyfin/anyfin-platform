# Run below with arguments, under the /backfill-beam directory 
# $1 - main/...,    | This will be used to tag the tempate name according to the credentials that were used to build
# $2 - daily/full,  | This will either create one query per day, or a select * query for the full scan
# $3 - main.json    | This will be the credentials file containing: {"username": <>, "password": <> ,"location": <>}
#                     for the DB. location is the DB URI, i.e. "jdbc:postgresql://<url>:<port>/<db>"
# example command
# sh buildDataflowTemplate.sh daily main main.json
DB=$1
QUERY_BREAKDOWN=$2
CREDENTIALS=$3

TABLES=( offers messages assessments signatures)

for TABLE_NAME in "${TABLES[@]}"
do
	mvn compile exec:java \
    -Dexec.mainClass=org.anyfin.ReadJdbc \
    -Dexec.args="--project=anyfin \
                --tempLocation=gs://sql-to-bq-etl/beam_backfill/Temp/ \
                --stagingLocation=gs://sql-to-bq-etl/beam_backfill/Staging/ \
                --templateLocation=gs://sql-to-bq-etl/beam_templates/postgres-backfill-$QUERY_BREAKDOWN-$DB-$TABLE_NAME \
                --sourceTable=$TABLE_NAME \
                --gcpTempLocation=gs://sql-to-bq-etl/beam_backfill/Temp/ \
                --runner=DataflowRunner \
                --credentialsFile=$CREDENTIALS \
                --queryBreakdown=$QUERY_BREAKDOWN \
                --region=europe-west1" \
    -Pdataflow-runner
done

