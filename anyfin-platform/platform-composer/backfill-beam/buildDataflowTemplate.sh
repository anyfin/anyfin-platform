# Run below with arguments, under the /backfill-beam directory 
# $1 - main/...,    | This will be used to tag the tempate name according to the credentials that were used to build
# $2 - main.json    | This will be the credentials file containing: {"username": <>, "password": <> ,"location": <>}
#                     for the DB. location is the DB URI, i.e. "jdbc:postgresql://<url>:<port>/<db>"
# example command
# sh buildDataflowTemplate.sh daily main main.json
DB=$1
CREDENTIALS=$2

TABLES=(
    "messages:50000"
    "cycles:100000"
    "assessments:2000"
)

for ENTRY in "${TABLES[@]}"
do
    TABLE_NAME="${ENTRY%%:*}"
    STEP_SIZE="${ENTRY##*:}"
	mvn compile exec:java \
    -Dexec.mainClass=org.anyfin.ReadJdbc \
    -Dexec.args="--project=anyfin \
                --tempLocation=gs://sql-to-bq-etl/beam_backfill/Temp/ \
                --stagingLocation=gs://sql-to-bq-etl/beam_backfill/Staging/ \
                --templateLocation=gs://sql-to-bq-etl/beam_templates/postgres-backfill-$DB-$TABLE_NAME \
                --sourceTable=$TABLE_NAME \
                --stepSize=$STEP_SIZE \
                --gcpTempLocation=gs://sql-to-bq-etl/beam_backfill/Temp/ \
                --runner=DataflowRunner \
                --credentialsFile=$CREDENTIALS \
                --region=europe-west1" \
    -Pdataflow-runner
done

