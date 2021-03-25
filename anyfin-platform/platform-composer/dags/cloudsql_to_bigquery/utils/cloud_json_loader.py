import json
from google.cloud import storage

GCS_BUCKET = 'sql-bq-etl'

# Load previous state from GCS
def cloud_json_loader(gs_schema_path):
    client = storage.Client(project='anyfin-platform')
    bucket = client.get_bucket(GCS_BUCKET)
    blob = bucket.get_blob(gs_schema_path)
    blob = blob.download_as_string()
    blob = blob.decode('utf-8')
    bucket_json_schemata = json.loads(blob)
    return bucket_json_schemata
