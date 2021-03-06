## GDPR data request

This DAG automates the extraction of data on a specific customer and the construction of a report containing that data.

It is not scheduled on a given interval but is build to be run manually with configuration. This can be done programmatically, or through the Airflow UI by triggering it with configuration where a JSON Configuration must be provided. The JSON object should look like this:

```
{
    "customer_id":"1234567890"
}
```

Trigger the DAG run. After it has finished successfully the report can be found [here](https://console.cloud.google.com/storage/browser/anyfin-platform-gdpr/reports)