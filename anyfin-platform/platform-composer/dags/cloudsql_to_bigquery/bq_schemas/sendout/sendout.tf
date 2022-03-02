resource "google_bigquery_table" "sendouts_raw" {
	dataset_id = "sendout_staging"
	table_id   = "sendouts_raw"
	project    = "anyfin"

	labels = {
		env = "default"
	}

	time_partitioning { 
    	type = "DAY" 
		field = "created_at" 
	}

	schema = <<EOF
[
	{
		"mode": "NULLABLE",
		"name": "id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "public_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "created_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "updated_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "recipient",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "content",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "options",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "scheduled_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "sent_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "delivered_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "failed_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "retry_count",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "retry_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "key",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "channel_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "external_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "request_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "namespace",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


resource "google_bigquery_table" "sendout_events_raw" {
	dataset_id = "sendout_staging"
	table_id   = "sendout_events_raw"
	project    = "anyfin"

	labels = {
		env = "default"
	}

	time_partitioning { 
    	type = "DAY" 
		field = "created_at" 
	}

	schema = <<EOF
[
	{
		"mode": "NULLABLE",
		"name": "id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "created_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "updated_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "sendout_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "source",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "event",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "data",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


resource "google_bigquery_table" "sendout_settings_raw" {
	dataset_id = "sendout_staging"
	table_id   = "sendout_settings_raw"
	project    = "anyfin"

	labels = {
		env = "default"
	}

	time_partitioning { 
    	type = "DAY" 
		field = "created_at" 
	}

	schema = <<EOF
[
	{
		"mode": "NULLABLE",
		"name": "id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "created_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "updated_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "value",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


