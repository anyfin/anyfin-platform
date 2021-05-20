resource "google_bigquery_table" "ddi_sessions_raw" {
	dataset_id = "ddi_staging"
	table_id   = "ddi_sessions_raw"
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
		"name": "provider",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "details",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "items",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "metadata",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "platform",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "user_ip",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "user_agent",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "user_identifier",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "metrics",
		"type": "STRING"
	},
	{
		"mode": "REPEATED",
		"name": "decision_tree",
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


