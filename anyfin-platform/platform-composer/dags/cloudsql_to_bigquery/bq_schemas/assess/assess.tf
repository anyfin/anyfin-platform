resource "google_bigquery_table" "lookups_raw" {
	dataset_id = "assess_staging"
	table_id   = "lookups_raw"
	project    = "anyfin"

	labels = {
		env = "default"
	}

	time_partitioning { 
    	type = "DAY" 
		field = "ts" 
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
		"name": "namespace",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "key",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "data",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "ts",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


