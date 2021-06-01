resource "google_bigquery_table" "balance_widgets_raw" {
	dataset_id = "pfm_staging"
	table_id   = "balance_widgets_raw"
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
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "title",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "color",
		"type": "STRING"
	},
	{
		"mode": "REPEATED",
		"name": "account_ids",
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


