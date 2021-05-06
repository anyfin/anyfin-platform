resource "google_bigquery_table" "accounts_raw" {
	dataset_id = "psd2_staging"
	table_id   = "accounts_raw"
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
		"name": "connection_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "external_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "provider_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "name",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "currency_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "balance",
		"type": "FLOAT"
	},
	{
		"mode": "NULLABLE",
		"name": "last_synced_at",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "backfilled_until",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "raw_data",
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


resource "google_bigquery_table" "psd2_connections_raw" {
	dataset_id = "psd2_staging"
	table_id   = "psd2_connections_raw"
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
		"name": "provider_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "status_payload",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "provider_data",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "access_token",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "refresh_token",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "refresh_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "expires_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "accounts_synced_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "transactions_synced_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "error",
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


resource "google_bigquery_table" "psd2_log_raw" {
	dataset_id = "psd2_staging"
	table_id   = "psd2_log_raw"
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
		"name": "request_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "connection_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "provider_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "activity",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "duration",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "info",
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


