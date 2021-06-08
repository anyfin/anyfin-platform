resource "google_bigquery_table" "users_raw" {
	dataset_id = "dolph_staging"
	table_id   = "users_raw"
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
		"name": "manual_income",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "settings",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "primary_account",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


resource "google_bigquery_table" "tink_integrations_raw" {
	dataset_id = "dolph_staging"
	table_id   = "tink_integrations_raw"
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
		"name": "tink_user_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "user_id",
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
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


resource "google_bigquery_table" "providers_raw" {
	dataset_id = "dolph_staging"
	table_id   = "providers_raw"
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
		"name": "name",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "display_name",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "last_sync_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "credential",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "metadata",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "user_id",
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
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


resource "google_bigquery_table" "accounts_raw" {
	dataset_id = "dolph_staging"
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
		"name": "user_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "account_number",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "available_credit",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "balance",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "name",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "currency_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "metadata",
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
		"name": "provider_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


resource "google_bigquery_table" "transactions" {
	dataset_id = "dolph"
	table_id   = "transactions"
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
		"name": "account_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "user_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "date",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "description",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "amount",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "currency_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "category",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "category_metadata",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "metadata",
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
		"name": "selected",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "recurring_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "tags",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "reference_id",
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


resource "google_bigquery_table" "recurrings_raw" {
	dataset_id = "dolph_staging"
	table_id   = "recurrings_raw"
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
		"name": "user_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "metadata",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "recurring_key",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "description",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "display_name",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "score",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "category",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "subcategory",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "amount",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "currency",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "selected_by_user",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "type",
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
		"name": "active",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


resource "google_bigquery_table" "cash_advance_applications_raw" {
	dataset_id = "dolph_staging"
	table_id   = "cash_advance_applications_raw"
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
		"name": "user_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "amount",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "currency",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "stage",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "underwriting",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "payout_id",
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
		"name": "reject_reason",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "error",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "loan_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "purpose",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "contract_file",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "pull_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


resource "google_bigquery_table" "payouts_raw" {
	dataset_id = "dolph_staging"
	table_id   = "payouts_raw"
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
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "amount",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "currency",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "user_id",
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
		"name": "error",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "ref_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "account_number",
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


resource "google_bigquery_table" "payins_raw" {
	dataset_id = "dolph_staging"
	table_id   = "payins_raw"
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
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "ref_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "amount",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "currency",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "user_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "error",
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
		"name": "cash_advance_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


