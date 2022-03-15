resource "google_bigquery_table" "savings_customers_raw" {
	dataset_id = "savings_staging"
	table_id   = "savings_customers_raw"
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
		"name": "has_external_account",
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


resource "google_bigquery_table" "customer_triggers_raw" {
	dataset_id = "savings_staging"
	table_id   = "customer_triggers_raw"
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
		"name": "active",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "trigger_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "trigger_name",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "last_enabled_at",
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


resource "google_bigquery_table" "roundup_triggers_raw" {
	dataset_id = "savings_staging"
	table_id   = "roundup_triggers_raw"
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
		"name": "multiplier",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_trigger_id",
		"type": "INTEGER"
	},
	{
		"mode": "REPEATED",
		"name": "account_ids",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "last_scheduled_at",
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


resource "google_bigquery_table" "save_on_payday_triggers_raw" {
	dataset_id = "savings_staging"
	table_id   = "save_on_payday_triggers_raw"
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
		"name": "amount",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "monthly_pull_day",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_trigger_id",
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


resource "google_bigquery_table" "happy_monday_triggers_raw" {
	dataset_id = "savings_staging"
	table_id   = "happy_monday_triggers_raw"
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
		"name": "min_amount",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "max_amount",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_trigger_id",
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


resource "google_bigquery_table" "customer_events_raw" {
	dataset_id = "savings_staging"
	table_id   = "customer_events_raw"
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
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "event",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "metadata",
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


resource "google_bigquery_table" "savings_payouts_raw" {
	dataset_id = "savings_staging"
	table_id   = "savings_payouts_raw"
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
		"name": "amount",
		"type": "FLOAT"
	},
	{
		"mode": "NULLABLE",
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "transaction_rowkey",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "country_code",
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
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


resource "google_bigquery_table" "promo_codes_raw" {
	dataset_id = "savings_staging"
	table_id   = "promo_codes_raw"
	project    = "anyfin"

	labels = {
		env = "default"
	}

	time_partitioning { 
    	type = "DAY" 
		field = "updated_at" 
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
		"name": "start_date",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "expiry_date",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "amount",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "max_applicants",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "channel",
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


resource "google_bigquery_table" "user_stats_raw" {
	dataset_id = "savings_staging"
	table_id   = "user_stats_raw"
	project    = "anyfin"

	labels = {
		env = "default"
	}

	time_partitioning { 
    	type = "DAY" 
		field = "updated_at" 
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
		"name": "week",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "month",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "year",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "deposit",
		"type": "FLOAT"
	},
	{
		"mode": "NULLABLE",
		"name": "withdrawal",
		"type": "FLOAT"
	},
	{
		"mode": "NULLABLE",
		"name": "triggers",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "bucket_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "pot_id",
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


resource "google_bigquery_table" "customer_external_facts_raw" {
	dataset_id = "savings_staging"
	table_id   = "customer_external_facts_raw"
	project    = "anyfin"

	labels = {
		env = "default"
	}

	time_partitioning { 
    	type = "DAY" 
		field = "updated_at" 
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
		"name": "external_customer_created",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "kyc_answered",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "external_account_created",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "direct_debit_mandate_created",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "direct_debit_mandate_status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "external_account_created_at",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "mandate_created_at",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "mandate_established_at",
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


resource "google_bigquery_table" "savings_referrals_raw" {
	dataset_id = "savings_staging"
	table_id   = "savings_referrals_raw"
	project    = "anyfin"

	labels = {
		env = "default"
	}

	time_partitioning { 
    	type = "DAY" 
		field = "updated_at" 
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
		"name": "referral_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "referral_context",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "referrer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "referrer_payout_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "referent_payout_id",
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


resource "google_bigquery_table" "promo_code_usages_raw" {
	dataset_id = "savings_staging"
	table_id   = "promo_code_usages_raw"
	project    = "anyfin"

	labels = {
		env = "default"
	}

	time_partitioning { 
    	type = "DAY" 
		field = "updated_at" 
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
		"name": "promo_code_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "payout_id",
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


resource "google_bigquery_table" "savings_bonuses_raw" {
	dataset_id = "savings_staging"
	table_id   = "savings_bonuses_raw"
	project    = "anyfin"

	labels = {
		env = "default"
	}

	time_partitioning { 
    	type = "DAY" 
		field = "updated_at" 
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
		"name": "bonus_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "bonus_description",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "admin_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "payout_id",
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


