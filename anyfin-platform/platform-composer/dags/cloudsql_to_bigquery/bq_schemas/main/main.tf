resource "google_bigquery_table" "countries_raw" {
	dataset_id = "main_staging"
	table_id   = "countries_raw"
	project    = "anyfin"

	labels = {
		env = "default"
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
		"name": "code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "default_currency_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "default_language",
		"type": "STRING"
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


resource "google_bigquery_table" "creditors_raw" {
	dataset_id = "main_staging"
	table_id   = "creditors_raw"
	project    = "anyfin"

	labels = {
		env = "default"
	}

	schema = <<EOF
[
	{
		"mode": "NULLABLE",
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "primary_payment_account_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "currency_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "country_code",
		"type": "STRING"
	},
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
		"name": "name",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "terms_url",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "bgc_account",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "display_name",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "organisation_number",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "sign_autogiro",
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


resource "google_bigquery_table" "currencies_raw" {
	dataset_id = "main_staging"
	table_id   = "currencies_raw"
	project    = "anyfin"

	labels = {
		env = "default"
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
		"name": "code",
		"type": "STRING"
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


resource "google_bigquery_table" "payment_accounts_raw" {
	dataset_id = "main_staging"
	table_id   = "payment_accounts_raw"
	project    = "anyfin"

	labels = {
		env = "default"
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
		"name": "account_number",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "currency",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "company",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "description",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "underlying_account_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "creditor_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "bic",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "changelog",
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


resource "google_bigquery_table" "assessments_raw" {
	dataset_id = "main_staging"
	table_id   = "assessments_raw"
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
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "application_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "main_policy",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "interest_rate",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "monthly_fee",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "reviewed_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "reviewed_by",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "reviewed_comment",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "final_policy",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "amortization_rate",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "external_score",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "score",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "external_lookup_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "external_lookup_provider",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "updated_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "creditor_id",
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


resource "google_bigquery_table" "applications_raw" {
	dataset_id = "main_staging"
	table_id   = "applications_raw"
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
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "step",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "reject_reason",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "image_url",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "message_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "external_statement_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "assessment_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "offer_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "loan_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "submission_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "assigned_to",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "assigned_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "channel_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "referrer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "affiliate",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "promo_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "ddi_session_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "metadata",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "transfer_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "completion_sendout",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "country_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "old_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "currency_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "is_demo",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "source",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_kalp_input_id",
		"type": "STRING"
	},
	{
		"mode": "REPEATED",
		"name": "reject_tags",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "has_confidential_data",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "updated_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_provided",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "creditor_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "rejected_by",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "sub_product",
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


resource "google_bigquery_table" "loans_raw" {
	dataset_id = "main_staging"
	table_id   = "loans_raw"
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
		"name": "original_months",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "monthly_fee",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "cycle_end_day",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "start_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "lender_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "source_loan_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "currency_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "country_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "balance",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "signature_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "interest_rate",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "acquisition_details",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "end_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "creditor_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "months",
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
		"name": "application_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "original_amount",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "amortization_rate",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "contract_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "dpd",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "last_processed",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "contract_sent_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "sub_product",
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


resource "google_bigquery_table" "customers_raw" {
	dataset_id = "main_staging"
	table_id   = "customers_raw"
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
		"name": "termination_date",
		"type": "DATE"
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
		"name": "id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "primary_channel_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "referrer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "reminded_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "trustpilot_invitation_sent_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "termination_report_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "autogiro_authorization_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "creditor_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "birthdate",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "candidate_for_creditor_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "originated_at",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "ocr",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "autogiro_payment_plan_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "full_name",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "smooch_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "address_street",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "address_postcode",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "address_city",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "first_name",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "last_name",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "offers_token",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "referral_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "country_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "gender",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "currency_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "language_preference",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "intercom_user_id",
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


resource "google_bigquery_table" "cycles_raw" {
	dataset_id = "main_staging"
	table_id   = "cycles_raw"
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
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "due_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "updated_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "loan_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "used_payment_vacation_at",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "dpd",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "balance",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "start_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "old_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "created_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "end_date",
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


resource "google_bigquery_table" "ddi_providers_raw" {
	dataset_id = "main_staging"
	table_id   = "ddi_providers_raw"
	project    = "anyfin"

	labels = {
		env = "default"
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
		"name": "order",
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
		"name": "name",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "auth",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "lender_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "country_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "min_android_app_version",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "min_ios_app_version",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "active_android",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "active_ios",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "active_web",
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


resource "google_bigquery_table" "offers_raw" {
	dataset_id = "main_staging"
	table_id   = "offers_raw"
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
		"name": "short_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "created_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "sign_result",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "signed_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "application_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "assessment_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "amount",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "interest_rate",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "monthly_fee",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "months",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "pricing_months",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "financing_cost",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "saved_amount",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "monthly_payment",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "effective_apr",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "old_interest_rate",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "old_monthly_fee",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "old_financing_cost",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "amortization_rate",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "lender_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "signature_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "updated_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "old_effective_apr",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "creditor_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "original_months",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "original_monthly_payment",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


resource "google_bigquery_table" "external_statements_raw" {
	dataset_id = "main_staging"
	table_id   = "external_statements_raw"
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
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "application_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "image_url",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "engine_result",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "lender_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "personal_identifier",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_name",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_street",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_postal",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "loan_type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "loan_balance",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "loan_months",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "loan_interest",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "loan_fee",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "payment_due_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "payment_reference",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "payment_account",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "payment_amount",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "ddi_result",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "has_reminder_fees",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "updated_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "is_personal_payment_account",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_birthdate",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_gender",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_city",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "assessment_overrides",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "creditor_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "loan_credit_limit",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "amortization_rate",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


resource "google_bigquery_table" "transactions_raw" {
	dataset_id = "main_staging"
	table_id   = "transactions_raw"
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
		"name": "source",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "comment",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "created_by",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "type",
		"type": "STRING"
	},
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
		"name": "loan_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "accrual_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "amount",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "old_cycle_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "statement_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "metadata",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "cycle_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "creditor_id",
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


resource "google_bigquery_table" "payments_raw" {
	dataset_id = "main_staging"
	table_id   = "payments_raw"
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
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "source",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "amount",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "deposit_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "reference",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "name",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "address",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "postal",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "city",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "country",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "deposit_account",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "updated_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "updated_by",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "source_data",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "currency_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "payer_identifier",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "product",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "category",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "loan_id",
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


resource "google_bigquery_table" "lenders_raw" {
	dataset_id = "main_staging"
	table_id   = "lenders_raw"
	project    = "anyfin"

	labels = {
		env = "default"
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
		"name": "name",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "default_payment_account",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "country_code",
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
		"name": "brand_attributes",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "duplicate",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "has_branches",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "available_loan_products",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "default_reciever_name",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "has_multiple_product_flows",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "sub_product",
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


resource "google_bigquery_table" "customer_kalp_input_raw" {
	dataset_id = "main_staging"
	table_id   = "customer_kalp_input_raw"
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
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "submission_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "number_of_kids",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "has_spouse",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "accommodation_arrangement",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "created_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "rent_monthly",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "debt_monthly",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "version",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "updated_at",
		"type": "TIMESTAMP"
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
		"name": "source_of_income",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "income_gross_monthly",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "income_net_monthly",
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


resource "google_bigquery_table" "customer_identities_raw" {
	dataset_id = "main_staging"
	table_id   = "customer_identities_raw"
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
		"name": "verified",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "country_code",
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
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


resource "google_bigquery_table" "channels_raw" {
	dataset_id = "main_staging"
	table_id   = "channels_raw"
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
		"name": "is_active",
		"type": "BOOLEAN"
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
		"name": "smooch_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "metadata",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "primary_customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "slack_channel_id",
		"type": "STRING"
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


resource "google_bigquery_table" "credit_block_applications_raw" {
	dataset_id = "main_staging"
	table_id   = "credit_block_applications_raw"
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
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "reject_reason",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "lender_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "signature_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "poa_file",
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


resource "google_bigquery_table" "signatures_raw" {
	dataset_id = "main_staging"
	table_id   = "signatures_raw"
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
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "content",
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
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "intent",
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


resource "google_bigquery_table" "messages_raw" {
	dataset_id = "main_staging"
	table_id   = "messages_raw"
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
		"name": "direction",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "smooch_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "from",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "text",
		"type": "STRING"
	},
	{
		"mode": "REPEATED",
		"name": "file_urls",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "metadata",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "channel_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "payload",
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


resource "google_bigquery_table" "customer_alarms_raw" {
	dataset_id = "main_staging"
	table_id   = "customer_alarms_raw"
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
		"name": "description",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "data",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "country_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "assigned_to_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "assigned_at",
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


resource "google_bigquery_table" "customer_attributes_raw" {
	dataset_id = "main_staging"
	table_id   = "customer_attributes_raw"
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
		"name": "start_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "key",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "value",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "source",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "admin_id",
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


resource "google_bigquery_table" "statements_raw" {
	dataset_id = "main_staging"
	table_id   = "statements_raw"
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
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "prev_balance",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "sum_payments",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "added_principal",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "sum_interest",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "sum_fees",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "sum_penalties",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "next_balance",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "min_payment",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "min_amortization",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "ocr_number",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "due_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "sum_discounts",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "public_url",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "s3_key",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "sent_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "dpd",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "payment_vacation_month",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "payment_method",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "payment_plan_pull_amount",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "currency_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "creditor_migration",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "country_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "updated_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "min_electricity_payment",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "added_electricity_principal",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "sum_electricity_payments",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


resource "google_bigquery_table" "custom_terms_raw" {
	dataset_id = "main_staging"
	table_id   = "custom_terms_raw"
	project    = "anyfin"

	labels = {
		env = "default"
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
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "created_by",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "start_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "end_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "terminated_at",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "comment",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "attributes",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "admin_id",
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


resource "google_bigquery_table" "customer_discounts_raw" {
	dataset_id = "main_staging"
	table_id   = "customer_discounts_raw"
	project    = "anyfin"

	labels = {
		env = "default"
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
		"name": "currency_code",
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


resource "google_bigquery_table" "demo_accounts_raw" {
	dataset_id = "main_staging"
	table_id   = "demo_accounts_raw"
	project    = "anyfin"

	labels = {
		env = "default"
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
		"name": "description",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "settings",
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
	dataset_id = "main_staging"
	table_id   = "promo_codes_raw"
	project    = "anyfin"

	labels = {
		env = "default"
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
		"name": "currency_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "country_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "start_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "end_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "description",
		"type": "STRING"
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


resource "google_bigquery_table" "autogiro_payment_plans_raw" {
	dataset_id = "main_staging"
	table_id   = "autogiro_payment_plans_raw"
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
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "pull_day_of_month",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "starting_at",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "amount",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


resource "google_bigquery_table" "ddi_sessions_raw" {
	dataset_id = "main_staging"
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
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


resource "google_bigquery_table" "assessment_reviews_raw" {
	dataset_id = "main_staging"
	table_id   = "assessment_reviews_raw"
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
		"name": "assessment_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "reviewed_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "reviewed_by",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "comment",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "interest_rate",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "monthly_fee",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "amortization_rate",
		"type": "NUMERIC"
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
	dataset_id = "main_staging"
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
		"type": "STRING"
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


resource "google_bigquery_table" "notes_raw" {
	dataset_id = "main_staging"
	table_id   = "notes_raw"
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
		"name": "created_by",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "content",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "application_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "files",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "type",
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


resource "google_bigquery_table" "customer_consents_raw" {
	dataset_id = "main_staging"
	table_id   = "customer_consents_raw"
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
		"name": "key",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "payload",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "signature_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "agreement_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "is_active",
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


resource "google_bigquery_table" "customer_features_raw" {
	dataset_id = "main_staging"
	table_id   = "customer_features_raw"
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
		"name": "customer_id",
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
		"name": "key",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "value",
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


resource "google_bigquery_table" "autogiro_payments_raw" {
	dataset_id = "main_staging"
	table_id   = "autogiro_payments_raw"
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
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "authorization_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "amount",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "reference",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "status_log",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "s3_key",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "pull_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "statement_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "payment_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "success_source",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "payment_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "creditor_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "currency_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "autogiro_payment_plan_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "product",
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


resource "google_bigquery_table" "payouts_raw" {
	dataset_id = "main_staging"
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
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "transfer_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "payment_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "payout_account",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "payment_type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "account_type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "account_number",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "reference",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "amount",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "currency",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "allocated_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "expected_payout_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "rejected_reason",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "admin_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "comment",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "product",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "receiver_name",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "country_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "rejected_by",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "failed_reason",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "initial_signed_by_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "initial_signed_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "signed_off_by_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "signed_off_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "creditor_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "allocation_needed",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "completed_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "application_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "bank_transaction_date",
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


resource "google_bigquery_table" "transfers_raw" {
	dataset_id = "main_staging"
	table_id   = "transfers_raw"
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
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "application_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "reviewed_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "reviewed_by",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "completed_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "completed_by",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "amount",
		"type": "NUMERIC"
	},
	{
		"mode": "NULLABLE",
		"name": "updated_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "updated_by",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "metadata",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "creditor_id",
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


resource "google_bigquery_table" "customers_aml_raw" {
	dataset_id = "main_staging"
	table_id   = "customers_aml_raw"
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
		"name": "customer_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "risk_level",
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
		"name": "_ingested_ts",
		"type": "TIMESTAMP"
	}
]
EOF
}


