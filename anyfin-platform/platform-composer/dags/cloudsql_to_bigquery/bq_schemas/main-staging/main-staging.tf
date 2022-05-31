resource "google_bigquery_table" "transactions_raw" {
	dataset_id = "main-staging_staging"
	table_id   = "transactions_raw"
	project    = "anyfin-staging"

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
		"name": "old_cycle_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "comment",
		"type": "STRING"
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
		"name": "source",
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


resource "google_bigquery_table" "cycles_raw" {
	dataset_id = "main-staging_staging"
	table_id   = "cycles_raw"
	project    = "anyfin-staging"

	labels = {
		env = "default"
	}

	schema = <<EOF
[
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
		"name": "loan_id",
		"type": "INTEGER"
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
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "dpd",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "due_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "balance",
		"type": "NUMERIC"
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
		"name": "customer_id",
		"type": "INTEGER"
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


resource "google_bigquery_table" "loans_raw" {
	dataset_id = "main-staging_staging"
	table_id   = "loans_raw"
	project    = "anyfin-staging"

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
		"name": "original_months",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "months",
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
		"name": "cycle_end_day",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "last_processed",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "dpd",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "contract_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "amortization_rate",
		"type": "NUMERIC"
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
		"name": "currency_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "creditor_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "source_loan_id",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "signature_id",
		"type": "STRING"
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
	dataset_id = "main-staging_staging"
	table_id   = "customers_raw"
	project    = "anyfin-staging"

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
		"name": "primary_channel_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "referral_code",
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
		"name": "status",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "autogiro_authorization_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "termination_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "termination_report_date",
		"type": "DATE"
	},
	{
		"mode": "NULLABLE",
		"name": "creditor_id",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "country_code",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "language_preference",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "birthdate",
		"type": "DATE"
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


resource "google_bigquery_table" "customer_events_raw" {
	dataset_id = "main-staging_staging"
	table_id   = "customer_events_raw"
	project    = "anyfin-staging"

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


resource "google_bigquery_table" "autogiro_payment_plans_raw" {
	dataset_id = "main-staging_staging"
	table_id   = "autogiro_payment_plans_raw"
	project    = "anyfin-staging"

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


resource "google_bigquery_table" "assessments_raw" {
	dataset_id = "main-staging_staging"
	table_id   = "assessments_raw"
	project    = "anyfin-staging"

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
	dataset_id = "main-staging_staging"
	table_id   = "applications_raw"
	project    = "anyfin-staging"

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
		"name": "promo_code_id",
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


