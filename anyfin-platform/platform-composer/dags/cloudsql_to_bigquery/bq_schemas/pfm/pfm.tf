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


resource "google_bigquery_table" "customer_widgets_raw" {
	dataset_id = "pfm_staging"
	table_id   = "customer_widgets_raw"
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
		"name": "type",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "position",
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


resource "google_bigquery_table" "salary_day_widgets_raw" {
	dataset_id = "pfm_staging"
	table_id   = "salary_day_widgets_raw"
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
		"name": "color",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "salary_day",
		"type": "INTEGER"
	},
	{
		"mode": "NULLABLE",
		"name": "adjust_weekends",
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


resource "google_bigquery_table" "no_spend_widgets_raw" {
	dataset_id = "pfm_staging"
	table_id   = "no_spend_widgets_raw"
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
		"name": "color",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "category",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "last_reset_at",
		"type": "TIMESTAMP"
	},
	{
		"mode": "NULLABLE",
		"name": "amount_per_day",
		"type": "FLOAT"
	},
	{
		"mode": "NULLABLE",
		"name": "active",
		"type": "BOOLEAN"
	},
	{
		"mode": "NULLABLE",
		"name": "past_streaks",
		"type": "STRING"
	},
	{
		"mode": "NULLABLE",
		"name": "milestone_checkpoints",
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


