# This file serves the sole purpose of maintaining this list of databases and their info
# It is used in postgres_bq_etl.py and export_bq_backfill.py and postgres_bq_backfill.py
# In order to dynamically generate tasks for all databases

DATABASES_INFO = [
	{'DATABASE_NAME': 'main', 'INSTANCE_NAME': 'anyfin-main-replica', 'DATABASE':'main', 'DESTINATION_PROJECT': 'anyfin'},
	{'DATABASE_NAME': 'dolph', 'INSTANCE_NAME': 'anyfin-dolph-read-replica', 'DATABASE':'postgres', 'DESTINATION_PROJECT': 'anyfin'},
	{'DATABASE_NAME': 'pfm', 'INSTANCE_NAME': 'pfm-replica', 'DATABASE':'postgres', 'DESTINATION_PROJECT': 'anyfin'},
	{'DATABASE_NAME': 'psd2', 'INSTANCE_NAME': 'psd2-replica', 'DATABASE':'postgres', 'DESTINATION_PROJECT': 'anyfin'},
	{'DATABASE_NAME': 'sendout', 'INSTANCE_NAME': 'sendout-replica', 'DATABASE':'postgres', 'DESTINATION_PROJECT': 'anyfin'},
	{'DATABASE_NAME': 'savings', 'INSTANCE_NAME': 'savings-replica', 'DATABASE':'postgres', 'DESTINATION_PROJECT': 'anyfin'},
	{'DATABASE_NAME': 'assess', 'INSTANCE_NAME': 'assess-replica', 'DATABASE':'assess', 'DESTINATION_PROJECT': 'anyfin'},
	{'DATABASE_NAME': 'ddi', 'INSTANCE_NAME': 'ddi-service-replica', 'DATABASE':'main', 'DESTINATION_PROJECT': 'anyfin'},
	{'DATABASE_NAME': 'main-staging', 'INSTANCE_NAME': 'main-staging-replica', 'DATABASE':'main', 'DESTINATION_PROJECT': 'anyfin-staging'},
	{'DATABASE_NAME': 'paypull', 'INSTANCE_NAME': 'paypull-replica', 'DATABASE':'postgres', 'DESTINATION_PROJECT': 'anyfin'},
	{'DATABASE_NAME': 'payout', 'INSTANCE_NAME': 'payout-replica', 'DATABASE': 'payout', 'DESTINATION_PROJECT': 'anyfin'}
]