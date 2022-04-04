# This file serves the sole purpose of maintaining this list of databases and their info
# It is used in postgres_bq_etl.py and export_bq_backfill.py and postgres_bq_backfill.py
# In order to dynamically generate tasks for all databases

DATABASES_INFO = [
					{'DATABASE_NAME': 'main', 'INSTANCE_NAME': 'anyfin-main-replica', 'DATABASE':'main'},
					{'DATABASE_NAME': 'dolph', 'INSTANCE_NAME': 'anyfin-dolph-read-replica', 'DATABASE':'postgres'},
					{'DATABASE_NAME': 'pfm', 'INSTANCE_NAME': 'pfm-replica', 'DATABASE':'postgres'},
					{'DATABASE_NAME': 'psd2', 'INSTANCE_NAME': 'psd2-replica', 'DATABASE':'postgres'},
					{'DATABASE_NAME': 'sendout', 'INSTANCE_NAME': 'sendout-replica', 'DATABASE':'postgres'},
					{'DATABASE_NAME': 'savings', 'INSTANCE_NAME': 'savings-replica', 'DATABASE':'postgres'},
					{'DATABASE_NAME': 'assess', 'INSTANCE_NAME': 'assess-replica', 'DATABASE':'assess'},
					{'DATABASE_NAME': 'ddi', 'INSTANCE_NAME': 'ddi-service-replica', 'DATABASE':'main'}
				 ]