import json
import re
from string import Template
import sys


### 

DATABASE_NAME = sys.argv[1]

def convert_type_to_bq(t):
	if t == 'timestamp with time zone':
		return 'TIMESTAMP'

	elif t == 'date':
		return 'DATE'

	elif t == 'numeric':
		return 'NUMERIC'

	elif t == 'integer':
		return 'INTEGER'

	elif t == 'boolean':
		return 'BOOLEAN'

	elif t == 'real':
		return 'FLOAT'
	
	else: 
		return 'STRING'


with open(f'./pg_schemas/{DATABASE_NAME}_schemas_state.json') as f:
  schemas = json.load(f)


with open(f'./bq_schemas/{DATABASE_NAME}/{DATABASE_NAME}.tf', 'w') as output_f:
	for table in schemas:
		table_suffix = '_raw'
		database_suffix = '_staging'
		if DATABASE_NAME == 'dolph' and table == 'transactions': 
			table_suffix = ''
			database_suffix = ''

		output_f.write(f'resource "google_bigquery_table" "{table}{table_suffix}" {{\n')
		output_f.write(f'	dataset_id = "{DATABASE_NAME}{database_suffix}"\n')
		output_f.write(f'	table_id   = "{table}{table_suffix}"\n')
		output_f.write('	project    = "anyfin"\n')
		output_f.write('\n')
		output_f.write('	labels = {\n')
		output_f.write('		env = "default"\n')
		output_f.write('	}\n')				    
		output_f.write('\n')

		 
		if 'bq_partition_column' in [type_name for type_name in schemas[table]]:
			partition = schemas[table]['bq_partition_column']
			output_f.write('	time_partitioning { \n')
			output_f.write('    	type = "DAY" \n')
			output_f.write('		field = "{time_partitioning}" \n'.format(time_partitioning=partition))
			output_f.write('	}\n')
			output_f.write('\n')


		output_f.write('	schema = <<EOF\n')
		output_f.write('[\n')
		for column, c_type in schemas[table]['schema'].items():
			
			if column.startswith("\"") and column.endswith("\""):
				column = re.findall('"([^"]*)"', column)[0]
			if c_type == 'ARRAY':
				mode = 'REPEATED'
				c_type = 'STRING'
			else:
				mode = 'NULLABLE'
			output_f.write('	{\n')
			output_f.write('		"mode": "{mode}",\n'.format(mode=mode))
			output_f.write('		"name": "{column}",\n'.format(column=column))
			output_f.write('		"type": "{c_type}"\n'.format(c_type=convert_type_to_bq(c_type)))
			output_f.write('	}')
			output_f.write(',\n')




		output_f.write('	{\n')
		output_f.write('		"mode": "NULLABLE",\n')
		output_f.write('		"name": "_ingested_ts",\n')
		output_f.write('		"type": "TIMESTAMP"\n')
		output_f.write('	}')
		output_f.write('\n')

		output_f.write(']\n')
		output_f.write('EOF\n')
		output_f.write('}\n')
		output_f.write('\n')
		output_f.write('\n')
						









