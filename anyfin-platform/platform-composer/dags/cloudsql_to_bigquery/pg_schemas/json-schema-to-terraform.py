import json
import re
from string import Template


### REPLACE double quote "" with empty string
## @malcolmtivelius 17/12-20

def convert_type_to_bq(t):
	if t == 'timestamp with time zone':
		return 'TIMESTAMP'

	elif t == 'date':
		return 'DATE'

	elif t == 'numeric':
		return 'FLOAT'

	elif t == 'integer':
		return 'INTEGER'

	elif t == 'boolean':
		return 'BOOLEAN'

	else: 
		return 'STRING'


with open('main_schemas_state.json') as f:
  schemas = json.load(f)


with open('../bq_schemas/main/main.tf', 'w') as output_f:
	for table in schemas:
		output_f.write('resource "google_bigquery_table" "{table_name}_raw" {{\n'.format(table_name=table))
		output_f.write('	dataset_id = "main_staging"\n')
		output_f.write('	table_id   = "{table_name}_raw"\n'.format(table_name=table))
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
						









