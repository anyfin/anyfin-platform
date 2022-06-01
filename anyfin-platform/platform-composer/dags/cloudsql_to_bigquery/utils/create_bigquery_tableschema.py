
# Function to convert postgres schema types into BQ format
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

def create_bigquery_schema(schema):
	import re

	table_schema = {
		'fields': []
	}

	for column, t in schema.items():
		if column.startswith("\"") and column.endswith("\""):
			column = re.findall('"([^"]*)"', column)[0]
		if t == 'ARRAY':
			mode = 'REPEATED'
			t = 'STRING'
		else:
			mode = 'NULLABLE'
		table_schema['fields'].append( {"mode": mode, "name": column, "type": convert_type_to_bq(t)} )
	table_schema['fields'].append({"mode": "NULLABLE", "name": "_ingested_ts", "type": "TIMESTAMP"})
	
	return table_schema