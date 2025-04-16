import psycopg2
import yaml
import os

def get_table_schema(conn, table_name):
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = 'public'
        AND table_name = %s
        ORDER BY ordinal_position;
    """, (table_name,))
    
    columns = []
    for row in cur.fetchall():
        column = {
            'name': row[0],
            'description': f"{row[1]} column",
            'tests': []
        }
        
        if row[2] == 'NO':
            column['tests'].append('not_null')
            
        if 'id' in row[0].lower():
            column['tests'].append('unique')
            
        columns.append(column)
    
    return columns

def generate_yaml(table):
    schema = {
        'version': 2,
        'models': [{
            'name': table['name'],
            'description': f"Data from {table['name']} table",
            'columns': table['columns']
        }]
    }
    
    return yaml.dump(schema, sort_keys=False, allow_unicode=True)

def main():
    # Database connection parameters
    conn_params = {
        'host': '45.9.27.162',
        'database': 'econometrics',
        'user': 'root',
        'password': 'Ozz1TLeojwdaG0mg',
        'port': '5432'
    }
    
    # Connect to the database
    conn = psycopg2.connect(**conn_params)
    
    # Create models directory if it doesn't exist
    os.makedirs('models', exist_ok=True)
    
    # Get schema for each table and generate separate YAML files
    tables = ['econometrics', 'econometrics_2025']
    for table_name in tables:
        table = {
            'name': table_name,
            'columns': get_table_schema(conn, table_name)
        }
        
        # Generate YAML
        yaml_content = generate_yaml(table)
        
        # Write to file
        with open(f'models/{table_name}.yml', 'w') as f:
            f.write(yaml_content)
    
    conn.close()

if __name__ == '__main__':
    main() 