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
            'description': row[1]
        }
        
        # Add dimension type for numeric columns
        if row[1] in ['integer', 'bigint', 'numeric', 'decimal', 'real', 'double precision']:
            column['meta'] = {
                'dimension': {
                    'type': 'number'
                }
            }
            
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
    file_name = ['эконометрика', 'эконометрика_2025']
    for table_name, file_name in zip(tables, file_name):
        table = {
            'name': table_name,
            'columns': get_table_schema(conn, table_name)
        }
        
        # Generate YAML
        yaml_content = generate_yaml(table)
        
        # Write to file
        with open(f'models/base/{file_name}.yml', 'w') as f:
            f.write(yaml_content)
    
    conn.close()

if __name__ == '__main__':
    main() 