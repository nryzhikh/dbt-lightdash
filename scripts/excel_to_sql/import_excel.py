import pandas as pd
import psycopg
from psycopg import sql
import os
from dotenv import load_dotenv
import logging
from tqdm import tqdm
import numpy as np
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database connection parameters
DB_PARAMS = {
    'host': os.getenv('DB_HOST', '45.9.27.162'),
    'dbname': os.getenv('DB_NAME', 'econometrics'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', 'Ozz1TLeojwdaG0mg'),
    'port': os.getenv('DB_PORT', '5432')
}

def is_date_column(series):
    """Check if a pandas Series contains date-like values"""
    # First check if the column name suggests it's a date
    date_keywords = ['date', 'time', 'day', 'month', 'year', 'week']
    if any(keyword in series.name.lower() for keyword in date_keywords):
        # Then verify the values are actually dates
        try:
            # Convert non-null values to datetime
            non_null_values = series.dropna()
            if len(non_null_values) > 0:
                # Try to convert to datetime
                pd.to_datetime(non_null_values)
                # If more than 50% of non-null values are dates, consider it a date column
                date_count = sum(pd.to_datetime(non_null_values, errors='coerce').notna())
                return date_count / len(non_null_values) > 0.5
        except:
            pass
    return False

def get_column_types(df):
    """Determine column types based on content"""
    column_types = {}
    for col in df.columns:
        if is_date_column(df[col]):
            column_types[col] = 'DATE'
        else:
            # Check if column is numeric
            try:
                pd.to_numeric(df[col].dropna())
                column_types[col] = 'NUMERIC'
            except:
                column_types[col] = 'TEXT'
    return column_types

def create_table_if_not_exists(df, table_name, conn):
    """Drop and recreate table with proper column types"""
    logger.info(f"Dropping and recreating table {table_name}...")
    
    # Get column types
    column_types = get_column_types(df)
    column_definitions = []
    for col, dtype in column_types.items():
        if dtype == 'DATE':
            column_definitions.append(f"{col} DATE")
        elif dtype == 'NUMERIC':
            column_definitions.append(f"{col} NUMERIC")
        else:
            column_definitions.append(f"{col} TEXT")
    
    # Drop table if it exists
    drop_table_query = sql.SQL("""
    DROP TABLE IF EXISTS {table}
    """).format(
        table=sql.Identifier(table_name)
    )
    
    # Create table
    create_table_query = sql.SQL("""
    CREATE TABLE {table} (
        {fields}
    )
    """).format(
        table=sql.Identifier(table_name),
        fields=sql.SQL(',\n        ').join(map(sql.SQL, column_definitions))
    )
    
    with conn.cursor() as cur:
        cur.execute(drop_table_query)
        cur.execute(create_table_query)
        conn.commit()
    
    logger.info(f"Table {table_name} recreated")
    logger.info(f"Column types: {column_types}")

def read_excel_file(file_path):
    """Read Excel file and return DataFrame with proper date handling"""
    logger.info(f"Reading {file_path}...")
    
    # Read Excel file with header row
    df = pd.read_excel(file_path, header=0)
    
    # Clean column names (remove spaces, special characters, etc.)
    df.columns = [col.strip().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '') 
                 for col in df.columns]
    
    # Convert date columns to datetime
    for col in df.columns:
        if is_date_column(df[col]):
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Convert non-date columns to strings and handle NULL values
    for col in df.columns:
        if not is_date_column(df[col]):
            df[col] = df[col].astype(str)
            df[col] = df[col].replace({
                'nan': None,
                'None': None,
                '': None,
                'NaT': None,
                'NaN': None,
                'NULL': None,
                'null': None
            })
    
    logger.info(f"Successfully read {len(df)} rows from {file_path}")
    return df

def prepare_values_for_insertion(df):
    """Prepare DataFrame values for insertion, converting Timestamps to datetime"""
    values = []
    for _, row in df.iterrows():
        row_values = []
        for val in row:
            if pd.isna(val):
                row_values.append(None)
            elif isinstance(val, pd.Timestamp):
                # Convert to string in ISO format, which PostgreSQL can parse
                row_values.append(val.strftime('%Y-%m-%d'))
            else:
                row_values.append(str(val) if val is not None else None)
        values.append(tuple(row_values))
    return values

def insert_data_to_db(df, table_name, conn, batch_size=1000):
    """Insert DataFrame data into PostgreSQL table in batches"""
    logger.info(f"Inserting data into {table_name}...")
    
    # Create table if it doesn't exist
    create_table_if_not_exists(df, table_name, conn)
    
    # Get column names and types
    columns = df.columns.tolist()
    column_types = get_column_types(df)
    
    # Create the SQL query with proper type casting
    placeholders = []
    for i in range(len(columns)):
        if column_types[columns[i]] == 'DATE':
            placeholders.append(sql.SQL('CAST(%s AS DATE)'))
        elif column_types[columns[i]] == 'NUMERIC':
            placeholders.append(sql.SQL('CAST(%s AS NUMERIC)'))
        else:
            placeholders.append(sql.SQL('CAST(%s AS TEXT)'))
    
    query = sql.SQL("""
    INSERT INTO {table} ({fields})
    VALUES ({values})
    ON CONFLICT DO NOTHING
    """).format(
        table=sql.Identifier(table_name),
        fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
        values=sql.SQL(', ').join(placeholders)
    )
    
    # Process data in batches
    total_rows = len(df)
    with conn.cursor() as cur:
        for i in tqdm(range(0, total_rows, batch_size), desc=f"Inserting into {table_name}"):
            batch = df.iloc[i:i+batch_size]
            values = prepare_values_for_insertion(batch)
            
            try:
                cur.executemany(query, values)
                conn.commit()
            except Exception as e:
                logger.error(f"Error inserting batch {i//batch_size + 1}: {e}")
                conn.rollback()
                raise
    
    logger.info(f"Successfully inserted {total_rows} rows into {table_name}")

def validate_data(df, table_name):
    """Validate data before insertion"""
    logger.info(f"Validating data for {table_name}...")
    
    # Check for data quality
    null_counts = df.isnull().sum()
    if null_counts.any():
        logger.warning(f"Found null values in columns: {null_counts[null_counts > 0].to_dict()}")
    
    return True

def main():
    # Connect to the database
    try:
        conn = psycopg.connect(**DB_PARAMS)
        logger.info("Successfully connected to the database")
        
        # Process each Excel file
        excel_files = {
            'sber_national_tv_(20200101-20250119).xlsx': 'sber_national_tv_20200101_20250119',
            'sber_all_media_(20230101-20241231).xlsx': 'sber_all_media_20230101_20241231'
        }
        
        for file_name, table_name in excel_files.items():
            file_path = os.path.join('files', file_name)
            if os.path.exists(file_path):
                try:
                    df = read_excel_file(file_path)
                    if validate_data(df, table_name):
                        insert_data_to_db(df, table_name, conn)
                except Exception as e:
                    logger.error(f"Error processing {file_name}: {e}")
                    continue
            else:
                logger.error(f"File not found: {file_path}")
        
    except Exception as e:
        logger.error(f"Database connection error: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main() 