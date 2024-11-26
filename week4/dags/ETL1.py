import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import traceback
from dotenv import load_dotenv


load_dotenv()

# PostgreSQL connection
postgresql_user = os.getenv('POSTGRESQL_USER')
postgresql_password = os.getenv('POSTGRESQL_PASSWORD')
postgresql_db = os.getenv('POSTGRESQL_DB')
postgresql_host = os.getenv('POSTGRESQL_HOST')
postgresql_port = os.getenv('POSTGRESQL_PORT')

# Path to the CSV file
csv_file_path = os.getenv('CSV_FILE_PATH')

# Columns for the SalesData table
columns = [
    'Invoice ID', 'Branch', 'City', 'Customer type', 'Gender', 'Product line', 
    'Unit price', 'Quantity', 'Tax 5%', 'Total', 'Date', 'Time', 'Payment', 
    'cogs', 'gross margin percentage', 'gross income', 'Rating'
]

# Function to load data from CSV file
def extract_data(csv_file_path):
    try:
        # Load the data from CSV into a pandas DataFrame
        print(f"Extracting data from {csv_file_path}...")
        df = pd.read_csv(csv_file_path)
        print(f"Data loaded with {df.shape[0]} rows and {df.shape[1]} columns.")
        
        return df
    except Exception as e:
        print(f"Error during data extraction: {e}")
        traceback.print_exc()
        return None

# Function to transform data
def transform_data(df):
    try:
        # Ensure the 'Date' column is in the correct format (Timestamp -> string)
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')

       
        df.fillna(method='ffill', inplace=True)
        
        print(f"Data transformed. {df.shape[0]} rows and {df.shape[1]} columns.")
        return df
    except Exception as e:
        print(f"Error during data transformation: {e}")
        traceback.print_exc()
        return None

# load data into PostgreSQL
def load_data(df):
    try:
        # Connect to PostgreSQL
        print("Attempting to connect to PostgreSQL...")
        conn_pg = psycopg2.connect(
            dbname=postgresql_db,
            user=postgresql_user,
            password=postgresql_password,
            host=postgresql_host,
            port=postgresql_port
        )
        print("PostgreSQL connection successful!")

        cursor_pg = conn_pg.cursor()

        # Prepare data for insertion
        data_to_insert = []

        if df is not None:
            # Iterate over DataFrame rows to prepare them for bulk insertion
            for index, row in df.iterrows():
                # Convert the DataFrame row into a tuple of values to match the table's columns
                row_data = tuple(
                    row[col] if pd.notna(row[col]) else None for col in columns
                )

                # Handle the 'Date' column separately to ensure it's in the right format (Timestamp -> string)
                if isinstance(row_data[10], pd.Timestamp):  # 'Date' is at index 10
                    row_data = row_data[:10] + (row_data[10].strftime('%Y-%m-%d %H:%M:%S'),) + row_data[11:]

                data_to_insert.append(row_data)

            
            insert_query = '''
                INSERT INTO SalesData 
                VALUES %s
            '''
            
            
            try:
                print("Executing bulk insert...")
                execute_values(cursor_pg, insert_query, data_to_insert)
                conn_pg.commit()
                print("Data successfully loaded into PostgreSQL table: SalesData")

            except Exception as e:
                print(f"Error during bulk insert: {e}")
                traceback.print_exc()
                conn_pg.rollback()

        # Close the cursor and connection
        cursor_pg.close()
        conn_pg.close()

    except psycopg2.OperationalError as e:
        print(f"Error: {e}")
        traceback.print_exc()

# ETL
def run_etl_pipeline():
    # Step 1: Extract data
    df = extract_data(csv_file_path)
    if df is None:
        print("Data extraction failed. Aborting pipeline.")
        return
    
    # Step 2: Transform data
    df_transformed = transform_data(df)
    if df_transformed is None:
        print("Data transformation failed. Aborting pipeline.")
        return

    # Step 3: Load data into PostgreSQL
    load_data(df_transformed)

if __name__ == '__main__':
    run_etl_pipeline()
