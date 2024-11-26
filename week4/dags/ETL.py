import os
from dotenv import load_dotenv
import pandas as pd
import pyodbc
import psycopg2
from psycopg2.extras import execute_values
import traceback
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator  # Updated import for EmptyOperator

# Load environment variables from the .env file
load_dotenv()

# Define database connection information from environment variables
postgresql_user = os.getenv('POSTGRESQL_USER')  # PostgreSQL user
postgresql_password = os.getenv('POSTGRESQL_PASSWORD')  # PostgreSQL password
postgresql_db = os.getenv('POSTGRESQL_DB')  # Target database in PostgreSQL
postgresql_host = os.getenv('POSTGRESQL_HOST')
postgresql_port = os.getenv('POSTGRESQL_PORT')

# SQL Server connection details from environment variable
connection_string = os.getenv('SQLSERVER_CONNECTION_STRING')

# Columns for SalesData table
columns = [
    'Invoice ID', 'Branch', 'City', 'Customer type', 'Gender', 'Product line',
    'Unit price', 'Quantity', 'Tax 5%', 'Total', 'Date', 'Time', 'Payment',
    'cogs', 'gross margin percentage', 'gross income', 'Rating'
]

# Airflow default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 26),  # Start date of your DAG
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to extract data from SQL Server
def extract_data(**kwargs):
    try:
        # Connect to SQL Server using pyodbc
        conn_sql = pyodbc.connect(connection_string)
        print("SQL Server connection successful!")
        
        cursor_sql = conn_sql.cursor()
        cursor_sql.execute("SELECT * FROM SalesData")
        
        # Fetch column names
        columns = [column[0] for column in cursor_sql.description]
        
        # Fetch the data from SQL Server
        data = cursor_sql.fetchall()

        # Convert the fetched data to a list of tuples
        data_tuples = [tuple(row) for row in data]
        
        # Convert to pandas DataFrame
        df = pd.DataFrame(data_tuples, columns=columns)
        
        cursor_sql.close()
        conn_sql.close()

        # Push data to XCom for use in downstream tasks
        kwargs['ti'].xcom_push(key='data', value=df.to_dict())

        print(f"Extracted {len(df)} rows from SQL Server.")
    except Exception as e:
        print(f"Error during data extraction: {e}")
        traceback.print_exc()

# Function to transform data (this example just formats the date column)
def transform_data(**kwargs):
    ti = kwargs['ti']
    df_dict = ti.xcom_pull(task_ids='extract_data', key='data')

    try:
        df = pd.DataFrame.from_dict(df_dict)
        # Convert 'Date' column to datetime
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        
        # Fill missing values with forward fill (optional)
        df.fillna(method='ffill', inplace=True)

        # Push transformed data to XCom for loading
        kwargs['ti'].xcom_push(key='transformed_data', value=df.to_dict())
        print(f"Transformed data: {df.shape[0]} rows, {df.shape[1]} columns.")
    except Exception as e:
        print(f"Error during data transformation: {e}")
        traceback.print_exc()

# Function to load data into PostgreSQL
def load_data(**kwargs):
    ti = kwargs['ti']
    df_dict = ti.xcom_pull(task_ids='transform_data', key='transformed_data')

    try:
        df = pd.DataFrame.from_dict(df_dict)

        # Connect to PostgreSQL
        conn_pg = psycopg2.connect(
            dbname=postgresql_db,
            user=postgresql_user,
            password=postgresql_password,
            host=postgresql_host,
            port=postgresql_port
        )
        print("PostgreSQL connection successful!")
        
        cursor_pg = conn_pg.cursor()

        # Prepare data for bulk insert
        data_to_insert = []
        for index, row in df.iterrows():
            row_data = tuple(
                row[col] if pd.notna(row[col]) else None for col in columns
            )
            # Convert datetime columns to string (PostgreSQL compatibility)
            if isinstance(row_data[10], pd.Timestamp):  # 'Date' is at index 10
                row_data = row_data[:10] + (row_data[10].strftime('%Y-%m-%d %H:%M:%S'),) + row_data[11:]

            data_to_insert.append(row_data)

        # Insert data into PostgreSQL using psycopg2 execute_values
        insert_query = 'INSERT INTO SalesData VALUES %s'
        execute_values(cursor_pg, insert_query, data_to_insert)
        conn_pg.commit()
        
        print("Data successfully loaded into PostgreSQL.")
        
        cursor_pg.close()
        conn_pg.close()

    except psycopg2.OperationalError as e:
        print(f"Error connecting to PostgreSQL: {e}")
        traceback.print_exc()
    except Exception as e:
        print(f"Error during data load: {e}")
        traceback.print_exc()

# Define the Airflow DAG
with DAG(
    'daily_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline to extract, transform, and load data daily',
    schedule=timedelta(days=1),  # DAG will run once per day
    catchup=False,  # Prevents DAG from running for past dates
) as dag:

    # Define the tasks
    start_task = EmptyOperator(task_id='start')

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    end_task = EmptyOperator(task_id='end')

    # Set task dependencies
    start_task >> extract_task >> transform_task >> load_task >> end_task
