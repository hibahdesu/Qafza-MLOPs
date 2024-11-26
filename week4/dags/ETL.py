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
from airflow.operators.empty import EmptyOperator  

load_dotenv()

postgresql_user = os.getenv('POSTGRESQL_USER')  
postgresql_password = os.getenv('POSTGRESQL_PASSWORD')  
postgresql_db = os.getenv('POSTGRESQL_DB')  
postgresql_host = os.getenv('POSTGRESQL_HOST')
postgresql_port = os.getenv('POSTGRESQL_PORT')


connection_string = os.getenv('SQLSERVER_CONNECTION_STRING')


columns = [
    'Invoice ID', 'Branch', 'City', 'Customer type', 'Gender', 'Product line',
    'Unit price', 'Quantity', 'Tax 5%', 'Total', 'Date', 'Time', 'Payment',
    'cogs', 'gross margin percentage', 'gross income', 'Rating'
]


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 26),  
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def extract_data(**kwargs):
    try:
        
        conn_sql = pyodbc.connect(connection_string)
        print("SQL Server connection successful!")
        
        cursor_sql = conn_sql.cursor()
        cursor_sql.execute("SELECT * FROM SalesData")
        
      
        columns = [column[0] for column in cursor_sql.description]
        
        
        data = cursor_sql.fetchall()

        
        data_tuples = [tuple(row) for row in data]
        
        
        df = pd.DataFrame(data_tuples, columns=columns)
        
        cursor_sql.close()
        conn_sql.close()

      
        kwargs['ti'].xcom_push(key='data', value=df.to_dict())

        print(f"Extracted {len(df)} rows from SQL Server.")
    except Exception as e:
        print(f"Error during data extraction: {e}")
        traceback.print_exc()

def transform_data(**kwargs):
    ti = kwargs['ti']
    df_dict = ti.xcom_pull(task_ids='extract_data', key='data')

    try:
        df = pd.DataFrame.from_dict(df_dict)

        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        
        df.fillna(method='ffill', inplace=True)

        
        kwargs['ti'].xcom_push(key='transformed_data', value=df.to_dict())
        print(f"Transformed data: {df.shape[0]} rows, {df.shape[1]} columns.")
    except Exception as e:
        print(f"Error during data transformation: {e}")
        traceback.print_exc()


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

        
        data_to_insert = []
        for index, row in df.iterrows():
            row_data = tuple(
                row[col] if pd.notna(row[col]) else None for col in columns
            )

            if isinstance(row_data[10], pd.Timestamp):  # 'Date' is at index 10
                row_data = row_data[:10] + (row_data[10].strftime('%Y-%m-%d %H:%M:%S'),) + row_data[11:]

            data_to_insert.append(row_data)

        
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


with DAG(
    'daily_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline to extract, transform, and load data daily',
    schedule=timedelta(days=1),  
    catchup=False,  
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

    start_task >> extract_task >> transform_task >> load_task >> end_task
