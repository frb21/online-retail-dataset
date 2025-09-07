# ETL Pipeline for automation using Airflow
# CSV FROM KAGGLE -> PANDAS DATAFRAME -> POSTGRESQL DB -> CLOUD (OPTIONAL)

# Libraries
from datetime import datetime, timedelta, date
from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import logging

# CONFIG
CSV_FILE_PATH = "data/Online_Retail.csv"

# Extract data and transform
def import_data_and_transform():
    # Read csv file from kaggle
    url = "/opt/airflow/data/Online_Retail.csv"
    df = pd.read_csv(url, encoding='ISO-8859-1', on_bad_lines='warn')

    # Data properties
    """ print("DATASET INFO:")
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    print(f"Data Types: {df.dtypes}")
    print(df.head(10))
 """
    # Clean data
    df = df.dropna(subset=['CustomerID', 'InvoiceNo', 'StockCode'])
    df = df.drop_duplicates()
    
    # Convert relevant columns to string dtype
    for col in ['Country', 'Description', 'StockCode']:
        df[col] = df[col].astype('string')

    df = df[(df['Quantity'] > 0) & (df['UnitPrice'] > 0)]
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format="%m/%d/%y %H:%M")
    df['TotalPrice'] = df['Quantity'] * df['UnitPrice']

    hook = PostgresHook(postgres_conn_id='retail_connection')
    insert_query = """
        INSERT INTO retail_transactions (
        InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country, TotalPrice
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (InvoiceNo, StockCode) DO NOTHING;
        """

    records = list(df.itertuples(index=False, name=None))

    # Insert each record individually
    for record in records:
        hook.run(insert_query, parameters=record)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.today(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'extract_and_store_retail_data',
    default_args=default_args,
    description="A simple DAG that extracts csv file of online retail data and store in Postgres",
    start_date=datetime.today(),
    schedule='@daily',
    catchup=False,
)

create_table_task = SQLExecuteQueryOperator(
    task_id="create_table",
    conn_id='retail_connection',
    sql="""
    CREATE TABLE IF NOT EXISTS retail_transactions (
        InvoiceNo VARCHAR(20) NOT NULL,
        StockCode VARCHAR(20) NOT NULL,
        Description TEXT,
        Quantity INTEGER NOT NULL,
        InvoiceDate TIMESTAMP NOT NULL,
        UnitPrice NUMERIC(10, 4) NOT NULL,
        CustomerID INTEGER,
        Country VARCHAR(50),
        TotalPrice NUMERIC(12, 4),
        PRIMARY KEY (InvoiceNo, StockCode)
    );
""",
    dag=dag,
)

insert_transactions_task = PythonOperator(
    task_id="import_data",
    python_callable=import_data_and_transform,
    dag=dag,
)


create_table_task >> insert_transactions_task