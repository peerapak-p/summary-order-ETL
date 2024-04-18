import os
import boto3
import s3fs
import pymssql
import pandas as pd
from airflow import DAG
from datetime import datetime
from sqlalchemy import create_engine
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

#env
MSSQL_HOST = os.getenv('MSSQL_SERVER')
MSSQL_PORT = os.getenv('MSSQL_PORT')
MSSQL_USER = os.getenv('MSSQL_USER')
MSSQL_PASSWORD = os.getenv('MSSQL_PASSWORD')
MSSQL_DB = os.getenv('MSSQL_DATABASE')

#Connect database
conn = pymssql.connect(server='103.138.179.246', user='itf', password='iTr@nsf0rm', database='StoreManagement')
cursor = conn.cursor(as_dict=True)
engine = create_engine("postgresql://airflow:airflow@172.18.0.4/data_en_pro")
# engine = create_engine('mssql+pymssql://itf:iTr@nsf0rm@103.138.179.246/StoreManagement')
# engine = create_engine(f'mssql+pymssql://{MSSQL_USER}:{MSSQL_PASSWORD}@{MSSQL_HOST}/{MSSQL_DB}')
# engine = create_engine('mssql+pyodbc://user:password@server/database')
# Define your DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
}


# Define a Python function to execute SQL query
def sum_sale_order():
    print('Start extract.')
    sql_query2 = """
        SELECT TOP(1) *
        FROM Doc_Trans
    """
    sql_query = """
        SELECT *
        FROM sum_sale
    """
    cursor.execute(sql_query2)
    data = cursor.fetchall()
    sale_data_df = pd.DataFrame(data)
    # sum_sale_df = pd.read_sql(sql_query, engine)
    print(sale_data_df.head())  
    # print(sum_sale_df.head())  
    # file_name = '/tmp/sale_clean.csv'
    # sale_data_df.to_csv(file_name, index=False)

    #Load
    # sum_sale_df.to_csv("/Users/peerapak/Downloads/sum_sale.csv", index=False)
    # sum_sale_df.to_csv("/Users/peerapak/Documents/sum_sale.csv", index=False)
    return sale_data_df

def upload_files(dataframe: pd.DataFrame, bucket_name, name_file):
    # s3_hook = S3Hook(aws_conn_id='s3_conn')
    # s3_hook.load_file(
    #     filename=file_path,
    #     key=s3_key,
    #     bucket_name=bucket_name,
    #     replace=True
    # )
    # s3 = boto3.resource('s3')
    fs = s3fs.S3FileSystem()
    with fs.open(f's3://{bucket_name}/{name_file}') as f:
        dataframe.to_csv(f, index=False)
    # dataframe.to_csv(f's3://{bucket_name}/{name_file}', index=False)


with DAG('mssql_example', default_args=default_args, schedule_interval='*/30 * * * *') as dag:

    # Define a PythonOperator to execute the Python function
    extract_data = PythonOperator(
        task_id='sum_sale_order',
        python_callable=sum_sale_order,
        dag=dag
    )
    print(f'Output ==> {extract_data}')

    upload_s3 = PythonOperator(
        task_id='upload_file',
        python_callable=upload_files,
        op_kwargs={
        'dataframe': extract_data.output,
        'bucket_name': 'clean-data-mssql',
        'name_file': 'clean_sale_data.csv'
        },
        dag=dag
    )

extract_data >> upload_s3