from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import csv
import os
import pymongo
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from pymongo import MongoClient
from airflow.providers.mongo.hooks.mongo import MongoHook
from bson import json_util


# Define your PostgreSQL connection details (Ensure it's defined in Airflow's Connection UI)
mongo_hook = MongoHook(conn_id='mongodb_stream')


# Define the directory path and the CSV file name
csv_output_dir = '/home/sami/airflow/output/'
csv_filename = 'nosql_output.csv'

# Ensure the directory exists, or create it if it doesn't
os.makedirs(csv_output_dir, exist_ok=True)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'nosql_stream_dag',
    default_args=default_args,
    schedule=timedelta(minutes=1),  # Run every 2 minutes
    catchup=False,  # Disable catchup
)


# Task 1: Execute an SQL query


def execute_nosql_query():
    client = MongoClient('127.0.0.1', 27017)
    db = client['datalake_db']  # Replace with your actual database name
    collection = db['sales_data']

    # Define your NoSQL query
    nosql_query = {}

    # Execute the query and retrieve the results
    results = collection.find(nosql_query)

    # Convert the cursor to a list, excluding the "_id" field or converting it to a string
    result_list = [list(doc.values()) for doc in results]

    # Convert any ObjectId fields to strings
    for doc in result_list:
        doc[0] = str(doc[0])

    client.close()

    return result_list


execute_sql_task = PythonOperator(
    task_id='execute_nosql_query',
    python_callable=execute_nosql_query,
    dag=dag,
)

# Task 2: Append the query results to the same CSV file


def get_last_processed_id(csv_filename):
    # Initialize the last processed ID as -1, indicating no data has been processed yet
    last_processed_id = -1

    if os.path.isfile(csv_filename):
        # If the CSV file exists, read it to find the last processed ID
        with open(csv_filename, 'r') as csvfile:
            csv_reader = csv.reader(csvfile)
            try:
                next(csv_reader)  # Skip the header row
            except StopIteration:
                # Handle the case where the file is empty or doesn't have a header row
                pass

            for row in csv_reader:
                if row and row[1].isdigit():  # Check if the value is a valid integer
                    last_processed_id = max(last_processed_id, int(row[1]))

    return last_processed_id


def append_to_csv(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='execute_nosql_query')

    if result:
        # Define the column names
        column_names = ['_id', 'id', 'date', 'customer_id', 'transaction_id',
                        'sku_category', 'sku', 'quantity', 'sales_amount']

        # Properly reference the global csv_filename
        csv_filepath = os.path.join(csv_output_dir, csv_filename)

        # Check if the file already exists
        file_exists = os.path.isfile(csv_filepath)

        with open(csv_filepath, 'a', newline='') as csvfile:
            csv_writer = csv.writer(csvfile)

            # If the file doesn't exist, write the header
            if not file_exists:
                csv_writer.writerow(column_names)

            # Determine the last processed ID
            last_processed_id = get_last_processed_id(csv_filepath)

            # Append only the new data
            new_data = [row for row in result if row[1] > last_processed_id]
            csv_writer.writerows(new_data)
    else:
        # Handle the case where there is no data in the result
        print("No data to append to CSV.")


append_csv_task = PythonOperator(
    task_id='append_to_csv',
    python_callable=append_to_csv,
    provide_context=True,
    dag=dag,
)

execute_sql_task >> append_csv_task

print("Task Executed")
