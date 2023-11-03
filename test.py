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


mongo_hook = MongoHook(conn_id='mongodb_stream')

client = MongoClient('127.0.0.1', 27017)
db = client['datalake_db']  # Replace with your actual database name
collection = db['sales_data']

# Define your NoSQL query
nosql_query = {}

# Execute the query and retrieve the results
results = collection.find(nosql_query)

# Convert the cursor to a list and exclude the "_id" field
# result_list = [json_util.loads(json_util.dumps( doc, default=json_util.default)) for doc in results]
result_list = [list(doc.values()) for doc in results]
# Close the MongoDB client connection
client.close()

print(result_list)
# for result in results:
#     print(result)
