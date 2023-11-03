import csv
from pymongo import MongoClient
from datetime import datetime

# MongoDB connection information
mongo_uri = "mongodb://127.0.0.1:27017/"  # Update with your MongoDB URI
database_name = "datalake_db"
collection_name = "sales_data"

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[database_name]
collection = db[collection_name]

# CSV file path
csv_file = "scanner_data.csv"

# Function to convert data types


def convert_data_types(row):
    row["id"] = int(row["id"])
    row["date"] = datetime.strptime(row["date"], "%Y-%m-%d")
    row["customer_id"] = int(row["customer_id"])
    row["transaction_id"] = int(row["transaction_id"])
    row["quantity"] = float(row["quantity"])
    row["sales_amount"] = float(row["sales_amount"])
    return row


# Insert data from CSV file
with open(csv_file, 'r') as file:
    reader = csv.DictReader(file)
    data_to_insert = [convert_data_types(row) for row in reader]

    if data_to_insert:
        result = collection.insert_many(data_to_insert)
        print(f"Inserted {len(result.inserted_ids)} documents")

# Close the MongoDB connection
client.close()
