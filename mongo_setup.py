"""
MongoDB Setup and Python Connection

What we are undertaking:

        -> Establish MongoDB in the cloud using MongoDB Atlas.
        -> Establish a connection between MongoDB Atlas and Python.

Processes:

    -> Create a database.
        -> Within the database, create a collection.
            -> Insert data (document in JSON format) into the collection.

"""

## Code:

from pymongo.mongo_client import MongoClient

# MongoDB Atlas connection URI
uri = "mongodb+srv://navya:------@cluster0.brupo7v.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri)

# Verify the connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

# Database and Collection Setup
db = client["mydatabase"]
mycol = db["myfirstcollection"]

# Inserting Data into Collection
myfirstrecord = {"name": "Navya", "lname": "somu", "add": "UP"}
mycol.insert_one(myfirstrecord)

mysecondrecord = {"name": "shiva", "lname": "singh", "add": "UP urban"}
mythirdrecord = {"name": "uma", "lname": "somu", "add": "AP"}
mycol.insert_many([mysecondrecord, mythirdrecord])

myfourthrecord = {"name": "uma", "lname": "somu", "add": "AP", "sal": 20000}
mycol.insert_one(myfourthrecord)

# Data Retrieval and Manipulation
print(client.list_database_names())

for i in mycol.find():
    print(i)

# Sorting records
for i in mycol.find().sort("name"):
    print(i)

for i in mycol.find().sort("name", -1):
    print(i)

# Deleting all records in the collection
mycol.delete_many({})

# Verify deletion
for i in mycol.find():
    print(i)

# Importing and converting CSV data to JSON
import pandas as pd
df = pd.read_csv(r"C:\Users\navya.somu\Downloads\fsdmstats\fsdmstats\titanic_train.csv")

# Displaying DataFrame Information
print(df.columns)
print(df.shape)
print(df.head())
print(df.T)

# Converting DataFrame to JSON
import json
cov_csvtojson = list(json.loads(df.T.to_json()).values())

# Creating a new collection for converted data
mycol2 = db["converted_files"]
mycol2.insert_many(cov_csvtojson)


"""
MongoDB Atlas URI: Replace 'navya' and '------' with your MongoDB Atlas username and password.
Modify file paths accordingly. 
"""