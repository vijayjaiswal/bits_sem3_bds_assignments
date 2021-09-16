# Part 1: Data loader application

# Write a data loader application to do the following:
# 1.	Read the records and insert them into a MongoDB collection but with the transformations specified below.
#       Choose appropriate data types.
# 2.	There may be duplicate records in the data set. Delete duplicates during insertion.
# 3.	Remove the URL field during insertion.
# 4.	Combine the outcode and postcode into one zip code field in the DB, i.e. zip code = outcode + postcode.
# 5.	Create any indices that you think may be important based on the query types specified in the next part.
import pymongo
from pymongo import MongoClient
import pandas as pd

# Creating Mongodb connectivity to "rest" database
client = MongoClient('localhost', 27017)
# Database Name : rest
db = client.rest
# Collection Name : rest
rest = db.rest

# Reading "res.json" file to be loaded into the Mongodb
df=pd.read_json('res.json', lines="true")

# Transformation as per detail above
print("Performing Data transformations")

df['_id'] = [a['$oid'] for a in df['_id']]

# Creating ZipCode using outcode and postcode
df['zipcode'] = df['outcode']+df['postcode']

# Deleting NOT required attributes URL, postcodeand outcode
del df['URL']
del df['postcode']
del df['outcode']

# Updating rating is the are missing and converting it into float64 type
df['rating'] = df['rating'].apply(lambda x: 0 if x == 'Not yet rated' else x )

# Dropping Duplicate records
df = df.drop_duplicates(subset=["address", "name", "zipcode", "rating", "type_of_food"])


print("After Data transformations")
print(df.info())
print(df.head())

data_dict = df.to_dict('records')

print("Saving to Mongo DB")
# Purging collection before inserting new transformed data
rest.delete_many({})
rest.insert_many(data_dict)

# Number of documents in neighborhoods collection
records_inserted=rest.count_documents({})
print(records_inserted," records inserted successfully")

# Creating Index on type_of_food, zipcode, address
print("\nCreating Required index.")
index_response= rest.create_index([("type_of_food", pymongo.DESCENDING),
          ("zipcode", pymongo.ASCENDING),
          ("address", pymongo.ASCENDING)], name="ind_food_zip_address")
print("index created with name:", index_response)
print(rest.index_information())