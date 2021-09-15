from pymongo import MongoClient
import numpy as np

client = MongoClient()
db = client.rest
rest = db.rest

import pandas as pd
df=pd.read_json('res.json', lines="true")

print("Performing Data transformations")
df['_id'] = [a['$oid'] for a in df['_id']]
del df['URL']
df['zipcode'] = df['outcode']+df['postcode']
del df['postcode']
del df['outcode']
df['rating'] = df['rating'].apply(lambda x: 0 if x == 'Not yet rated' else x )
df = df.drop_duplicates(subset=["address", "name", "zipcode", "rating", "type_of_food"])

print("After Data transformations")
print(df.head())

data_dict = df.to_dict('records')

print("Saving to Mongo DB")
rest.delete_many({})
rest.insert_many(data_dict)
