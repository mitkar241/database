#!/usr/bin/env python3

import pymongo
from pymongo.errors import BulkWriteError

"""
##########
# searchquery example
##########
# query all entries
searchquery = {}
# query using specific value
searchquery = { "credit_card_type":"jcb" }
# comparison query
searchquery = { "_id": { "$gt": 7000 } }
# regex query
searchquery = { "credit_card_type": { "$regex": "^j" } }

##########
# projectionquery example
##########
# _id is the only field that you can mix inclusions and exclusions
# project all columns
projectionquery = {}
# exclusion example
projectionquery = {"_id":0, "uid":0, "credit_card_number":0}
# inclusion example
projectionquery = {"_id":0, "credit_card_number":1, "credit_card_type":1}
# hybrid example - error
projectionquery = {"_id":0, "uid":0, "credit_card_type":1})

##########
# updationquery example
##########
updationquery = { "$set": { "credit_card_type":"visa" } }
"""

##########
# Mongo Client
##########

# MC = Mongo Client
def get_mc(ip="localhost", port=27017):
  #mc = pymongo.MongoClient('mongodb://'+ip+':'+str(port)+'/')
  mc = pymongo.MongoClient(ip, port)
  return mc

##########
# Database
##########

def get_db(mc, db_name):
  db = mc[db_name]
  return db

def get_db_list(mc):
  db_list = mc.list_database_names()
  return db_list

def exists_db(mc, db_name):
  db_list = mc.list_database_names()
  if db_name in db_list:
    return True
  return False

def drop_db(mc, db_name):
  mc.drop_database(db_name)

##########
# Collection
##########

def get_col(db, col_name):
  col = db[col_name]
  return col

def get_col_list(db):
  col_list = db.list_collection_names()
  return col_list

def exists_col(db, col_name):
  col_list = db.list_collection_names()
  if col_name in col_list:
    return True
  return False

def drop_col(col):
  col.drop()

##########
# Insert
##########

def insert_one(col, entry):
  result = col.insert_one(entry)
  return result.inserted_id

def insert_many(col, entry_list):
  try:
    result = col.insert_many(entry_list)
    return result.inserted_ids
  except BulkWriteError as bwe:
    print(bwe.details)
    #werrors = bwe.details['writeErrors']
    raise

##########
# Query
##########

def find_one(col, searchquery={}, projectionquery={}):
  result = col.find_one(searchquery, projectionquery)
  return result

# Need Generic Function
def find_many(col, searchquery={}, projectionquery={}):
  result = col.find(searchquery, projectionquery)
  return result

def find_many_with_limit(col, limit, searchquery={}, projectionquery={}):
  result = col.find(searchquery, projectionquery).limit(limit)
  return result

def find_many_with_skip(col, skip, searchquery={}, projectionquery={}):
  result = col.find(searchquery, projectionquery).skip(skip)
  return result

##########
# Update
##########

def update_one(col, searchquery={}, updationquery={}):
  result = col.update_one(searchquery, updationquery)
  return result.modified_count

def update_many(col, searchquery={}, updationquery={}):
  result = col.update_many(searchquery, updationquery)
  return result.modified_count

##########
# Delete
##########

def delete_one(col, searchquery={}):
  result = col.delete_one(searchquery)
  return result.deleted_count

def delete_many(col, searchquery={}):
  result = col.delete_many(searchquery)
  return result.deleted_count

##########
# Utils
##########

def sort_asc(col, target_key):
  result = col.find().sort(target_key, 1)
  return result

def sort_desc(col, target_key):
  result = col.find().sort(target_key, -1)
  return result

def count_documents(col, searchquery={}, projectionquery={}):
  result = col.count_documents(searchquery, projectionquery)
  return result

def get_sample_list():
  creditlist = [
    {"id":7930,"uid":"ef1dbe3f-04a4-4ede-844f-391a759d61ca","credit_card_number":"1212-1221-1121-1234","credit_card_expiry_date":"2026-01-28","credit_card_type":"dankort"},
    {"id":3885,"uid":"209c9b4c-6b50-4c69-9084-bc88780aa9d6","credit_card_number":"1211-1221-1234-2201","credit_card_expiry_date":"2026-01-28","credit_card_type":"jcb"},
    {"id":7102,"uid":"59cbec34-0d6d-411c-88e0-44b9bcf3d7b9","credit_card_number":"1234-2121-1221-1211","credit_card_expiry_date":"2023-01-29","credit_card_type":"jcb"},
    {"id":7748,"uid":"5131e5fb-4573-4e50-b23e-544e2f39bdcb","credit_card_number":"1234-2121-1221-1211","credit_card_expiry_date":"2024-01-29","credit_card_type":"visa"},
    {"id":7621,"uid":"492ac7e7-7a91-4620-8fd0-8d3071b69a60","credit_card_number":"1211-1221-1234-2201","credit_card_expiry_date":"2025-01-28","credit_card_type":"american_express"}
  ]
  return creditlist

def modify_key_to_id(entry, target_key):
  if target_key in entry:
    entry["_id"] = entry[target_key]
    del entry[target_key]
  return entry

if __name__ == "__main__":
  mc = get_mc()
  db_name = "mitkardb"
  mitkardb = get_db(mc, db_name)
  col_name = "creditcol"
  creditcol = get_db(mitkardb, col_name)

  creditlist = get_sample_list()
  insert_many(creditcol, creditlist)

  searchquery = { "credit_card_type": { "$regex": "^j" } }
  projectionquery = {"_id":0, "uid":0, "credit_card_number":0}
  updationquery = { "$set": { "credit_card_type":"visa" } }

  result = find_many(creditcol, searchquery, projectionquery)
  for entry in result:
    print(entry)
  result = update_many(creditcol, searchquery, updationquery)
  print("Modified " + str(result) + " entries.")
  result = find_many(creditcol)
  for entry in result:
    print(entry)
  
  if exists_col(mitkardb, col_name):
    print("The collection " + col_name + " exists.")
    drop_col(creditcol)
    print("Dropped collection " + col_name)
  drop_db(mc, db_name)
