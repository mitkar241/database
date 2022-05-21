#!/usr/bin/env python3

import warnings
from elasticsearch import Elasticsearch, exceptions

##########
# Elasticsearch Client
##########

def throwClientError():
  exit(0)

def get_es_client(ip="localhost", port=9200):
  es = None
  try:
    es = Elasticsearch(
    "http://" + ip + ":" + str(port),
    #username, pwd
    #tls
    )
    res = es.info()
    print({"msg" : "connected to elasticsearch", "ip" : ip, "port" : port, "client_info" : res})
  except exceptions.ConnectionError as err:
    print({"msg" : "ConnectionError took place, check if elasticsearch is running on given port", "ip" : ip, "port" : port, "err" : err})
    throwClientError()
  except Exception as err:
    print({"msg" : "Unexpected error", "ip" : ip, "port" : port, "err" : err})
    throwClientError()
  return es

##########
# Index
##########

def create_index(es, idx_name):
  # create an index in elasticsearch, ignore status code 400 (index already exists)
  res = es.indices.create(index=idx_name, ignore=[400, 404])
  print(res)

def get_index_list(es):
  #print(dir(es.indices))
  index_list = es.indices.get_alias().keys()
  print(index_list)

def exists_index(es, idx_name):
  if es.indices.exists(index=idx_name):
    return True
  return False

def drop_index(es, idx_name):
  res = es.indices.delete(index=idx_name, ignore=[400, 404])
  print(res)

##########
# Insert
##########

def insert_one(es, doc, doc_id_key=""):
  res = None
  if doc_id_key != "":
    res = es.index(
      index = idx_name,
      #doc_type = '_doc',
      id = doc[doc_id_key],
      document = doc, # or body
      request_timeout=10
    )
  else:
    res = es.index(
      index = idx_name,
      #doc_type = '_doc',
      document = doc, # or body
      request_timeout=10
    )
  #print(res["result"])

def insert_many(es, doc_list, doc_id_key=""):
  body = []
  for doc in doc_list:
    if doc_id_key != "":
      body.append({
        'index': {
          '_index': idx_name,
          #'_type': 'doc',
          '_id': doc[doc_id_key]
        }
      })
    else:
      body.append({
        'index': {
          '_index': idx_name,
          #'_type': 'doc',
        }
      })
    body.append(doc)
  # send bulk request
  res = es.bulk(body=body)
  print(res)

##########
# Query
##########

def find_by_id(es, idx_name, doc_id):
  result = []
  try:
    result = es.get(index=idx_name, id=doc_id)['_source']
  except exceptions.NotFoundError:
    pass
  return result

#curl -X GET 'localhost:9200/creditindex/_search?pretty' -H 'Content-Type: application/json' -d'{"query":{"match_all":{}}}'
def find_by_query(es, idx_name, searchquery={"match_all": {}}, source_includes=[], source_excludes=[]):
  result = []
  #### CRITICAL
  res = es.indices.refresh(index=idx_name)
  #print(res)
  if source_includes != [] and source_excludes != []:
    result = es.search(
      index=idx_name,
      source_includes=source_includes, #["id", "credit_card_type"],
      source_excludes=source_excludes, #["uid"],
      query=searchquery
    )
  elif source_includes != []:
    result = es.search(
      index=idx_name,
      source_includes=source_includes, #["id", "credit_card_type"],
      query=searchquery
    )
  elif source_excludes != []:
    result = es.search(
      index=idx_name,
      source_excludes=source_excludes, #["uid"],
      query=searchquery
    )
  else:
    result = es.search(
      index=idx_name,
      query=searchquery
    )
  match_count = result['hits']['total']['value']
  doc_list = result['hits']['hits']
  return match_count, doc_list

##########
# Delete
##########

def delete_by_id(es, idx_name, doc_id):
  try:
    res = es.delete(index=idx_name, id=doc_id)
    print(res)
  except exceptions.NotFoundError:
    pass

def delete_by_query(es, idx_name, searchquery={"match_all": {}}):
  res = es.indices.refresh(index=idx_name)
  res = es.delete_by_query(index=idx_name, query=searchquery)
  return (res['deleted'])


##########
# Utils
##########

def ignore_warnings():
  warnings.filterwarnings("ignore")

def get_sample_list():
  creditlist = [
    {"id":7930,"uid":"ef1dbe3f-04a4-4ede-844f-391a759d61ca","credit_card_number":"1212-1221-1121-1234","credit_card_expiry_date":"2026-01-28","credit_card_type":"dankort"},
    {"id":3885,"uid":"209c9b4c-6b50-4c69-9084-bc88780aa9d6","credit_card_number":"1211-1221-1234-2201","credit_card_expiry_date":"2026-01-28","credit_card_type":"jcb"},
    {"id":7102,"uid":"59cbec34-0d6d-411c-88e0-44b9bcf3d7b9","credit_card_number":"1234-2121-1221-1211","credit_card_expiry_date":"2023-01-29","credit_card_type":"jcb"},
    {"id":7748,"uid":"5131e5fb-4573-4e50-b23e-544e2f39bdcb","credit_card_number":"1234-2121-1221-1211","credit_card_expiry_date":"2024-01-29","credit_card_type":"visa"},
    {"id":7621,"uid":"492ac7e7-7a91-4620-8fd0-8d3071b69a60","credit_card_number":"1211-1221-1234-2201","credit_card_expiry_date":"2025-01-28","credit_card_type":"american_express"}
  ]
  return creditlist

if __name__ == "__main__":
  idx_name = "creditindex"
  creditlist = get_sample_list()
  ignore_warnings()
  es = get_es_client()

  # Create Index
  get_index_list(es)
  create_index(es, idx_name)
  print(exists_index(es, idx_name))
  get_index_list(es)

  # Single Insert
  #for credit in creditlist:
  #  insert_one(es, credit, "id")

  # Bulk Insert
  insert_many(es, creditlist, "id")

  # Find by ID
  result = find_by_id(es, idx_name, 7621)
  print(result)

  # Find by Query
  match_count, doc_list = find_by_query(es, idx_name, {"match":{"credit_card_type":"jcb"}})
  print(doc_list)
  #print("found %d docs" % match_count)
  #for doc in doc_list:
  #  print("%(uid)s %(credit_card_number)s: %(credit_card_type)s" % doc["_source"])

  # Delete by ID
  delete_by_id(es, idx_name, 7621)

  # Delete by Query
  delete_count = delete_by_query(es, idx_name)
  print("deleted %d docs" % delete_count)

  # Find All Docs
  match_count, doc_list = find_by_query(es, idx_name)
  print(doc_list)

  # Drop Index
  drop_index(es, idx_name)
  get_index_list(es)
