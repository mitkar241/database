import mysql.connector

"""
best practice at everyone who comes across the sql connection using MySQLdb
The process is to be simple 7 steps

- Create connection
- Create cursor
- Create Query string
- Execute the query
- Commit to the query
- Close the cursor
- Close the connection
"""

def exec_cmd(db_name, sql, val=None):
  try:
    conn = None
    if db_name == "":
      conn = mysql.connector.connect(
      host="localhost",
      user="raktim",
      password="Raktim!241",
      database=db_name
    )
    else:
      conn = mysql.connector.connect(
      host="localhost",
      user="raktim",
      password="Raktim!241",
      database=db_name
    )
    #cursor = conn.cursor()
    # without a buffered cursor, the results are "lazily" loaded,
    # meaning that "fetchone" actually only fetches one row from the full result set of the query.
    # When you will use the same cursor again, it will complain that you still have n-1 results,
    # where n is the result set amount) waiting to be fetched.
    # However, when you use a buffered cursor the connector fetches ALL rows behind the scenes
    # and you just take one from the connector so the mysql db won't complain.
    # Bonus reason to use buffered=True: it makes .rowcount return the total from the start.
    cursor = conn.cursor(buffered=True)
    if val == None:
      val = ()
      cursor.execute(sql, val)
    elif isinstance(val, list):
      # CASE : insert_many
      cursor.executemany(sql, val)
    else:
      cursor.execute(sql, val)
    if "INSERT" in sql or "UPDATE" in sql or "DELETE" in sql:
      conn.commit()
    return conn, cursor
  except mysql.connector.Error as err:
    raise Exception("ERR : {}".format(err))

##########
# Database
##########

def create_db(db_name):
  conn, cursor = exec_cmd("", "CREATE DATABASE IF NOT EXISTS " + db_name)
  cursor.close()
  conn.close()

def get_db_list():
  db_list = []
  conn, cursor = exec_cmd("", "SHOW DATABASES")
  result = cursor.fetchall()
  for tuple in result:
    for db in tuple:
      db_list.append(db)
  cursor.close()
  conn.close()
  return db_list

def drop_db(db_name):
  conn, cursor = exec_cmd("", "DROP DATABASE IF EXISTS " + db_name)
  cursor.close()
  conn.close()

##########
# Table
##########

def create_tab(db_name, tab_name, schema_str):
  conn, cursor = exec_cmd(db_name, "CREATE TABLE IF NOT EXISTS " + tab_name + " ("+schema_str+")")
  cursor.close()
  conn.close()

def get_tab_list(db_name):
  tab_list = []
  conn, cursor = exec_cmd(db_name, "SHOW TABLES")
  result = cursor.fetchall()
  for tuple in result:
    for db in tuple:
      tab_list.append(db)
  cursor.close()
  conn.close()
  return tab_list

def drop_tab(db_name, tab_name):
  conn, cursor = exec_cmd(db_name, "DROP TABLE IF EXISTS " + tab_name)
  cursor.close()
  conn.close()

##########
# Insert
##########

def insert_row(db_name, tab_name, key_str, value_str, value_list):
  sql = "INSERT INTO " + tab_name + " ("+key_str+") VALUES ("+value_str+")"
  conn, cursor = exec_cmd(db_name, sql, value_list)
  cursor.close()
  conn.close()

##########
# Query
##########

def find_row(db_name, tab_name, col_list=["*"], where_condition="", val=()):
  entry_list = []
  col_str = ", ".join(col_list)
  sql = "SELECT "+col_str+" FROM " + tab_name + where_condition
  conn, cursor = exec_cmd(db_name, sql, val)
  result = cursor.fetchall()
  for entry in result:
    entry_list.append(entry)
  cursor.close()
  conn.close()
  return entry_list

##########
# Update
##########

def update_row(db_name, tab_name, where_key, credit_card_type, val):
  sql = "UPDATE "+tab_name+" SET "+where_key+" = %s WHERE "+credit_card_type+" = %s"
  conn, cursor = exec_cmd(db_name, sql, val)
  cursor.close()
  conn.close()

##########
# Delete
##########

def delete_row(db_name, tab_name, where_condition="", val=()):
  sql = "DELETE FROM " + tab_name+ where_condition
  conn, cursor = exec_cmd(db_name, sql, val)
  cursor.close()
  conn.close()

##########
# Sort
##########

def sort_tab(db_name, tab_name, col_list, sort_condition):
  col_str = ", ".join(col_list)
  sql = "SELECT "+col_str+" FROM " + tab_name + sort_condition #LIMIT 3 OFFSET 2"
  conn, cursor = exec_cmd(db_name, sql)
  cursor.close()
  conn.close()

##########
# Schema
##########

def get_schema_str(schema):
  schema_str = ""
  for key in schema:
    if schema_str == "":
      schema_str = schema_str + key + " " + schema[key]
    else:
      schema_str = schema_str + ", " + key + " " + schema[key]
  return schema_str

def get_key_str(schema):
  # use join
  key_str = ""
  for key in schema:
    if key_str == "":
      key_str = key_str + key
    else:
      key_str = key_str + ", " + key
  return key_str

def get_value_str(schema):
  # In this example, id=7930.
  # Why, then, use %s in the string?
  # Because MySQLdb will convert it to a SQL literal value,
  # which is the string '7930'.
  value_str = ""
  for key in schema:
    if value_str == "":
      value_str = value_str + "%s"
    else:
      value_str = value_str + ", " + "%s"
  return value_str

##########
# Utils
##########

def get_schema():
  schema = {
    "id":"INT",
    "uid":"VARCHAR(255)",
    "credit_card_number":"VARCHAR(255)",
    "credit_card_expiry_date":"VARCHAR(255)",
    "credit_card_type":"VARCHAR(255)"
  }
  return schema

def get_sample_list():
  creditlist = [
    (7930, "ef1dbe3f-04a4-4ede-844f-391a759d61ca", "1212-1221-1121-1234", "2026-01-28", "dankort"),
    (3885, "209c9b4c-6b50-4c69-9084-bc88780aa9d6", "1211-1221-1234-2201", "2026-01-28", "jcb"),
    (7102, "59cbec34-0d6d-411c-88e0-44b9bcf3d7b9", "1234-2121-1221-1211", "2023-01-29", "jcb"),
    (7748, "5131e5fb-4573-4e50-b23e-544e2f39bdcb", "1234-2121-1221-1211", "2024-01-29", "visa"),
    (7621, "492ac7e7-7a91-4620-8fd0-8d3071b69a60", "1211-1221-1234-2201", "2025-01-28", "american_express")
  ]
  return creditlist

if __name__ == "__main__":
  db_name = "mitkardb"
  create_db(db_name)
  result = get_db_list()
  print(result)

  tab_name = "credittab"
  schema = get_schema()
  schema_str = get_schema_str(schema)
  create_tab(db_name, tab_name, schema_str)
  result = get_tab_list(db_name)
  print(result)

  key_str = get_key_str(schema)
  value_str = get_value_str(schema)
  creditlist = get_sample_list()
  insert_row(db_name, tab_name, key_str, value_str, creditlist)

  col_list = ["*"]
  #col_list = ["id", "credit_card_number", "credit_card_type"]
  
  where_condition = " WHERE credit_card_type = %s"
  val = ("visa", )
  #where_condition = " WHERE credit_card_number NOT LIKE %s"
  #val = ("%1211%", )
  result = find_row(db_name, tab_name, col_list, where_condition, val)
  for entry in result:
    print(entry)

  val = ("visa", "jcb")
  update_row(db_name, tab_name, "credit_card_type", "credit_card_type", val)

  print("ORDER BY credit_card_type")
  sort_condition = " ORDER BY credit_card_type"
  sort_tab(db_name, tab_name, col_list, sort_condition)
  result = find_row(db_name, tab_name)
  for entry in result:
    print(entry)

  print("DELETE based on credit_card_type")
  # Prevent SQL Injection
  where_condition = " WHERE credit_card_type = %s"
  val = ("visa", )
  #where_condition = " WHERE credit_card_number NOT LIKE %s"
  #val = ("%1211%", )
  delete_row(db_name, tab_name, where_condition, val)
  result = find_row(db_name, tab_name)
  for entry in result:
    print(entry)

  drop_tab(db_name, tab_name)
  drop_db(db_name)
