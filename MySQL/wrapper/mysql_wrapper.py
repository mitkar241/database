import mysql.connector

mydb = mysql.connector.connect(
  host="localhost",
  user="raktim",
  password="******"
)
print(mydb)
mycursor = mydb.cursor()
mycursor.execute("CREATE DATABASE mitkar")
mycursor.execute("SHOW DATABASES")
for x in mycursor:
  print(x)

mydb = mysql.connector.connect(
  host="localhost",
  user="raktim",
  password="******",
  database="mitkar"
)
mycursor = mydb.cursor()
#sql = "DROP TABLE mitkar"
sql = "DROP TABLE IF EXISTS mitkar"
mycursor.execute(sql)

mydb = mysql.connector.connect(
  host="localhost",
  user="raktim",
  password="******"
)
mycursor = mydb.cursor()
sql = "DROP DATABASE IF EXISTS mitkar"
mycursor.execute(sql)

