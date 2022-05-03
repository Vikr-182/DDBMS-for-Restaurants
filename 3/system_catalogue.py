####################################################################################
# Generates system catalogoue and populates it
####################################################################################

import csv
import sys
import json
from itsdangerous import exc
import mysql.connector
from numpy import insert
import requests

HOSTNAME = ""
USERNAME = "user"
PASSWORD = "iiit123"
AUTH_PLUGIN = "mysql_native_password"
DB = 'preql'

SYS_CAT_TABLES = ['Columns','Relations','Fragmentation','Site']
APP_DB_TABLES = ['Restaurants', 'Menu','OrderItem','Orders','Users']
# NON_FRAG_REALTIONS = set(['Categories','Products','Inventories','Vendors','Customers','Addresses'])

with open("./new_schema.json") as f:
    DIC = json.load(f)

with open("../schema_type.json") as f:
    SCHEMA = json.load(f)

def clearAppDB(servername):
    global APP_DB_TABLES, SYS_CAT_TABLES
    PR_TABLES = set(DIC.keys())

    db = mysql.connector.connect(host = servername, user = USERNAME, password = PASSWORD, database=DB)
    cursor = db.cursor()

    QUERY = "show tables"
    cursor.execute(QUERY)
    tables = set([ table[0] for table in cursor.fetchall() ])

    for table in tables:
        QUERY = "DROP TABLE {}".format(table)
        cursor.execute(QUERY)

    cursor.close()
    db.commit()
    db.close()

def createTable(servername, tableName, columns):
    print(tableName, columns)
    QUERY = " CREATE TABLE IF NOT EXISTS {} ({});".format(tableName, columns)
    print(QUERY)
    db = mysql.connector.connect(host = servername, user = USERNAME, password = PASSWORD, database=DB)
    cursor = db.cursor()
    cursor.execute(QUERY)
    db.commit()
    return

def insertIntoTable(servername, tableName, keys, values):
    QUERY = " INSERT INTO {}  ({}) VALUES ({});".format(tableName,keys, values)
    print(QUERY)
    db = mysql.connector.connect(host = servername, user = USERNAME, password = PASSWORD, database=DB)
    cursor = db.cursor()
    resp = cursor.execute(QUERY)
    db.commit()
    print (resp)
    return

def retval(a):
    if type(a) == str:
        return "\'" + a + "\'"
    elif type(a) == int:
        return str(a)
    
def createDB(servername):
    QUERY = " CREATE DATABASE {};".format(DB)
    print(QUERY)
    db = mysql.connector.connect(host = servername, user = USERNAME, password = PASSWORD)
    cursor = db.cursor()
    cursor.execute(QUERY)
    db.commit()
    return    

# Conditionals, Fragments, FragmentsAttributesList
if __name__ == '__main__':
    for site in DIC["SITES"]:
        # in each site create the db
        try:
            createDB(site["ip"])
        except:
            pass
        #clearAppDB(site["ip"])
        for tableName, tableRows in DIC.items():
            print('='*30)
            print (tableRows)
            print (tableName)
            print('='*30)
            schem = SCHEMA[tableName]
            string = []
            for key, val in schem.items():
                string.append(key + " " + val)
            print(string)
            createTable(site["ip"], tableName, ",".join(string))
            for row in tableRows:
                print (row.keys())
                keys = ",".join([str(a) for a in list(row.keys())])
                print (row.values())
                values = ",".join([retval(a) for a in list(row.values())])
                insertIntoTable(site["ip"], tableName, keys, values)
