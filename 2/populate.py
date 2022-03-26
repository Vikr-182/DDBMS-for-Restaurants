import pandas as pd 
import sys
import json
import csv


schema = json.load(open('./new_schema.json'))
relationlist = schema['RELATIONS']
fraglist = schema['FRAGMENTATION']
bigcollist = schema['COLUMNS']


table1 = sys.argv[1]
table2 = sys.argv[2]
table3 = sys.argv[3]


####################################################################################
# Generates system catalogoue and populates it
####################################################################################

import csv
import sys
from venv import create
from matplotlib.pyplot import table
import mysql.connector
from numpy import insert
import requests

import json

HOSTNAME = ""
USERNAME = "user"
PASSWORD = "iiit123"
AUTH_PLUGIN = "mysql_native_password"
DB = 'preql'

SYS_CAT_TABLES = ['Columns','Relations','Fragmentation','Site']
APP_DB_TABLES = ['Restaurants', 'Menu','OrderItem','Orders','Users']
# NON_FRAG_REALTIONS = set(['Categories','Products','Inventories','Vendors','Customers','Addresses'])



def clearAppDB(servername):
    global APP_DB_TABLES, SYS_CAT_TABLES
    PR_TABLES = set(schema.keys())

    db = mysql.connector.connect(host = servername, user = USERNAME, password = PASSWORD, database=DB)
    cursor = db.cursor()

    QUERY = "show tables"
    cursor.execute(QUERY)
    tables = set([ table[0] for table in cursor.fetchall() ])

    for table in tables-PR_TABLES:
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
    QUERY = " INSERT INTO TABLE {} ({}) VALUES ({});".format(tableName,keys, values)
    print(QUERY)
    db = mysql.connector.connect(host = servername, user = USERNAME, password = PASSWORD, database=DB)
    cursor = db.cursor()
    cursor.execute(QUERY)
    db.commit()
    return

def createDB(servername):
    QUERY = " CREATE DATABASE {};".format(DB)
    print(QUERY)
    db = mysql.connector.connect(host = servername, user = USERNAME, password = PASSWORD)
    cursor = db.cursor()
    cursor.execute(QUERY)
    db.commit()
    return    



csvfile = open('given.csv')

spamreader = csv.reader(csvfile)


count = 0 
for row in spamreader:

    if count == 0 :
        count+= 1
        continue

tabldf1 = pd.read_csv(table1)
tabldf2 = pd.read_csv(table2)
tabldf3 = pd.read_csv(table3)

tablenames = [table1.split('.')[0],table2.split('.')[0],table3.split('.')[0]]
fragtype = []


for sit in schema["SITES"]:
    try:
        clearAppDB(sit["ip"])
        createDB(sit["ip"])
    except:
        pass


for i in tablenames:
    for j in relationlist:
        if j['TableName'] == i:
            fragtype.append(j["FragmentationType"])



for i in range(len(tablenames)):
    print (fragtype)
    if fragtype[i] == 'V':
        for j in relationlist:
            print (j)
            if j['TableName'] == tablenames[i]:
                tableid = j['idTable']

        for j in fraglist:
            if j['RelationId'] == tableid:
                fragcols = j['FragmentationCondition']
                site = int(j['SiteId'])
                SITE = schema["SITES"][site - 1]
                fragcolname = []
                #print ('='*30)
                #print (fragcols)
                #print ('='*30)
                for col in fragcols.split(','):
                    for k in bigcollist:
                        if k['ColumnID'] == col :
                            fragcolname.appned(k)
                
                print(j["FragmentationCondition"])
                keys = [bigcollist[int(colid)]["ColumnName"] + " " + bigcollist[int(colid)]["ColumnType"] for colid in j["FragmentationCondition"].split(',')]
                subset = tabldf1[fragcolname]
                for onerow in subset:
                    print (onerow)
                # insertIntoTable(SITE["ip"], tablenames[i], ','join(keys), values)

