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

with open("../schema.json") as f:
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

# Conditionals, Fragments, FragmentsAttributesList
if __name__ == '__main__':
    for sit in DIC["SITES"]:
        try:
            clearAppDB(sit["ip"])
            createDB(sit["ip"])
        except:
            pass
    for row in DIC["FRAGMENTATION"]:
        RELATION = DIC["RELATIONS"][row["RelationId"] - 1]
        SITE = DIC["SITES"][row["SiteId"] - 1]
        if RELATION["FragmentationType"] != "V":
            # just create the table from similar columns
            similarcols = []
            for i in range(len(DIC["COLUMNS"])):
                if DIC["COLUMNS"][i]["TableID"] == row["RelationId"]:
                    similarcols.append(i)
            string = [DIC["COLUMNS"][colid]["ColumnName"] + " " + DIC["COLUMNS"][colid]["ColumnType"] for colid in similarcols]
            createTable(SITE["ip"], RELATION["TableName"], columns=",".join(string))
        else:
            # create table with vertical splits
            print(row["FragmentationCondition"])
            string = [DIC["COLUMNS"][int(colid)]["ColumnName"] + " " + DIC["COLUMNS"][int(colid)]["ColumnType"] for colid in row["FragmentationCondition"].split(",")]
            createTable(SITE["ip"], RELATION["TableName"], columns=",".join(string))
