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

sites_mapping = {
        "10.3.5.211":0,
        "10.3.5.208":1,
        "10.3.5.204":2,
        "10.3.5.205":3
}

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
    QUERY = " INSERT INTO {} ({}) VALUES ({});".format(tableName + "_" + str(sites_mapping[servername]),keys, values)
    print ('*'*30)
    print(QUERY)
    print (SITE)
    print ('*'*30)
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

def parsehcond(rowobj,cond):
    cond=cond.split('.')[1]
    if '<=' in cond:
        colname = cond.split('<=')[0]
        colval = cond.split('<=')[1]
        if int(rowobj[colname]) <= int(colval):
            return True
        else :
            return False 
    elif '<' in cond :
        colname = cond.split('<')[0]
        colval = cond.split('<')[1]
        if int(rowobj[colname]) < int(colval):
            return True
        else :
            return False   
    elif '>=' in cond :
        colname = cond.split('>=')[0]
        colval = cond.split('>=')[1]
        if int(rowobj[colname]) >= int(colval):
            return True
        else :
            return False  
    elif '>' in cond :
        colname = cond.split('>')[0]
        colval = cond.split('>')[1]
        if int(rowobj[colname]) > int(colval):
            return True
        else :
            return False  
    elif '!=' in cond :
        colname = cond.split('!=')[0]
        colval = cond.split('!=')[1]
        try :
            if int(rowobj[colname]) != int(colval):
                return True
            else :
                return False 
        except :
            if rowobj[colname].strip() != colval.strip():
                return True
            else :
                return False 

    elif '==' in cond :
        colname = cond.split('==')[0]
        colval = cond.split('==')[1]
        try:
            if int(rowobj[colname]) == int(colval):
                return True
            else :
                return False
        except Exception as e :
            print (e)
            print (rowobj[colname])
            if rowobj[colname].strip() == colval.strip():
                return True
            else :
                return False
                        

def parsedhcond(rowobj,cond):
    condlist = cond.split(',')
    #print (condlist,"lololololololol")
    thatTableKey = condlist[1]
    forKey = condlist[0]
    #print (condlist[2].split('.')[1])
    cond=condlist[2].split('.')[1]
    #print (cond)
    thatTableName = condlist[2].split('.')[0]
    #print (thatTableName)
    for aname in range(len(tablenames)):
        if tablenames[aname] == thatTableName:
            tableind = aname


    """
    print (rowobj, forKey,"lol rowobj")
    print (type(dflist[tableind]))
    print (rowobj[forKey])
    print ("====")
    print (rowobj)
    print ("YAAR BAHAR NIKAL")
    """
    rowdf = rowobj.to_frame().T
    mergedf = pd.merge(rowdf, dflist[tableind], left_on=forKey, right_on=thatTableKey)
    #print (rowdf.head())
    print ('-*-'*30)
    print (mergedf)
    print (cond)
    print ('-*-'*30)

    mergedf = mergedf.iloc[0]
    if '<=' in cond:
        colname = cond.split('<=')[0]
        colval = cond.split('<=')[1]
        if int(mergedf[colname]) <= int(colval):
            return True
        else :
            return False 
    elif '<' in cond :
        colname = cond.split('<')[0]
        colval = cond.split('<')[1]
        if int(mergedf[colname]) < int(colval):
            return True
        else :
            return False   
    elif '>=' in cond :
        colname = cond.split('>=')[0]
        colval = cond.split('>=')[1]
        if int(mergedf[colname]) >= int(colval):
            return True
        else :
            return False  
    elif '>' in cond :
        colname = cond.split('>')[0]
        colval = cond.split('>')[1]
        if int(mergedf[colname]) > int(colval):
            return True
        else :
            return False  
    elif '!=' in cond :
        colname = cond.split('!=')[0]
        colval = cond.split('!=')[1]
        try :
            if int(mergedf[colname]) != int(colval):
                return True
            else :
                return False 
        except :
            if mergedf[colname].strip() != colval.strip():
                return True
            else :
                return False 
    elif '==' in cond :
        colname = cond.split('==')[0]
        colval = cond.split('==')[1]
        #print (mergedf[colname].strip(),colval.strip(),"AAAAAAAAAAA")
        try:
            if int(mergedf[colname]) == int(colval):
                return True
            else :
                return False
        except:
            if mergedf[colname].strip() == colval.strip():
                return True
            else :
                return False
                

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

dflist = [tabldf1,tabldf2,tabldf3]
tablenames = [table1.split('.')[0],table2.split('.')[0],table3.split('.')[0]]
fragtype = []



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
                        #print (col,k['ColumnID'])
                        if int(k['ColumnID']) == int(col) :
                            fragcolname.append(k['ColumnName'])
                #print(j["FragmentationCondition"])
                #keys = [bigcollist[int(colid)]["ColumnName"] + " " + bigcollist[int(colid)]["ColumnType"] for colid in j["FragmentationCondition"].split(',')]
                keys = [bigcollist[int(colid)]["ColumnName"]  for colid in j["FragmentationCondition"].split(',')]
                #print (dflist[i].columns)
                #print (fragcolname)
                subset = dflist[i][fragcolname]
                #print (subset)
                #print (tabldf1)
                #print (keys)
                for index,onerow in subset.iterrows():
                    print ('='*30)
                    values = []
                    for kkl in fragcolname:
                        print (onerow[kkl])
                        try:
                            int(onerow[kkl])
                            values.append(onerow[kkl])
                        except:
                            values.append("'"+onerow[kkl]+"'")
                    print(keys)
                    keys = [str(kkk) for kkk in keys]
                    values = [str(kkk) for kkk in values]
                    print(values)
                    print(tablenames[i],','.join(keys),','.join(values))
                    insertIntoTable(SITE["ip"], tablenames[i], ','.join(keys), ','.join(values))
                    print ('='*30)



    elif fragtype[i] == 'H':
        for j in relationlist:
            print (j)
            if j['TableName'] == tablenames[i]:
                tableid = j['idTable']

        for j in fraglist:
            if j['RelationId'] == tableid:
                fragcond = j['FragmentationCondition']
                site = int(j['SiteId'])
                SITE = schema["SITES"][site - 1]
                fragcolname = []

                for k in bigcollist:
                    #print (col,k['ColumnID'])
                    if int(k['TableID']) == int(tableid) :
                        fragcolname.append(k['ColumnName'])

                keys = fragcolname

                subset = dflist[i][fragcolname]
                #print (subset)

                #print (keys)
                for index,onerow in subset.iterrows():
                    if parsehcond(onerow,fragcond):
                        print ('='*30)
                        values = []
                        for kkl in fragcolname:
                            try:
                                int(onerow[kkl])
                                values.append(onerow[kkl])
                            except:
                                values.append("'"+onerow[kkl]+"'")
                            print (onerow[kkl])
                        print(keys)
                        keys = [str(kkk) for kkk in keys]
                        values = [str(kkk) for kkk in values]
                        print(values)
                        print(tablenames[i],','.join(keys),','.join(values))
                        insertIntoTable(SITE["ip"], tablenames[i], ','.join(keys), ','.join(values))
                        print ('='*30)



    elif fragtype[i] == 'DH':
        for j in relationlist:
            print (j)
            if j['TableName'] == tablenames[i]:
                tableid = j['idTable']

        for j in fraglist:
            if j['RelationId'] == tableid:
                fragcond = j['FragmentationCondition']
                site = int(j['SiteId'])
                SITE = schema["SITES"][site - 1]
                fragcolname = []

                for k in bigcollist:
                    #print (col,k['ColumnID'])
                    if int(k['TableID']) == int(tableid) :
                        fragcolname.append(k['ColumnName'])

                keys = fragcolname

                subset = dflist[i][fragcolname]
                #print (subset)

                #print (keys)
                for index,onerow in subset.iterrows():
                    if parsedhcond(onerow,fragcond):
                        print ('='*30)
                        values = []
                        for kkl in fragcolname:
                            try:
                                int(onerow[kkl])
                                values.append(onerow[kkl])
                            except:
                                values.append("'"+onerow[kkl]+"'")
                            print (onerow[kkl])
                        print(keys)
                        keys = [str(kkk) for kkk in keys]
                        values = [str(kkk) for kkk in values]
                        print(values)
                        print(tablenames[i],','.join(keys),','.join(values))
                        insertIntoTable(SITE["ip"], tablenames[i], ','.join(keys), ','.join(values))
                        print ('='*30)











    else :
        for j in relationlist:
            print (j)
            if j['TableName'] == tablenames[i]:
                tableid = j['idTable']

        for j in fraglist:
            if j['RelationId'] == tableid:
                fragcond = j['FragmentationCondition']
                site = int(j['SiteId'])
                SITE = schema["SITES"][site - 1]
                fragcolname = []
                
                for k in bigcollist:
                    #print (col,k['ColumnID'])
                    if int(k['TableID']) == int(tableid) :
                        fragcolname.append(k['ColumnName'])

                keys = fragcolname
                subset = dflist[i][fragcolname]
                #print (subset)
                #print (tabldf1)
                #print (keys)
                for index,onerow in subset.iterrows():
                    print ('='*30)
                    values = []
                    for kkl in fragcolname:
                        try:
                            int(onerow[kkl])
                            values.append(onerow[kkl])
                        except:
                            values.append("'"+onerow[kkl]+"'")
                    print(keys)
                    keys = [str(kkk) for kkk in keys]
                    values = [str(kkk) for kkk in values]
                    print(values)
                    print(tablenames[i],','.join(keys),','.join(values))
                    insertIntoTable(SITE["ip"], tablenames[i], ','.join(keys), ','.join(values))
                    print ('='*30)





