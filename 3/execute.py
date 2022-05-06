from logging import root
from parser import Parser
import site
import sqlparse
import os
import pickle
import requests

import networkx as nx

# PARSING QUERY
print("Enter your mySQL Query : ")
# query = "SELECT M.Cuisine, SUM(R.RestaurantID) FROM Restaurant R, Menu M WHERE ((R.Zone=='NORTH' OR R.Zone==WEST) OR (R.RestaurantID==1)) and (R.RestaurantName != M.Description) and (R.Zone!='EAST')"
# query = "SELECT * FROM Users U INNER JOIN Orderr O ON U.idUsers=O.UserID"
query = "SELECT * FROM menu WHERE menu.itemprice<40"
parser = Parser(query=query, schema="../schema.json", schema_type="../schema_type.json")

from node import *
from tree import *
tree = Tree(parser=parser)
tree.build(parser=parser)
tree.localize(parser=parser)
tree.visualize()

G = tree.G
root_nodes = [x for x in G.nodes() if G.out_degree(x)==0]
print(root_nodes)

root_node = root_nodes[-1]
print(list(nx.bfs_edges(G, root_node, True)))

start = list(nx.bfs_edges(G, root_node, True))[-1]

sites = ["10.3.5.211", "10.3.5.208", "10.3.5.204", "10.3.5.205"]

cnx = mysql.connector.connect(user="user", password="iiit123",host="CP5",database="preql")
cursor = cnx.cursor()

PORT = 5001


def drop_table(tablename):
    try:
        query = "DROP TABLE {}".format(tablename)
        cursor.execute(query)
    except:
        pass

def make_copy(src, dest):
    try:
        query = "CREATE TABLE {} AS (SELECT * FROM {})".format(dest, src)
        cursor.execute(query)
    except:
        pass

def execute(G, nodenum):
    data = G.nodes[nodenum]['data']
    content = data.content
    print("POPOPO", content, data.nodetype)
    if data.executed == False:
        c1 = data.child1
        c2 = data.child2
        nodetype = data.nodetype
        if c1 != None:
            # execute at c1
            print("C1)")
            execute(G, c1)
            pass
        if c2 != None and c1 != c2:
            # execute at c2
            print("C2@)")
            execute(G, c2)
            pass
        if nodetype == "SELECT":
            # only c1
            from moz_sql_parser import format
            cond = content["columns"]
            keyy = list(cond.keys())[0]
            print(cond, "BBBBBBBBBBBBBBBBBB")
            cond[keyy] = ["TEMP_{}_TEMP".format(c1) + "." + cond[keyy][0].split(".")[1], cond[keyy][1]]
            print(cond, "UUUUUUUUUUUUUUUUUU")
            select_query = format({"from":"TEMP_{}_TEMP".format(c1), "select":["*"], "where": cond})
            drop_table("TEMP_" + str(nodenum) + "_TEMP")
            query = "CREATE TABLE TEMP_{}_TEMP AS ({})".format(nodenum, select_query)
            print("queryAA", query)
            cursor.execute(query)
            pass
        elif nodetype == "PROJECT":
            # only c1
            # should be having c1_TEMP
            print(content["columns"],"AAA")
            columns = [i.split(".")[1] for i in content["columns"]]
            print(columns,"AAA")
            string = ""
            for i in range(len(columns) - 1):
                string += columns[i]
                string += ","
            string += columns[-1]
            drop_table("TEMP_" + str(nodenum) + "_TEMP")
            query = "CREATE TABLE TEMP_{}_TEMP AS (SELECT {} FROM TEMP_{}_TEMP)".format(nodenum, string, c1)
            print(string)
            cursor.execute(query)
            pass
        elif nodetype == "UNION":
            # take union
            drop_table(str(nodenum) + "_TEMP")
            drop_table("TEMP_" + str(nodenum) + "_TEMP")
            query = "CREATE TABLE TEMP_{}_TEMP AS ((SELECT * FROM TEMP_{}_TEMP) UNION (SELECT * FROM TEMP_{}_TEMP))".format(nodenum, c1, c2)
            cursor.execute(query)
            pass
        elif nodetype == "JOIN":
            # take join
            print(content["column"])
            col1 = content["column"][0].split(".")[1]
            col2 = content["column"][1].split(".")[1]
            query = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME=\"TEMP_{}_TEMP\";".format(c1)
            cursor.execute(query)
            rows1 = cursor.fetchall()
            rows1 = [i[0] for i in rows1]

            query = "select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME=\"TEMP_{}_TEMP\";".format(c2)
            cursor.execute(query)
            rows2 = cursor.fetchall()
            rows2 = [i[0] for i in rows2]

            print(rows1)
            print(col1)
            rows1.remove(col1)
            rows2.remove(col2)

            print(rows1, "AAAA")
            col1s = rows1
            col2s = rows2
            string = ""
            for i in range(len(col1s) - 1):
                string += "TEMP_{}_TEMP".format(c1) + "." + col1s[i]
                string += ","
            string += col1s[-1]
            string += ","

            for i in range(len(col2s) - 1):
                string += "TEMP_{}_TEMP".format(c2) + "." + col2s[i]
                string += ","
            string += col2s[-1]
            string += ","
            string += "TEMP_{}_TEMP.{}".format(c1, col1)

            print(string)
            drop_table("TEMP_" + str(nodenum) + "_TEMP")
            query = "CREATE TABLE TEMP_{}_TEMP AS (SELECT {} FROM TEMP_{}_TEMP INNER JOIN TEMP_{}_TEMP ON TEMP_{}_TEMP.{}=TEMP_{}_TEMP.{})".format(nodenum, string, c1, c2, c1, col1, c2, col2)
            print(query)
            cursor.execute(query)
            pass
        elif nodetype == "RELATION":
            # fetch relation
            siteid = int(content['site'])
            table =  content['name'].split("@")[0]
            data = {
                "table": table + "_" + str(siteid)
            }
            r = requests.post(url = "http://" + sites[int(siteid)] + ":{}/get-data".format(PORT), data=data)
            sqlfile = r.text
            fil = open("{}.sql".format(table), "w")
            fil.write(sqlfile)
            fil.close()
            os.system("mysql -u user -piiit123 preql < {}.sql".format(table))
            drop_table("TEMP_" + str(nodenum) + "_TEMP")
            query = "CREATE TABLE {} AS (SELECT * FROM {})".format("TEMP_" + str(nodenum) + "_TEMP", table + "_" + str(siteid - 1))
            cursor.execute(query)
            pass
        elif nodetype == "GROUPBY":
            pass
        G.nodes[nodenum]['data'].executed = True
    else:
        G.nodes[nodenum]['data'].executed = True
        return -1
# nx.bfs_edges(G, 'n')

execute(G, root_node)
