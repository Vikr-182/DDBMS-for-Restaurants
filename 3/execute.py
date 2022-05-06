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
query = "SELECT * FROM Orderr"
parser = Parser(query=query, schema="../schema.json", schema_type="../schema_type.json")

from node import *
from tree import *
tree = Tree(parser=parser)
tree.build(parser=parser)
tree.localize(parser=parser)
with open("tree.pkl", "wb") as outp:
    pickle.dump(tree, outp, pickle.HIGHEST_PROTOCOL)    
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

def execute(G, nodenum):
    data = G.nodes[nodenum]['data']
    content = data.content
    print("POPOPO", content)
    if data.executed == False:
        c1 = data.child1
        c2 = data.child2
        nodetype = data.nodetype
        if c1 != None:
            # execute at c1
            execute(G, c1)
            pass
        if c2 != None and c1 != c2:
            # execute at c2
            execute(G, c2)
            pass
        if nodetype == "SELECT":
            # only c1
            pass
        elif nodetype == "PROJECT":
            # only c1
            
            pass
        elif nodetype == "UNION":
            # take union
            pass
        elif nodetype == "JOIN":
            # take join
            pass
        elif nodetype == "RELATION":
            # fetch relation
            siteid = content['site'] - 1
            table =  content['name'].split("@")[0]
            data = {
                "table": table
            }
            r = requests.post(url = "http://" + sites[int(siteid)] + ":5000/get-data", data=data)
            sqlfile = r.text
            fil = open("{}.sql".format(table), "w")
            fil.write(sqlfile)
            fil.close()
            os.system("mysql -u user -piiit123 preql < {}.sql".format(table))
            query = "CREATE TABLE {} AS {}".format(nodenum, table)
            cursor.execute(query)
            pass
        G.nodes[nodenum]['data'].executed = True
    else:
        G.nodes[nodenum]['data'].executed = True
        return -1
# nx.bfs_edges(G, 'n')

execute(G, root_node)