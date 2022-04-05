from parser import Parser
import sqlparse
# PARSING QUERY
print("Enter your mySQL Query : ")
# query = input()
# query = "SELECT SUM(Restaurant.RestaurantID), Restaurant.RestaurantID, Menu.Description, Restaurant.RestaurantName FROM Restaurant INNER JOIN Menu ON Restaurant.RestaurantID=Menu.RestaurantID WHERE Menu.MenuID > 10 ORDER BY Menu.MenuID ASC LIMIT 10;"
# query = "SELECT * FROM Menu AS CUS INNER JOIN Restaurant AS ORD ON CUS.MenuID = ORD.MenuID AND CUS.Name = 'John'"
query = "SELECT M.Cuisine, SUM(R.RestaurantID) FROM Restaurant R, Menu M WHERE ((R.Zone=='NORTH' OR R.Zone==WEST) OR (R.RestaurantID==1)) and (R.RestaurantName != M.Description) and (R.Zone!='EAST')"

query = "SELECT SUM(R.RestaurantID) FROM Restaurant R INNER JOIN Users U on U.idUsers=R.RestaurantID INNER JOIN Menu M on R.MenuID=M.MenuID"
# query = "SELECT * FROM Users U INNER JOIN Orderr O ON U.idUsers=O.UserID"
# query = "SELECT P.PNO FROM EMP E, ASG A, PROJ P WHERE E.ENO=A.ENO AND A.PNO=P.PNO"
parser = Parser(query=query, schema="../schema.json", schema_type="../schema_type.json")
# parser = Parser(query=query, schema="../schema_test.json", schema_type="../schema_type.json")

# parsed = sqlparse.parse(query)[0]
# print(parsed.tokens)

from node import *
from tree import *
tree = Tree(parser=parser)
tree.build(parser=parser)
# tree.localize(parser=parser)
tree.visualize()
