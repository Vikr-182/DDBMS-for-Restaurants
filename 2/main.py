from parser import Parser
import sqlparse
# PARSING QUERY
print("Enter your mySQL Query : ")
# query = input()
# query = "SELECT SUM(Restaurant.RestaurantID), Restaurant.RestaurantID, Menu.Description, Restaurant.RestaurantName FROM Restaurant INNER JOIN Menu ON Restaurant.RestaurantID=Menu.RestaurantID;"
# query = "SELECT * FROM Menu AS CUS INNER JOIN Restaurant AS ORD ON CUS.MenuID = ORD.MenuID AND CUS.Name = 'John' GROUP BY Menu.MenuID HAVING Menu.MenuID > 10"
# query = "SELECT * FROM Restaurant R, Menu M WHERE Restaurant.MenuID=Menu.MenuID"
query = "SELECT * FROM Menu AS CUS INNER JOIN Restaurant AS ORD ON CUS.MenuID = ORD.MenuID AND CUS.Name = 'John' GROUP BY CUS.MenuID HAVING CUS.MenuID > 10"
# query = "SELECT * FROM Restaurant R, Menu M WHERE R.MenuID=M.MenuID"
# query = "SELECT M.Cuisine FROM Restaurant R, Menu M WHERE (R.Zone=NORTH or R.Zone=WEST) and (R.RestaurantID==1 and M.Cuisine==Italian) and (R.RestaurantName != M.Description)"
parser = Parser(query=query, schema="../schema.json", schema_type="../schema_type.json")

# parsed = sqlparse.parse(query)[0]
# print(parsed.tokens)

from node import *
tree = Tree(parser=parser)

# print(parser.token_list)