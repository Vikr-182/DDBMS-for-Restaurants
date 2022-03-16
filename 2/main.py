from parser import Parser

# PARSING QUERY
print("Enter your mySQL Query : ")
# query = input()
# query = "SELECT Orders.OrderID, Customers.CustomerName, Orders.OrderDate FROM Orders INNER JOIN Customers ON Orders.CustomerID=Customers.CustomerID;"
query = "SELECT * FROM dbo.Customers AS CUS INNER JOIN dbo.Orders AS ORD ON CUS.CustomerID = ORD.CustomerID AND CUS.FirstName = 'John' GROUP BY dbo.Customers HAVING dbo.Customers > 10 "
# query = "SELECT * FROM EMPLOYEE E, PROJECT P WHERE E.Pid=P.Pid"
# query = "SELECT * FROM EMPLOYEE E, PROJECT P WHERE (E.Pid=P.Pid and E.Essn==1) OR (E.Cid=P.Cid)"
parser = Parser(query=query, schema="../schema.json", schema_type="../schema_type.json")

# print(parser.token_list)