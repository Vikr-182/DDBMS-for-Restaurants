from query import Parser

# PARSING QUERY
print("Enter your mySQL Query : ")
query = input()
parser = Parser(query=query)

print(parser.token_list)