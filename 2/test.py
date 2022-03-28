import sqlparse
query = 'select * from foo;'
sqlparse.split(query)
print(query)