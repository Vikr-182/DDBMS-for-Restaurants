from platform import node
import pprint
from mysql.connector import cursor
import sqlparse
import mysql.connector

from os import curdir
from re import L
from select import select
from mysql.connector.constants import ServerFlag

parsed_query = sqlparse.parse(sqlparse.format(query, keyword_case='upper'))[0].tokens

token_list = []
print(sqlparse.parse(sqlparse.format(query, keyword_case='upper'))[0])

print(token_list)

# IDENTIFY TABLES


# IDENTIFY CONDITIONS


# INITIAL QUERY TREE


# DATA LOCALIZATION
