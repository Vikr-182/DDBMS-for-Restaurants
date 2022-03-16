from platform import node
import pprint
from tokenize import String
from mysql.connector import cursor
import sqlparse
import mysql.connector

from os import curdir
from re import L
from select import select
from mysql.connector.constants import ServerFlag

class Parser:
    def __init__(self, query: String) -> None:
        self.query = query.strip(';')
        self.token_list = []
        self.table_names = []
        self.conditions = []
        self.parse_into_tokens(query=query)
    
    def parse_into_tokens(self, query):
        token_list = []
        parsed_query = sqlparse.parse(sqlparse.format(query, keyword_case='upper'))[0].tokens
        for i in parsed_query:
            if not i.is_whitespace:
                try:
                    token = []
                    for j in i.get_identifiers():
                        token.append(j)
                    token_list.append(token)
                except:
                    token_list.append(i)
        self.token_list = token_list

    def extract_table_names(self):
        
        pass