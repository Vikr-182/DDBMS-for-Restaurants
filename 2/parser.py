from platform import node
import pprint
from tokenize import String
from typing import Dict
from mysql.connector import cursor
import sqlparse
from moz_sql_parser import parse
import mysql.connector
import json
import sys

from os import curdir
from re import L
from select import select
from mysql.connector.constants import ServerFlag

class Parser:
    def __init__(self, query: String, schema: String, schema_type: String) -> None:
        self.query = query.strip(';')
        self.schema = json.load(open(schema))
        self.schema_type = json.load(open(schema_type))
        self.token_tree = {}
        self.relation_names = {}
        self.conditions = []
        self.token_tree = self.parse_into_tokens(query=self.query)
        self.relation_names = self.extract_table_names(self.token_tree)
        self.conditions = self.extract_conditions(self.token_tree, self.relation_names)
        self.groupby = self.extract_groupby(self.token_tree, self.relation_names)
        self.column_names = []

    def parse_into_tokens(self, query):
        token_tree = parse(sqlparse.format(query, keyword_case='upper'))
        print("token_tree:")
        print(json.dumps(token_tree, indent=4))
        return token_tree

    def extract_table_names(self, token_tree):
        relation_names = {}
        if type(token_tree['from']) == list: # either join or multiple relations in from
            if type(token_tree['from'][-1]) == dict and token_tree['from'][-1].get('inner join') != None:
                # if last one is dict, it is inner join
                for dic in token_tree['from'][:-1]:
                    relation_names[dic['name'] if type(dic) == dict else dic] = dic['value'] if type(dic) == dict else dic
                dic = token_tree['from'][-1]['inner join']
                relation_names[dic['name'] if type(dic) == dict else dic] = dic['value'] if type(dic) == dict else dic
            else:
                # where condition
                for dic in token_tree['from']:
                    relation_names[dic['name'] if type(dic) == dict else dic] = dic['value'] if type(dic) == dict else dic
        else: # plain SQL query
            dic = token_tree['from']
            relation_names[dic['name'] if type(dic) == dict else dic] = dic['value'] if type(dic) == dict else dic
        print(relation_names)
        return relation_names
    
    def parse_condition_block(self, cond, dic=[]):
        dic.append(cond)
        return dic

    def extract_conditions(self, token_tree, relation_names):
        conditions = []
        if type(token_tree['from']) == list: # either join or multiple relations in from
            if type(token_tree['from'][-1]) == dict  and token_tree['from'][-1].get('inner join') != None:
                # if last one is dict, it is inner join
                conditions = self.parse_condition_block(token_tree['from'][-1]['on'])
            else:
                # where condition
                conditions = self.parse_condition_block(token_tree['where'], conditions)
        else: # plain SQL query, where clause will have conditions
            dic = token_tree['where']
            conditions = self.parse_condition_block(token_tree['from']['inner join']['on'], conditions)
        if token_tree.get('having') != None:
            conditions = self.parse_condition_block(token_tree['having'], conditions)
        print(conditions)
        return conditions
    
    def extract_groupby(self, token_tree, relation_names):
        if token_tree.get('groupby') != None:
            return [token_tree['groupby']['value']]
        return []