from lib2to3.pgen2 import token
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

from utils import *

class Parser:
    def __init__(self, query: String, schema: String, schema_type: String) -> None:
        self.query = query.strip(';')
        self.schema = json.load(open(schema))
        self.schema_type = json.load(open(schema_type))
        self.alias_of_relation = {}
        self.token_tree = self.parse_into_tokens(query=self.query)
        self.relation_names = self.extract_table_names(self.token_tree)
        self.conditions = self.extract_conditions(self.token_tree, self.relation_names)
        self.groupby = self.extract_groupby(self.token_tree, self.relation_names)
        self.column_names, self.aggregate_names = self.extract_columns(self.token_tree, self.relation_names)

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
        for k,v in relation_names.items():
            self.alias_of_relation[v] = k
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

    def parse_column_dict(self, column_dict, relation_names=None):
        keyy = column_dict["value"]
        key = keyy
        tableInName = False
        if len(key.split(".")) > 1:
            # contains the column name
            tablename = key.split(".")[0]
            tableInName = True
        key = column_dict["value"].split(".")[-1] # if column name of format A.Col, then only select Col
        value = key
        if column_dict.get("name") != None:
            key = column_dict.get("name")
            keyy = key

        # find out which relation the column belongs to
        if not tableInName:
            value = findTable(self.schema, value) + "." + value
        else:
            value = findTable(self.schema, value, tablename=tablename) + "." + value
        return keyy,value

    # parses aggregate dictionary
    def parse_aggregate_column_dict(self, column_dict):
        print(column_dict)
        key = column_dict["value"].items() # ("agg", "val")
        key = list(key)[0] # ("agg", "val")
        value = key[1].split(".")[-1] # if column name of format A.Col, then only select Col
        keyy = key
        key = key[0].upper() + "(" + key[1].split(".")[-1] + ")"
        if column_dict.get("name") != None:
            key = column_dict.get("name")

        # find out which relation the column belongs to
        value = findTable(self.schema, value) + "." + value
        value = keyy[0].upper() + "(" + value + ")"
        return key,value

    def extract_columns(self, token_tree, relation_names):
        column_names = {}
        aggregate_names = {}
        print(self.alias_of_relation)
        if token_tree["select"] == "*":
            # all columns
            for column in self.schema["COLUMNS"]:
                if self.alias_of_relation.get(findTable(self.schema, column["ColumnName"])) != None: # relation called somewhere in query
                    column_names[self.alias_of_relation[findTable(self.schema, column["ColumnName"])] + "." + column["ColumnName"]] =  findTable(self.schema, column["ColumnName"]) + "." + column["ColumnName"]
        else:
            if type(token_tree["select"]) == list:
                # multiple column names
                for dic in token_tree["select"]:
                    if type(dic["value"]) == dict:
                        # is an aggregate
                        key, value = self.parse_aggregate_column_dict(dic)
                        aggregate_names[key] =  value
                    elif dic == "*":
                        # aggregate till here, rest *
                        for column in self.schema["COLUMNS"]:
                            if self.alias_of_relation.get(findTable(self.schema, column["ColumnName"])) != None: # relation called somewhere in query
                                column_names[self.alias_of_relation[findTable(self.schema, column["ColumnName"])] + "." + column["ColumnName"]] =  findTable(self.schema, column["ColumnName"]) + "." + column["ColumnName"]
                    else:
                        key, value = self.parse_column_dict(dic, relation_names)
                        column_names[key] = value
            else:
                # single column name
                dic = token_tree["select"]
                key, value = self.parse_column_dict(dic, relation_names)
                column_names[key] = value
        print(column_names, aggregate_names)
        return column_names, aggregate_names