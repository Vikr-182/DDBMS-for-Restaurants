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
        self.conditions = self.extract_conditions(self.token_tree)
        self.column_names, self.aggregate_names = self.extract_columns(self.token_tree, self.relation_names)

        self.groupby = self.extract_groupby(self.token_tree)
        self.orderby = self.extract_orderby(self.token_tree)
        self.limit = None

    def parse_into_tokens(self, query):
        token_tree = parse(sqlparse.format(query, keyword_case='upper'))
        print("token_tree:")
        print(json.dumps(token_tree, indent=4))
        return token_tree

    def extract_table_names(self, token_tree):
        relation_names = {}
        tablenames = [i["TableName"] for i in self.schema["RELATIONS"]]
        if inner_join_exists(token_tree):
            # either join or multiple relations in from
            indices = get_indices_join(token_tree)
            for ind in range(len(token_tree['from'])):
                if ind not in indices:
                    dic = token_tree['from'][ind]
                    relation_names[dic['name'] if type(dic) == dict else dic] = dic['value'] if type(dic) == dict else dic
                else:
                    dic = token_tree['from'][ind]['inner join']
                    relation_names[dic['name'] if type(dic) == dict else dic] = dic['value'] if type(dic) == dict else dic
        elif type(token_tree['from']) == list:
            # no join condition
            for dic in token_tree['from']:
                relation_names[dic['name'] if type(dic) == dict else dic] = dic['value'] if type(dic) == dict else dic
        else: 
            # plain SQL query
            dic = token_tree['from']
            relation_names[dic['name'] if type(dic) == dict else dic] = dic['value'] if type(dic) == dict else dic
        print(relation_names)
        for k,v in relation_names.items():
            self.alias_of_relation[v] = k
            if v not in tablenames:
                print("TABLE NAME NOT IN SCHEMA")
                exit()
        return relation_names
    
    def parse_condition_block(self, cond, dic=[]):
        dic.append(cond)
        return dic    

    def extract_conditions(self, token_tree):
        conditions = []
        if inner_join_exists(token_tree):
            # either join or multiple relations in from , if last one is dict, it is inner join
            indices = get_indices_join(token_tree)
            for ind in indices:
                conditions.append(token_tree['from'][ind]['on'])
        if token_tree.get('where') != None:
            # where condition
            conditions.append(token_tree['where'])
        if token_tree.get('having') != None:
            # having condition
            conditions.append(token_tree['having'])
        return conditions

    def parse_column_dict(self, column_dict):
        keyy = column_dict["value"]
        key = keyy
        key = column_dict["value"].split(".")[-1] # if column name of format A.Col, then only select Col
        value = key
        if column_dict.get("name") != None:
            key = column_dict.get("name")
            keyy = key

        # find out which relation the column belongs to
        value = findTable(self.relation_names, self.schema, value) + "." + value
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
        value = findTable(self.relation_names, self.schema, value) + "." + value
        value = keyy[0].upper() + "(" + value + ")"
        return key,value

    def extract_columns(self, token_tree, relation_names):
        column_names = {}
        aggregate_names = {}
        if token_tree["select"] == "*":
            # all columns
            for table in self.schema["RELATIONS"]:
                tableName = table["TableName"]
                tableID = table["idTable"]
                if self.alias_of_relation.get(tableName) != None:
                    # table exists
                    columns = list(filter(lambda dic: dic["TableID"] - 1 == tableID, self.schema["COLUMNS"]))
                    for column in columns:
                        # relation called somewhere in query
                        column_names[self.alias_of_relation[tableName] + "." + column["ColumnName"]] = tableName + "." + column["ColumnName"]
        elif type(token_tree["select"]) == list:
            # multiple column names
            for dic in token_tree["select"]:
                if type(dic["value"]) == dict:
                    # is an aggregate
                    key, value = self.parse_aggregate_column_dict(dic)
                    aggregate_names[key] =  value
                elif dic == "*":
                    # all columns
                    for table in self.schema["RELATIONS"]:
                        tableName = table["TableName"]
                        tableID = table["idTable"]
                        if self.alias_of_relation.get(tableName) != None:
                            # table exists
                            columns = list(filter(lambda dic: dic["TableID"] - 1 == tableID, self.schema["COLUMNS"]))
                            for column in columns:
                                # relation called somewhere in query
                                column_names[self.alias_of_relation[tableName] + "." + column["ColumnName"]] = tableName + "." + column["ColumnName"]
                else:
                    key, value = self.parse_column_dict(dic)
                    column_names[key] = value
        else:
            # single column name
            dic = token_tree["select"]
            if type(dic.get("value")) == dict:
                key, value = self.parse_aggregate_column_dict(dic)
                aggregate_names[key] =  value
            else:
                key, value = self.parse_column_dict(dic)
                column_names[key] = value
        return column_names, aggregate_names
    
    def extract_groupby(self, token_tree):
        if token_tree.get('groupby') != None:
            return [token_tree['groupby']['value']]
        return []
    
    def extract_orderby(self, token_tree):
        if token_tree.get('orderby') != None:
            return token_tree['orderby']
        return {}

    def extract_limit(self, token_tree):
        if token_tree.get('limit') != None:
            return token_tree['limit']
        return {}