from ntpath import join
from operator import truediv
from platform import node
import pprint
from mysql.connector import cursor
import sqlparse
import mysql.connector

from os import curdir
from re import L
from select import select
from mysql.connector.constants import ServerFlag

import networkx as nx
import matplotlib.pyplot as plt

from utils import *
from networkx.drawing.nx_pydot import graphviz_layout

# INITIAL QUERY TREE
# node
class Node:
    def __init__(self, nodenum, content, nodetype="cond") -> None:
        self.nodetype = nodetype
        self.nodenum = nodenum # node number in nx graph
        self.content = content
        self.child1 = None
        self.child2 = None
        self.parent = None
        self.usefulcolumns = []
        self.mapping = {}

class Tree:
    def __init__(self, parser) -> None:
        self.root = None
        self.parser = parser
        self.table_to_num = {}
        self.G = nx.DiGraph()
        self.join_conditions = []
        self.select_conditions = {}
        self.or_conditions = []
        self.condnum = 0
        self.cond_to_num = {}
        self.and_nodes = False
        self.operator_to_sign = {
            "eq": "==",
            "lt":"<",
            "le":"<=",
            "gt":">",
            "ge":">=",
            "neq":"!=",
            "cross": "X"
        }
        self.select_conditions = {}
        self.select_endpoints = {}
        self.nodenum = 0
        self.labeldict = {}
        self.join_mapping = {}
        self.build(parser=parser)
        print(self.labeldict)
        print(list(self.G.nodes))
        print(list(self.G.edges))
        pos = graphviz_layout(self.G, prog="dot")
        nx.draw(self.G, labels=self.labeldict, with_labels = True, pos=pos)
        plt.savefig("filename.png")

    def add_edge(self, u, v):
        print(type(self.G.nodes[u]))
        self.G.nodes[u]['data'].parent = v
        self.G.nodes[v]['data'].child1 = u
        self.G.nodes[v]['data'].child2 = u
        self.G.add_edge(v, u)

    def parse_condition(self, condition, parser):
        type_ = get_type(condition, parser)
        if type_ != "or":
            # literal eq
            cond = self.operator_to_sign[list(condition.items())[0][0]]
            colname = list(condition.items())[0][1][0]
            literal = list(condition.items())[0][1][1]
            relation_name = get_table(parser, colname)
            if self.select_conditions.get(relation_name) == None:
                self.select_conditions[relation_name] = []
            self.select_conditions[relation_name].append((condition, self.nodenum))
            node = Node(nodenum=self.nodenum, content=condition, nodetype="select")
            self.cond_to_num[self.condnum] = self.nodenum
            self.G.add_node(self.nodenum, data=node)
            self.labeldict[self.nodenum] =  colname + cond + str(literal)
        else:
            # put as 1 node
            self.or_conditions.append((condition, self.nodenum))

    def add_node(self, parser, k, cond, nodetype):
        node = Node(self.condnum, content=k, nodetype=nodetype)
        cond1 = k[cond][0]
        cond2 = k[cond][1]
        rel1 = get_table(parser, cond1)
        rel2 = get_table(parser, cond2)
        end1 = self.select_endpoints[rel1]
        end2 = self.select_endpoints[rel2]
        self.G.add_node(self.nodenum, data=node)
        if type(cond2) == dict:
            cond2 = cond2["literal"]
        self.labeldict[self.nodenum] = nodetype.upper() + " " + cond1 + " " + self.operator_to_sign[cond] + " " + str(cond2)
        self.add_edge(end1, self.nodenum)
        self.add_edge(end2, self.nodenum)
        self.select_endpoints[rel1] = self.nodenum
        self.select_endpoints[rel2] = self.nodenum
        self.nodenum = self.nodenum + 1
        self.condnum = self.condnum + 1

    def build(self, parser):
        # build itself using the parser
        self.nodenum = 0
        table_to_num = {}
        # build nodes of tables
        for table in list(parser.relation_names.keys()):
            node = Node(nodenum=self.nodenum, content=table, nodetype="RELATION")
            self.G.add_node(self.nodenum, data=node)
            self.labeldict[self.nodenum] = table
            table_to_num[table] = self.nodenum
            self.select_endpoints[table] = self.nodenum
            self.nodenum = self.nodenum + 1
        self.table_to_num = table_to_num

        # build condition tree
        print(parser.conditions, "AAA")
        for conditions in parser.conditions:
            condition = conditions
            type_ = get_type(condition, parser)
            if type_ == "join":
                # join condition, put in join queue
                self.join_conditions.append((condition, self.condnum))
            else:
                if type_ != "and":
                    self.parse_condition(condition=condition, parser=parser)
                    self.condnum = self.condnum + 1
                    self.nodenum = self.nodenum + 1
                else:
                    print("OO")
                    # and condition
                    for condition in conditions['and']:
                        print(self.condnum, "B")
                        type_ = get_type(condition, parser)
                        print(condition, type_)
                        if type_ == "join":
                            self.join_conditions.append((condition, self.nodenum))
                        else:
                            self.parse_condition(condition=condition, parser=parser)
                            self.condnum = self.condnum + 1
                            self.nodenum = self.nodenum + 1

        print("select conditions:" , self.select_conditions)
        print("join conditions:" , self.join_conditions)
        print("or conditions:" , self.or_conditions)

        # push select down
        for k,v in self.select_conditions.items():
            prevnode = self.table_to_num[k]
            for vtup in v:
                condition, condnum = vtup
                cond = self.operator_to_sign[list(condition.items())[0][0]]
                colname = list(condition.items())[0][1][0]
                literal = list(condition.items())[0][1][1]
                nodenum = condnum
                self.add_edge(prevnode, nodenum)
                if type(literal) == dict:
                    literal = literal["literal"]
                self.labeldict[nodenum] = "SELECT " + colname + cond + str(literal)
                prevnode = condnum
            self.select_endpoints[k] = prevnode

        # join conditions
        for k,v in self.join_conditions:
            cond = next(iter(k))
            if cond == "eq":
                self.add_node(parser,k,cond,nodetype="join")

        for k,v in self.join_conditions:
            cond = next(iter(k))
            if cond != "eq":
                # break into 2
                self.add_node(parser,{"cross": k[cond]},"cross",nodetype="cross")
                self.add_node(parser,k,cond,nodetype="select")
            pass
            
        # handle or conditions
        rel_name = next(iter(self.parser.relation_names))
        last_ep = self.select_endpoints[rel_name]
        for k in self.or_conditions:
            node = Node(self.nodenum, content=k[0], nodetype="or")
            self.cond_to_num[self.condnum] = self.nodenum
            cond = "or"
            self.G.add_node(self.nodenum, data=node)
            self.labeldict[self.nodenum] = "OR"
            self.add_edge(last_ep, self.nodenum)
            last_ep = self.nodenum
            self.nodenum = self.nodenum + 1
            self.condnum = self.condnum + 1        

        # attach to project
        node = Node(self.nodenum, content=parser.column_names, nodetype="project")
        self.G.add_node(self.nodenum, data=node)
        self.labeldict[self.nodenum] = "PROJECT " + (",").join(list(parser.column_names.keys()))
        self.add_edge(last_ep, self.nodenum)
        last_ep = self.nodenum
        self.nodenum = self.nodenum + 1
        self.condnum = self.condnum + 1

        # project optimization


        pass

# DATA LOCALIZATION
