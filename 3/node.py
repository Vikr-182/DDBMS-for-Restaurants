from heapq import merge
from ntpath import join
from operator import truediv
from platform import node
import pprint
# from tkinter import Frame
from mysql.connector import cursor
import sqlparse
import mysql.connector

from os import curdir
from re import L
from select import select
from mysql.connector.constants import ServerFlag

import networkx as nx
import matplotlib.pyplot as plt
import matplotlib
matplotlib.rcParams['figure.figsize'] = 15, 15


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
        self.usefulcolumnsabove = []
        self.columns = []
        self.mapping = {}
        self.statement = ""