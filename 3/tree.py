from moz_sql_parser import parse, format

import networkx as nx
import matplotlib.pyplot as plt
import matplotlib
matplotlib.rcParams['figure.figsize'] = 15, 15

from utils import *
from networkx.drawing.nx_pydot import graphviz_layout

from node import *

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
        self.last_ep = 0
        self.join_mapping = {}

    def add_edge(self, u, v):
        self.G.nodes[u]['data'].parent = v
        if self.G.nodes[v]['data'].child1 == None:
            self.G.nodes[v]['data'].child1 = u
        else:
            self.G.nodes[v]['data'].child2 = u
        self.G.add_edge(u, v)

    def add_node(self, content, nodetype, label):
        node = Node(nodenum=self.nodenum, content=content, nodetype=nodetype)
        self.G.add_node(self.nodenum, data=node)
        self.last_ep = self.nodenum
        self.labeldict[self.nodenum] = label
        self.nodenum = self.nodenum + 1

    def parse_condition(self, condition, parser):
        type_ = get_type(condition, parser)
        if type_ != "or":
            # literal eq
            cond = self.operator_to_sign[list(condition.items())[0][0]]
            colname = list(condition.items())[0][1][0]
            literal = list(condition.items())[0][1][1]
            relation_name = parser.alias_of_relation[findTable(parser.relation_names, parser, colname)]
            if self.select_conditions.get(relation_name) == None:
                self.select_conditions[relation_name] = []
            self.select_conditions[relation_name].append((condition, self.nodenum))
            self.add_node(condition, "SELECT", colname + cond + str(literal))
        else:
            # put as 1 node
            self.or_conditions.append((condition, self.nodenum))

    def parse_or_condition(self, cond):
        print(format({"from":"test", "select":["*"], "where":cond[0]}))
        return "SELECT " + format({"from":"test", "select":["*"], "where":cond[0]})[25:]

    def add_node_x(self, parser, k, cond, nodetype):
        node = Node(self.nodenum, content=k, nodetype=nodetype)
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

    # QUERY TREE
    def build(self, parser):
        # build itself using the parser
        self.nodenum = 0
        
        # build nodes of tables
        for table in list(parser.relation_names.keys()):
            self.add_node(table, "RELATION", table)
            self.table_to_num[table] = self.nodenum - 1
            self.select_endpoints[table] = self.nodenum - 1

        # build condition tree
        for conditions in parser.conditions:
            condition = conditions
            type_ = get_type(condition, parser)
            if type_ == "join":
                # join condition, put in join queue
                self.join_conditions.append((condition, -1))
            else:
                if type_ != "and":
                        self.parse_condition(condition=condition, parser=parser)
                else:
                    # and condition
                    for condition in conditions['and']:
                        type_ = get_type(condition, parser)
                        if type_ == "join":
                            self.join_conditions.append((condition, -1))
                        else:
                            self.parse_condition(condition=condition, parser=parser)
                            self.condnum = self.condnum + 1
                            self.nodenum = self.nodenum + 1

        print("select conditions:" , self.select_conditions)
        print("join conditions:" , self.join_conditions)
        print("or conditions:" , self.or_conditions)

        # push select down
        for k,v in self.select_conditions.items():
            self.last_ep = self.nodenum - 1
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

        # check if all conditions in OR of single table
        tables = {}
        try:
            for cond in self.or_conditions[0][0]['or']:
                print(cond)
                condition = cond
                key = list(condition.keys())[0]
                colname = condition[key][0]
                literal = condition[key][1]
                if type(literal) == dict:
                    print(literal)
                    literal = literal["literal"]
                table_name = parser.alias_of_relation[findTable(parser.relation_names, parser.schema, colname)]
                tables[table_name] = True
        except:
            pass
        orhandled = False
        if len(list(tables.keys())) == 1:
            # only 1 table
            orhandled = True
            node = Node(self.nodenum, content=self.or_conditions[0][0], nodetype="or")
            name = list(tables.keys())[0]
            prevnode = self.select_endpoints[name]
            self.G.add_node(self.nodenum, data=node)
            self.labeldict[self.nodenum] = self.parse_or_condition(self.or_conditions[0])
            self.add_edge(prevnode, self.nodenum)
            self.last_ep = self.nodenum
            self.select_endpoints[name] = self.nodenum
            self.nodenum = self.nodenum + 1
            self.condnum = self.condnum + 1            

        # handle join conditions
        for k,v in self.join_conditions:
            cond = next(iter(k))
            if cond == "eq":
                self.last_ep = self.nodenum
                self.add_node_x(parser, k, cond, nodetype="join")

        # handle cross join conditions
        for k,v in self.join_conditions:
            cond = next(iter(k))
            if cond != "eq":
                # break into 2
                self.add_node_x(parser, {"cross": k[cond]}, "cross", nodetype="cross")
                self.last_ep = self.nodenum
                self.add_node_x(parser, k, cond, nodetype="select")
            pass
         
        # handle or conditions
        if not orhandled:
            # needs to be above all join conditions
            for k in self.or_conditions:
                node = Node(self.nodenum, content=k[0], nodetype="or")
                self.cond_to_num[self.condnum] = self.nodenum
                cond = "or"
                self.G.add_node(self.nodenum, data=node)
                self.labeldict[self.nodenum] = self.parse_or_condition(k)
                self.add_edge(self.last_ep, self.nodenum)
                self.last_ep = self.nodenum
                self.nodenum = self.nodenum + 1
                self.condnum = self.condnum + 1        

        # handle groupby
        

        # attach to project
        node = Node(self.nodenum, content=parser.column_names, nodetype="project")
        self.G.add_node(self.nodenum, data=node)
        self.labeldict[self.nodenum] = "PROJECT " + (",").join(list(parser.aggregate_names.keys())) + ", " + (", ").join(list(parser.column_names.keys()))
        self.add_edge(self.last_ep, self.nodenum)
        self.last_ep = self.nodenum
        self.nodenum = self.nodenum + 1
        self.condnum = self.condnum + 1

        # project optimization
        curnode = self.last_ep
        
        # for every table go up and check what all tables are needed.

        pass

    def fragment_vertically(self, parser, table, vert_nodes):
        self.joined_tables = {}
        if parser.alias_of_relation.get(table['TableName']) != None:
            relationID = table['idTable']
            fragmentation_conditions = list(filter(lambda dic: dic["RelationId"] == relationID, parser.schema["FRAGMENTATION"]))
            cols = parser.schema["COLUMNS"]
            mergecol = None
            for cond in fragmentation_conditions:
                mergecol = int(cond["FragmentationCondition"].split(",")[0])
                node = Node(self.nodenum, content=parser.alias_of_relation[table['TableName']] + "@site" + str(cond["SiteId"]), nodetype="relation")
                vert_nodes.append(node)
                self.G.add_node(self.nodenum, data=node)
                self.labeldict[self.nodenum] = parser.alias_of_relation[table['TableName']] + "@site" + str(cond["SiteId"])
                self.nodenum = self.nodenum + 1
            prevnode = vert_nodes[0]
            for nodelen in range(1, len(vert_nodes)):
                node = vert_nodes[nodelen]
                nodee = Node(self.nodenum, content=parser.alias_of_relation[table['TableName']], nodetype="join")
                self.G.add_node(self.nodenum, data=nodee)
                firs, sec = [], []
                if self.joined_tables.get(prevnode.nodenum) != None:
                    firs = self.joined_tables.get(prevnode.nodenum)
                else:
                    firs = self.G.nodes[prevnode.nodenum]['data'].content
                if self.joined_tables.get(node.nodenum) != None:
                    sec = self.joined_tables.get(node.nodenum)
                else:
                    sec = self.G.nodes[node.nodenum]['data'].content
                self.joined_tables[self.nodenum] = firs + "," + sec
                columns = list(filter(lambda dic: dic["ColumnID"] == mergecol, parser.schema["COLUMNS"]))[0]
                self.labeldict[self.nodenum] = "JOIN " + firs + "," + sec + " at " + parser.alias_of_relation[table['TableName']] +  "." + columns["ColumnName"]
                self.add_edge(prevnode.nodenum, self.nodenum)
                self.add_edge(node.nodenum, self.nodenum)
                self.nodenum = self.nodenum + 1
                self.condnum = self.condnum + 1
                prevnode = nodee
            nodenum = self.table_to_num[parser.alias_of_relation[table['TableName']]]
            parent = self.G.nodes[nodenum]['data'].parent
            self.G.nodes[prevnode.nodenum]['data'].parent = parent
            if self.G.nodes[parent]['data'].child1 == nodenum:
                # at child1
                self.G.nodes[parent]['data'].child1 = prevnode.nodenum
            else:
                self.G.nodes[parent]['data'].child2 = prevnode.nodenum
            self.G.remove_edge(nodenum, parent)
            self.G.add_edge(prevnode.nodenum, parent)
            # self.G.remove_node(nodenum)
        return vert_nodes

    def fragment_horizontally(self, parser, table, horiz_nodes):
        if parser.alias_of_relation.get(table['TableName']) != None:
            relationID = table['idTable']
            fragmentation_conditions = list(filter(lambda dic: dic["RelationId"] == relationID, parser.schema["FRAGMENTATION"]))
            columns = list(filter(lambda dic: dic["TableID"] == relationID, parser.schema["COLUMNS"]))
            cols = parser.schema["COLUMNS"]
            for cond in fragmentation_conditions:
                node = Node(self.nodenum, content=parser.alias_of_relation[table['TableName']], nodetype="relation")
                horiz_nodes.append(node)
                self.G.add_node(self.nodenum, data=node)
                self.labeldict[self.nodenum] = parser.alias_of_relation[table['TableName']] + "@site" + str(cond["SiteId"])
                self.nodenum = self.nodenum + 1
                self.condnum = self.condnum + 1
            prevnode = horiz_nodes[0]
            for nodelen in range(1, len(horiz_nodes)):
                node = horiz_nodes[nodelen]
                nodee = Node(self.nodenum, content=parser.alias_of_relation[table['TableName']], nodetype="join")
                self.G.add_node(self.nodenum, data=nodee)
                self.labeldict[self.nodenum] = "UNION"
                self.add_edge(prevnode.nodenum, self.nodenum)
                self.add_edge(node.nodenum, self.nodenum)
                self.nodenum = self.nodenum + 1
                self.condnum = self.condnum + 1
                prevnode = nodee
            nodenum = self.table_to_num[parser.alias_of_relation[table['TableName']]]
            parent = self.G.nodes[nodenum]['data'].parent
            self.G.nodes[prevnode.nodenum]['data'].parent = parent
            if self.G.nodes[parent]['data'].child1 == nodenum:
                # at child1
                self.G.nodes[parent]['data'].child1 = prevnode.nodenum
            else:
                self.G.nodes[parent]['data'].child2 = prevnode.nodenum
            self.G.remove_edge(nodenum, parent)
            self.G.add_edge(prevnode.nodenum, parent)
            # self.G.remove_node(nodenum)
        return horiz_nodes

    # DATA LOCALIZATION
    def localize(self, parser):
        # find tables VF
        tables_vertical = list(filter(lambda dic: dic["FragmentationType"] == "V", parser.schema["RELATIONS"]))
        # break all by join
        vert_nodes = []
        for table in tables_vertical:
            vert_nodes = self.fragment_vertically(parser, table, vert_nodes)

        # find tables HF
        tables_horizontal = filter(lambda dic: dic["FragmentationType"] == "H", parser.schema["RELATIONS"])
        # break all by union
        horiz_nodes = []
        for table in tables_horizontal:
            horiz_nodes = self.fragment_horizontally(parser, table, horiz_nodes)

        # find tables DHF
        tables_dhf = filter(lambda dic: dic["FragmentationType"] == "DH", parser.schema["RELATIONS"])
        # break all by union        
        horiz_nodes = []
        for table in tables_dhf:
            horiz_nodes = self.fragment_horizontally(parser, table, horiz_nodes)    
        pass

    # GRAPH VISUALIZATION
    def visualize(self, filename="filename.png"):
        self.revG = self.G
        pos = graphviz_layout(self.revG, prog="dot")
        flipped_pos = {node: (x,-y) for (node, (x,y)) in pos.items()}
        plt.figure(figsize=(30,12))
        nx.draw(self.revG,  labels=self.labeldict, pos=flipped_pos, with_labels=True,  node_shape="s",  node_color="none", bbox=dict(facecolor="skyblue", edgecolor='black', boxstyle='round,pad=0.5'))
        plt.tight_layout()
        plt.savefig(filename)