# check if aggregate queries
from tabnanny import check

def inner_join_exists(token_tree) -> bool:
    return type(token_tree['from']) == list and type(token_tree['from'][-1]) == dict  and token_tree['from'][-1].get('inner join') != None

def is_aggregate_column(name) -> bool:
    if name[:3].upper() in ["MAX", "SUM", "AVG", "MIN"]:
        return True
    return False

def get_col(parser, col):
    if col in list(parser.column_names.keys()):
        return parser.column_names[col]
    if len(col.split(".")) > 1:
        # contains table
        return parser.relation_names[col.split(".")[0]] + "." + col.split(".")[1]
    return findTable(parser.schema, col) + "." + col.split(".")[0]

def get_fragmentation_type(parser, table):
    schema = parser.schema
    schema["RELATIONS"]
    tables = list(filter(lambda dic: dic["TableName"] == table, schema["RELATIONS"]))[0]
    return tables["FragmentationType"]

def get_fragmentation_site(parser, table):
    schema = parser.schema
    schema["RELATIONS"]
    tables = list(filter(lambda dic: dic["TableName"] == table, schema["RELATIONS"]))[0]
    tableid = tables["idTable"]
    frag = list(filter(lambda dic: dic["RelationId"] == tableid, schema["FRAGMENTATION"]))[0]
    return frag["SiteId"]

def get_table(parser, col):
    if len(col.split(".")) > 1:
        # contains table
        return col.split(".")[0]
    return parser.alias_of_relation[findTable(parser.schema, col)]

def check_if_a_col(parser, col):
    if col in list(parser.column_names.keys()):
        # aliased
        return True
    # unaliased
    colname = str(col).split(".")[-1]
    for column in parser.schema["COLUMNS"]:
        if column["ColumnName"] == colname:
            return True
    return False

# get type of condition
def get_type(cond, parser=None) -> str:
    if type(cond) == dict and cond.get("eq") != None:
        name1 = cond["eq"][0]
        name2 = cond["eq"][1]
        if check_if_a_col(parser, name2):
            # both are legit column names
            return "join"
    if type(cond) == dict and list(cond.keys())[0] not in ["and", "or","eq"]:
        name1 = cond[list(cond.keys())[0]][0]
        name2 = cond[list(cond.keys())[0]][1]
        if check_if_a_col(parser, name2):
            # both are legit column names
            return "join"
    if type(cond) != dict:
        return "literal"
    return list(cond.keys())[0]

# find which table column is in
def findTable(relation_names, schema, col):
    if len(col.split(".")) > 1:
        # if name of column contains it, then return it
        return  relation_names[col.split(".")[0]]
    columns = list(filter(lambda dic: dic["ColumnName"] == col, schema["COLUMNS"]))
    if len(columns) == 0:
        print("COLUMN {} not found !".format(col))
        exit()
    else:
        column = columns[0]
        table = list(filter(lambda dic: dic["idTable"] == column["TableID"], schema["RELATIONS"]))[0]
        return table["TableName"]

def get_indices_join(token_tree):
    indices = [i for i, x in enumerate(token_tree['from']) if type(token_tree['from'][i]) == dict and token_tree['from'][i].get('inner join') != None]
    return indices