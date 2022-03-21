# check if aggregate queries
from tabnanny import check


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
def findTable(schema, col, tablename=None):
    found = False
    if tablename != None:
        found = True
        return tablename
    for column in schema["COLUMNS"]:
        if column["ColumnName"] == col:
            col = column["TableID"]
            found = True
            break
    if not found:
        print("COLUMN {} not found !".format(col))
        exit()
    for table in schema["RELATIONS"]:
        if table["idTable"] == col - 1:
            return table["TableName"]