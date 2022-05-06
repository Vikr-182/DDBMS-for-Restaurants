from moz_sql_parser import parse, format

import os
import json

import mysql.connector
from mysql.connector.constants import ServerFlag

from flask import Flask, Response, request, send_file

class DBServer():
    def __init__(self,  curip, iplist, dbname, username, password, host) -> None:
        self.iplist = iplist
        self.curip = curip
        self.dbname = dbname
        self.username = username
        self.password = password
        self.cnx = mysql.connector.connect(user=username, password=password,host=host,database=dbname)
        self.cursor = self.cnx.cursor()
        self.app = Flask('app')

        @self.app.route('/')
        def __index():
            return self.index()

        @self.app.route('/get-item', methods=['POST'])
        def __get_item():
            return self.get_item(json.loads(request.data))

        @self.app.route('/get-data', methods=['POST'])
        def __get_data():
            return self.get_data((request.form))

    def get_data(self, dic):
        table = dic['table']
        os.system("mysqldump -u user -p{} preql {} > ./{}.sql".format(self.password, table, table + "_TEMP")) 
        return send_file("./{}.sql".format(table + "_TEMP"))
    
    def get_item(self, dic):
        table = dic['table']

        # duplicate this table
        query = "CREATE TABLE {} FROM {}".format("temp_" + table, table)

        conditions = dic['conditions']
        for cond in conditions:
            # apply the condition and filter this table.
            pass
        
        query = "SELECT * FROM temp_{}".format(table)
        self.cursor.execute(query)
        print(self.cursor)
        return self.cursor

    def run(self, host, port):
        self.app.run(host=host, port=port)

if __name__ == "__main__":
    iplist = ["10.3.5.211", "10.3.5.208", "10.3.5.204", "10.3.5.205"]
    hosts = ["CP5", "CP6", "CP7", "CP8"]
    curipind = 0
    dbname = "preql"
    username = "user"
    password = "iiit123"
    server = DBServer(curip=iplist[curipind], iplist=iplist, dbname=dbname, username=username, password=password, host=hosts[curipind])
    server.run(host='0.0.0.0', port=5000)