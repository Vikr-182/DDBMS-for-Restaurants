from time import sleep
from flask import Response, Flask, request, jsonify
from moz_sql_parser import parse, format 

import requests 
import json
import mysql.connector
from mysql.connector.constants import ServerFlag 

import logging


app = Flask(__name__)



class DBServer():
    def __init__(self,  curip, iplist, dbname, username, password):
        self.user = username
        self.curip = curip
        self.iplist = iplist
        self.dbname = dbname
        self.username = username 
        self.password = password
        print (self.dbname)
        print (self.dbname)
        self.state = 'none'
        self.db = mysql.connector.connect(host=self.curip,user=self.username, password=self.password,database=self.dbname)
        self.cursor = self.db.cursor()
        self.app = Flask('app')


        @self.app.route('/')
        def __index():
            return self.index()

        @self.app.route('/get-item', methods=['POST'])
        def __get_data():
            return self.get_data(json.loads(request.data))

        @self.app.route('/2pc', methods=['POST'])
        def exec_pc():
            print ('hello')
            print(request.data)
            return  self.two_pc(eval(request.data))
        
        @self.app.route('/prepare', methods=['POST'])
        def return_vote():
            print ('hello')
            return  self.decide(eval(request.data))
        
        @self.app.route('/phase2', methods=['POST'])
        def final():
            print ('hello')
            return  self.finexec(eval(request.data))



    def run(self, host, port):
        self.app.run(host=host, port=port)



    def two_pc(self,bodd):

        #starting two phase commit, sending prepare message to all sites 

        responseList = []
        print (bodd)
        print ('='*30)
        for ip in iplist:
            try:

                dicToSend = bodd
                res =  requests.post('http://'+ip+':8585/prepare',json=dicToSend)
                if res.ok:
                    responseList.append(res.json()['vote'])
                else :
                    responseList.append('abort')

            except:
                responseList.append('abort')

            
        print(responseList)
        if responseList == ['commit'] * len(iplist) :
            logfile.write('\n recieved commit from all, committing')

            # send global commit
            for ip in iplist:
                try:

                    dicToSend = {'query':bodd['query'], 'verdict':'global-commit'}
                    res =  requests.post('http://'+ip+':8585/phase2',json=dicToSend)
                    if res.ok:
                        # dicToSend = {'query':bodd['query'], 'verdict':'global-commit'}
                        return 'ok'
                    else :
                        return 'ok'
                
                except :
                    return 'ok'
             
        else :
            logfile.write('\n did not recieve commit from all, sending global abort')
            #send global abort
            for ip in iplist:
                try:
                    dicToSend = {'query':bodd['query'], 'verdict':'global-abort'}
                    res =  requests.post('http://'+ip+':8585/phase2',json=dicToSend)
                    if res.ok:
                        logfile.write('\n aborted successfully')
                        return 'ok'
                    return 'fail'

                except :
                    return 'ok'  



        """
        r=random.uniform(0,1)
        decision="vote-abort" if r>0.9 else "vote-commit"
        if decision=="vote-commit":
            log.write('commit')
            t1 = threading.Thread(target=self.execute_query, args=())
            t1.start()
        self.coordinator.record_vote(self.ip,decision)
        self.state="ready"
        """

    def decide(self,req_qry):
        print ('in phase 1 ')
        logfile.write('\n recieved prepare message')
        req_qry = req_qry['query']
        if self.state != 'ready':
            try:
                print ("trying commit")
                print(req_qry)
                upd_list = req_qry.split()
                tablename = upd_list[1]
                QUERY = 'DROP TABLE IF EXISTS UPD_{} ;'.format(tablename)
                CRT_QUERY = 'CREATE TABLE UPD_{} AS SELECT * FROM {}; '.format(tablename,tablename,tablename)
                
                print (QUERY)

                self.cursor.execute(QUERY)
                self.cursor.execute(CRT_QUERY) 
                

                upd_list[1] = 'UPD_' + tablename
                UPD_QUERY = ' '.join(upd_list)  
                print (UPD_QUERY)

                self.cursor.execute(UPD_QUERY)
                # self.cursor.close() 

                self.db.commit()

                print ('sending commit')
                logfile.write('\n sending commit ')
                return jsonify({'vote':'commit'})
                
            except Exception as e :
                print (e)
                logfile.write('\n sending abort message')
                return jsonify({'vote':'abort'}) 
        else :
            logfile.write('\n sending abort message')
            return jsonify({'vote':'abort'})


    def finexec(self,req_qry):
        print ("in phase 2")
        if req_qry['verdict'] == 'global-commit':
            logfile.write('\n doing the final commit')
            req_qry = req_qry['query']
            try:
                print(req_qry)
                upd_list = req_qry.split()
                tablename = upd_list[1]
                DROP_QUERY = 'DROP TABLE {};'.format(tablename)

                print (DROP_QUERY)
                self.cursor.execute(DROP_QUERY)
                # self.cursor.close()

                QUERY = 'RENAME table UPD_{} To {}; '.format(tablename,tablename)
                print (QUERY)
                self.cursor.execute(QUERY)
                # self.cursor.close()

                self.db.commit()

                logfile.write('\n committed ')
                return 'okay'
                
            except :
                logfile.write('\n sending abort message')
                return jsonify({'vote':'abort'}) 
        else :
            try:
                print(req_qry)
                upd_list = req_qry.split()
                tablename = upd_list[1]
                DROP_QUERY = 'DROP TABLE UPD_{};'.format(tablename)

                print (DROP_QUERY)
                self.cursor.execute(DROP_QUERY)
                # self.cursor.close()
                self.db.commit()
                logfile.write('\n RECIEVED GLOBAL ABORT, ABORTED ')

                return 'okay'
            except:
                return 'failed'



import logging.config


if __name__ == '__main__':


    iplist = ["10.3.5.211" , "10.3.5.208"] #, "10.3.5.204", "10.3.5.205"]
    curipind = 0
    dbname = "preql"
    username = "user"
    password = "iiit123"


    logfile = open(iplist[curipind]+'.log','a')
    logfile.write('\nThe debug message')


    print ('why')


    server = DBServer(curip=iplist[curipind], iplist=iplist, dbname=dbname, username=username, password=password)
    server.run(host='0.0.0.0', port=8585)