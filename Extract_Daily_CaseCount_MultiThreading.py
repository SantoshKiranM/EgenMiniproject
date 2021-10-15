import sys
from dateutil.parser.isoparser import isoparse
import requests
import json
import sqlite3
import os
from datetime import datetime
#import datetime
import logging
#import dill
import multiprocessing
from multiprocessing import freeze_support
#from multiprocessing.pool import ThreadPool as Pool
import dateutil.parser


class CaseCountExtract:
#    def __init__(self) -> None:
    def __init__(self):        

        logging.captureWarnings(True)
        dtTime = datetime.now().strftime("%Y%m%d_%H%M%S")
        logging.basicConfig(
            filename=os.getcwd()+'/Extract_Daily_CaseCount_'+ dtTime +'.log',
            filemode='w',
            level=logging.DEBUG
            )
        
        #Create database in memory
        logging.info("Creating database...")
        print("Creating database...")
        self.con = sqlite3.connect(":memory:")
        self.cur = self.con.cursor()
        
        if __name__ == '__main__':
            freeze_support()
        self.responseList = []

        #Multiprocessing
        self.PROCESSES = multiprocessing.cpu_count()
        print("Number of processes: %s" %(self.PROCESSES))

#    def __call__(self, response):
#        freeze_support()
#        return self.table_load(response)

    def table_load(self, response):
        logging.info("Starting load for %s, for date %s..." % (response[9], datetime.today().strftime("%Y-%m-%d")))
        #print("Starting load for %s, for date %s..." % (response[9], datetime.today().strftime("%Y-%m-%d")))                

        self.cur.execute("INSERT INTO %s VALUES ('%s', '%s', %s, %s, %s, %s, '%s')" %(
            response[9].upper().replace(' ','_').replace('.','_').replace(',','_'),
            #datetime.fromisoformat(response[8]).strftime("%Y-%m-%d"),
            dateutil.parser.isoparse(response[8]).strftime("%Y-%m-%d"),            
            response[9],
            response[10],
            response[11],
            response[12],
            response[13],
            datetime.today().strftime("%Y-%m-%d")
            ))

        logging.info("Load completed for %s, for date %s..." % (response[9], datetime.today().strftime("%Y-%m-%d")))
        #print("Load completed for %s, for date %s..." % (response[9], datetime.today().strftime("%Y-%m-%d")))
        #return 0

    def create_request(self):
        #Create a request and get response from API
        logging.info("Creating request to API...")
        print("Creating request to API...")
        responseAPI = requests.get("https://health.data.ny.gov/api/views/xdss-u53e/rows.json?accessType=DOWNLOAD")
        if responseAPI.status_code != 200:
            logging.error("Error: API not reachable. Exiting...")
            sys.exit(1)

        logging.info("Response received. Creating JSON...")
        print("Response received. Creating JSON...")
        responseDict = responseAPI.json()
        self.responseList = responseDict['data']
    
    def create_tables(self):
        #Create tables if they do not exist
        tablesList = []
        for response1 in self.responseList:
            tablesList.append(response1[9])
        tablesListUnique = list(set(tablesList))

        logging.info("Unique List of Tables: %s" %(tablesListUnique))
        tablesListUnique = [element.upper().replace(' ','_').replace('.','_').replace(',','_') for element in tablesListUnique]
        logging.info("Unique List of Tables, Updated: %s" %(tablesListUnique))

        for table in tablesListUnique:
            self.cur.execute("CREATE TABLE IF NOT EXISTS %s (TEST_DATE DATE, COUNTY TEXT, NEW_POSITIVES INTEGER, CUMULATIVE_POSITIVES INTEGER, TOTAL_TESTS INTEGER, CUMULATIVE_TESTS INTEGER, LOAD_DATE DATE)" %(table))
            logging.info("Table %s created." %(table))

    #logging.info("Starting load for %s..." % (datetime.today().strftime("%Y-%m-%d")))
    #print("Starting load for %s..." % (datetime.today().strftime("%Y-%m-%d")))

#    sampleList = []
#    sampleList.append(responseList[0])
#    sampleList.append(responseList[1])

#    newresponselist = []
#    for response in responseList:
#        newresponselist.append([cur, response])

    def load_tables(self):
        #with multiprocessing.Pool(self.PROCESSES) as p:    
        #p = multiprocessing.Pool(self.PROCESSES)
        #p = Pool(self.PROCESSES)
        #result = p.map_async(self.table_load,self.responseList)
        #result = dill.loads(p.map_async(self.table_load,dill.dumps(self.responseList)))
#Multiprocessing
#        p = multiprocessing.Pool(self.PROCESSES)
#        result = p.map_async(self.table_load,self.responseList)        
#        print(result.get())
#        p.close()
#        p.join()
#Without Multiprocessing
        for response in self.responseList:
            self.table_load(response)

    #for response in responseList:
    #    cur.execute("INSERT INTO %s VALUES ('%s', '%s', %s, %s, %s, %s, '%s')" %(
    #        response[9].replace(' ','_').replace('.','_').replace(',','_'),
    #        datetime.fromisoformat(response[8]).strftime("%Y-%m-%d"),
    #        response[9],
    #        response[10],
    #        response[11],
    #        response[12],
    #        response[13],
    #        datetime.today().strftime("%Y-%m-%d")
    #        ))

    def validate_close(self):
        logging.info("Sample records from YATES county:")
        self.cur.execute("SELECT * FROM YATES limit 10")
        logging.info(self.cur.fetchall())

        #logging.info("Load completed successfully for %s" % (datetime.today().strftime("%Y-%m-%d")))
        #print("Load completed successfully for %s" % (datetime.today().strftime("%Y-%m-%d")))

        self.cur.close()
        self.con.close()