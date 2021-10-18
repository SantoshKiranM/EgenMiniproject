import sys
import requests
import json
import sqlite3
import os
from datetime import datetime
import logging
import multiprocessing
import time


con = sqlite3.connect(":memory:")

class CaseCountExtract:

    def __init__(self):

        logging.captureWarnings(True)
        dtTime = datetime.now().strftime("%Y%m%d_%H%M%S")
        logging.basicConfig(
            filename=os.getcwd()+'/Extract_Daily_CaseCount_'+ dtTime +'.log',
            filemode='w',
            level=logging.DEBUG
            )

    def load_table(self, response):
        global con
        print("Sample Func String")
        logging.info("Starting load for %s, for date %s..." % (response[9], datetime.today().strftime("%Y-%m-%d")))
        print("INSERT INTO %s VALUES ('%s', '%s', %s, %s, %s, %s, '%s')" %(
            response[9].upper().replace(' ','_').replace('.','_').replace(',','_'),
            datetime.fromisoformat(response[8]).strftime("%Y-%m-%d"),
            response[9],
            response[10],
            response[11],
            response[12],
            response[13],
            datetime.today().strftime("%Y-%m-%d")
            ))
        cur = con.cursor()
        cur.execute("INSERT INTO %s VALUES ('%s', '%s', %s, %s, %s, %s, '%s')" %(
            response[9].upper().replace(' ','_').replace('.','_').replace(',','_'),
            datetime.fromisoformat(response[8]).strftime("%Y-%m-%d"),
            response[9],
            response[10],
            response[11],
            response[12],
            response[13],
            datetime.today().strftime("%Y-%m-%d")
            ))
        logging.info("Load completed for %s, for date %s..." % (response[9], datetime.today().strftime("%Y-%m-%d")))
        cur.execute("SELECT * FROM ALBANY limit 10")
        logging.info(cur.fetchall())

        con.commit()
        cur.close()



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
        responseList = responseDict['data']
        return responseList

    def create_tables(self, responseList):
        #Create tables if they do not exist
        global con
        cur = con.cursor()
        tablesList = []
        for response1 in responseList:
            tablesList.append(response1[9])
        tablesListUnique = list(set(tablesList))

        logging.info("Unique List of Tables: %s" %(tablesListUnique))
        tablesListUnique = [element.upper().replace(' ','_').replace('.','_').replace(',','_') for element in tablesListUnique]
        logging.info("Unique List of Tables, Updated: %s" %(tablesListUnique))

        for table in tablesListUnique:
            cur.execute("CREATE TABLE IF NOT EXISTS %s (TEST_DATE DATE, COUNTY TEXT, NEW_POSITIVES INTEGER, CUMULATIVE_POSITIVES INTEGER, TOTAL_TESTS INTEGER, CUMULATIVE_TESTS INTEGER, LOAD_DATE DATE)" %(table))
            logging.info("Table %s created." %(table))
        cur.close()


    def validate_close(self):
        global con
        con.close()
