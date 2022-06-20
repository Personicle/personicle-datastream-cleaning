import pymysql
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import logging

class DBConnectionETL:
    """
        For now using sync DB calls. Based on requirements we can make a equivalent async DB class to handle loads.
        Using disposable pattern to make sure that all DB connections are closed after each call cleanly, as this project is intented
        to run in parallel environment. So closing DB connection in all cases is very important including during any
        exceptions.

        Hardcoding the connection params for now. We can drive them by using config file for more generalised way.
    """
    def __init__(self, host = "localhost", user = "root", passwd = "root", database = "etl"):
        try:
            pymysql.converters.encoders[np.float32] = pymysql.converters.escape_float
            pymysql.converters.conversions = pymysql.converters.encoders.copy()
            pymysql.converters.conversions.update(pymysql.converters.decoders)
            self.host = host
            self.user = user
            self.passwd = passwd
            self.database = database
            self.connection = pymysql.connect(host=self.host, user=self.user, passwd=self.passwd, database=self.database)
            self.cursor = self.connection.cursor()
            self.engine = create_engine("mysql+pymysql://root:root@localhost/etl?host=localhost?port=3306")
        except:
            logging.exception("Error occurred during db Connection")
            raise ValueError("Incorrect connection info passed")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        '''

            Cleanly close the DB connection
        '''
        self.connection.close()
        self.engine.dispose()

    def select(self, query):
        try:
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            return rows
        except:
            logging.exception("Error occurred during db Select")
            raise

    def updateCommit(self, query):
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except:
            logging.exception("Error occurred during db Update")
            raise

    def insertCommit(self, query):
        try:
            self.cursor.execute(query)
            self.connection.commit()
        except:
            logging.exception("Error occurred during db Insert")
            raise

    def dfInsert(self, df, tblName):
        '''
            Pandas specific insert function to push whole Data frame into DB.
        '''
        try:
            df.to_sql(name=tblName, con=self.engine, if_exists='append', index=False)
        except:
            logging.exception("Error occurred during pandas db Insert")
            raise

    def dfSelect(self, query):
        '''
            Pandas specific select function to get data from DB into a data frame.
        '''
        try:
            df = pd.read_sql(query, con=self.engine)
            return df
        except:
            logging.exception("Error occurred during pandas db select")
            raise


