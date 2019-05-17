# -*- coding: utf-8 -*-
"""
Created on Tue May  15 19:54:51 2019

@author: Sunil Manocha
"""

import sqlalchemy
import pandas as pd
import numpy as np
import time

class sqlalchemyDb:

    def __init__(self,server,dbName,isTrustedConnection,userName,password):
        super().__init__()
        self.__server = server
        self.__dbName = dbName
        self.__isTrustedConnection = isTrustedConnection
        self.__userName = userName
        self.__password = password
        self.__initialise()

    def __initialise(self):
        """Connect to a specified db."""
        self.__engine = sqlalchemy.create_engine(
            'mssql+pymssql://{0}:{1}@{2}/{3}'.format(
                '', '',
                self.__server, self.__dbName
                )
            )

    def persist(self, df, tableName,schemaName='dbo'):
        """Load data using pandas."""
        start = time.time()
        df.to_sql(tableName, con=self.__engine,schema=schemaName, index=False, if_exists='append') # Append the data to existing table
        stop = time.time() - start
        return print(f'finished committing {df.shape[0]} records in {stop} seconds')

