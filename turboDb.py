# -*- coding: utf-8 -*-
"""
Created on Tue May  15 18:54:51 2019

@author: Sunil Manocha
"""

import pandas as pd
import turbodbc
import numpy.ma as ma
import time


class turboDb:

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
        self.__connection = turbodbc.connect(
                                driver="ODBC Driver 13 for SQL Server",# Change this for driver
                                server=self.__server,
                                database=self.__dbName,
                                trusted_connection='yes'
                              )

    def persist(self, df, tableName,schemaName='dbo'):

        """Load data using pandas."""
        start = time.time()
        # preparing columns
        columns = '('
        columns += ', '.join(df.columns)
        columns += ')'

        # preparing value place holders
        val_place_holder = ['?' for col in df.columns]
        sql_val = '('
        sql_val += ', '.join(val_place_holder)
        sql_val += ')'

        # writing sql query for turbodbc
        sql = f"""
        INSERT INTO {self.__dbName}.{schemaName}.{tableName} {columns}
        VALUES {sql_val}
        """

        # writing array of values for turbodbc

        values_df = [ma.MaskedArray(df[col].values, mask=pd.isnull(df[col].values)) for col in df.columns]

        # inserts data, for real
        with self.__connection.cursor() as cursor:
            try:
                cursor.executemanycolumns(sql, values_df)
                self.__connection.commit()
            except Exception as e:
                self.__connection.rollback()
                print('something went wrong')
                return False,e

        stop = time.time() - start
        return True,f'finished committing {df.shape[0]} records in {stop} seconds'


    def executeQuery(self,sql):
        start = time.time()
        try:
            cursor = self.__connection.cursor()
            self.__connection.commit()
            cursor.execute(sql)
        except Exception as e:
                self.__connection.rollback()
                print('something went wrong')
                return(e)
        stop = time.time() - start
        return f'finished executing {sql}  in {stop} seconds'

