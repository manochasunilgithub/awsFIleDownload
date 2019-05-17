# -*- coding: utf-8 -*-
"""
Created on Fri May 16 18:31:11 2019

@author: Sunil Manocha
"""
import turboDb
import sqlalchemyDb
from aws import aws
#import pandas

def main():
   awsCredentials = {'VaultUrl':'',
                     'role_id' : '',
                    'sts_role' : ''}
   awsInstance = aws(awsCredentials)
   awsInstance.printCredenetials()
   source_path= '<BucketName>/test.xz'
   # Read the compressed csv into data frame
   df = awsInstance.downloadData(source_path,"xz")
   print(df)

   tableName = ''

   # persist using sqlalchemydb, we are connecting using windows authentication
   sqlDb = sqlalchemyDb('','',True,'','')
   result = sqlDb.persist(df,tableName,'dbo')
   print(result)

   # persist using turbodb
   tDb = turboDb('','',True,'','')
   status,result = tDb.persist(df,tableName,'dbo')
   print(result)


if __name__ == "__main__":
    main()

