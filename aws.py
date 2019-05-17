# -*- coding: utf-8 -*-
"""
Created on Thu May  15, 19:58:25 2019

@author: Sunil Manocha
"""

import hvac
from s3fs.core import S3FileSystem
import pandas as pd

## This class deals with AWS buckets and help in downloading csv files into data frame

class aws:
    # credentials are the AWS credential dictionary having following key values vault_url, vault_token, role_id, sts_role
    # vault_token if already generated can be used otherwise it will be created on run time, no need to set that.

	# User can put default values
    __vault_url = ''
    __role_id = ''
    __sts_role = ''
    __vault_token = None
    __aws_creds = None
    __expirtationTime = None


    def __get_aws_credentials(self):
        if self.__vault_token is None:
            vault_client = hvac.Client(url=self.__vault_url)
        else:
            vault_client = hvac.Client(url=self.__vault_url, token=self.__vault_token)

        vault_client.auth_approle(self.__role_id)

        assert vault_client.is_authenticated()

        data = vault_client.write('aws/sts/{}'.format(self.__sts_role))['data']
        self.__aws_creds = data

    def __init__(self,credentials=None):
        if credentials is not None: # override the values
            if type(credentials) is not dict:
                raise "Please pass dictionary as an argument with following keys to be overriden values vault_url, vault_token, role_id, sts_role";

            # override value uri
            if 'vault_url' in credentials.keys():
                self.__vault_url = credentials['vault_url']

            # override value token
            if 'vault_token' in credentials.keys():
                self.__vault_token = credentials['vault_token']

           # override role id
            if 'role_id' in credentials.keys():
                self.__role_id = credentials['role_id']

            # override sts role

            if 'sts_role' in credentials.keys():
                self.__sts_role = credentials['sts_role']

        # get user auhtenticated and store the session
        self.__get_aws_credentials()
        self.__s3FileSystem = S3FileSystem(anon=False,key=self.__aws_creds['access_key'],secret=self.__aws_creds['secret_key'],token=self.__aws_creds['security_token'])



    def printCredenetials(self):
        newline = '\n'
        print(f"key={self.__aws_creds['access_key']}{newline}aws_secret_access_key={self.__aws_creds['secret_key']}{newline}aws_session_token={self.__aws_creds['security_token']}")



    def downloadFile(self,bucketName,sourcePath,downloadPath):
        pass


    def downloadData(self,path,compressionType,bucketName=None):
        try:

            if(bucketName==None):
                df = pd.read_csv(self.__s3FileSystem.open(path,
                                 mode='rb'), sep=",", compression=compressionType)
            else:
                dirPath = '{}/{}'.format(bucketName, path)
                df = pd.read_csv(self.__s3FileSystem.open(dirPath,
                                 mode='rb'), sep=",", compression=compressionType)
        except Exception as e:
            #print(e)
            return(None)
        ## returns the dataframe
        return df

    ## returns the subdirectory/files under the path
    def list(self,path,bucketName=None):
        if(bucketName==None):
            listofDir = self.__s3FileSystem.ls(path)
            ## returns the list of child folders/files
            return listofDir
        else:
            dirPath = '{}/{}'.format(bucketName, path)
            listofDir = self.__s3FileSystem.ls(dirPath)
            ## returns the list of child folders/files
            return listofDir