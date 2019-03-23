import pandas as pd
import json
import requests
import boto3
import sys
import datetime
import traceback
from pyspark.sql import *
from pyspark.sql.functions import col
import MySQLdb
from sqlalchemy import create_engine
from pyspark.sql import SQLContext



class dataInsert:
    def auditwrite(self,dateNow, sub_ar_id, job_id, dateEnd, datelast):
        df = pd.DataFrame({'job_trkng_id': ['100' + '_' + str(dateNow).replace(' ', '_')], 'sub_ar_id': [sub_ar_id],
                           'job_id': [job_id], 'steps': [''], 'status': ['Succesfully Completed'],
                           'strt_tm': [str(dateNow)], 'end_tm': [str(dateEnd)],
                           'msg_lst_upd': ['Completed Successfully'], 'err_msg': ['no'], 'lst_act_ts': [str(datelast)]})

        engine = create_engine(
            "mysql://bigdata:DummyD3l0itt3@mydbinstance.cdohayrpcsrh.us-east-1.rds.amazonaws.com/DV_CMSMDH")
        mysql_cn = engine.connect()
        df.to_sql(name='job_trckng_log', con=mysql_cn, if_exists='append', index=False)

    def writeFiles(bucketName, fileStr, filePath, filelocation):
        """ This Function is to write the file to bucket
    arg1: Bucket Object
    arg2: File String to be saved in S3
    arg3: Name of the file by which the file will be saved.
    """
        # bucketObject = s3Connection()
        s3 = boto3.resource('s3')
        objectBody = s3.Object(bucketName, filePath)
        print("Writing files to ", filePath)
        objectBody.put(Body=str(fileStr))