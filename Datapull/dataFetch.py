import pandas as pd
import json
import requests
import boto3
import sys
import datetime
import traceback
import MySQLdb


class dataFetch:
    def getAuditData(self):
        mysql_cn = MySQLdb.connect(host='mydbinstance.cdohayrpcsrh.us-east-1.rds.amazonaws.com', port=3306,user='bigdata', passwd='DummyD3l0itt3', db='DV_CMSMDH')
        df_mysql = pd.read_sql("select max(strt_tm) from job_trckng_log;", con=mysql_cn)
        mysql_cn.close()
        return df_mysql

    def getsrcData(self,source,area):
        mysql_cn = MySQLdb.connect(host='mydbinstance.cdohayrpcsrh.us-east-1.rds.amazonaws.com', port=3306,user='bigdata', passwd='DummyD3l0itt3', db='DV_CMSMDH')
        df_mysql = pd.read_sql(
            "select src_id, sub_ar_id, src_url_adr, da_method, api_nm, api_params, fltr_cond,  data_format, usr_id, AES_DECRYPT(pswd,'CMS DEV Open Cloud Open Secret') pswd, frqncy, location from src_dtl where src_id='" + source + "' and sub_ar_id='" + area + "';",
            con=mysql_cn)
        mysql_cn.close()
        return df_mysql
