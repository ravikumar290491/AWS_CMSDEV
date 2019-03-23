from Datapull import dataFetch as fetch
from DataPush import dataInsert as di
from validation import dataValidation as dv


#if __name__ == "__main__":
src_id = sys.argv[1]
print(src_id)
src_ar_id = sys.argv[2]
print(src_ar_id)
dateNow = datetime.datetime.now()
sql_df = fetch.dataFetch.getsrcData(src_id , src_ar_id)
print(sql_df)
src_url_adr , usr_id , pswd , location = getConfigData(sql_df)
bucket_name , key_name = split_s3_bucket_key(location)
jsonData = getJson(src_url_adr , usr_id , pswd)
fileName = src_ar_id + "_" + str(dateNow).replace(" " , "_").replace("-" , "_").replace(":" , "_") + ".json"
writeFiles(bucket_name , jsonData , key_name + "/" + fileName , location)
dateEnd = datetime.datetime.now()
job_id = 100
print(bucket_name)
print(key_name)
print(fileName)
full_dataframe = jsontocsv("s3a://" + bucket_name + "/" + key_name + "/" + fileName)
datelast = datetime.datetime.now()
auditwrite(dateNow , src_ar_id , job_id , dateEnd , datelast)
