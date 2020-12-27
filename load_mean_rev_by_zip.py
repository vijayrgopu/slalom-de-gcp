import pandas as pd
import argparse
#import pandas_gbq
from google.cloud import storage
from google.oauth2 import service_account

'''
   Must export GOOGLE_APPLICATION_CREDENTIALS
'''

def set_credentials(credentials_file):
  #Credentials required to query data
  credentials = service_account.Credentials.from_service_account_file(
    credentials_file,
  )
  return credentials

def get_mean_reviews_by_zip(credentials,bucket_name):
  q_mean_reviews_by_zip = "WITH TOP_5 AS ("
  q_mean_reviews_by_zip +="  SELECT ZIP_CODE, NUM_OF_BIZ, RANK() OVER (ORDER BY NUM_OF_BIZ DESC) AS RNK" 
  q_mean_reviews_by_zip +="  FROM ( SELECT "
  q_mean_reviews_by_zip +="           SAFE.SUBSTRING(BUSINESS_ADDRESS,LENGTH(BUSINESS_ADDRESS)-5,LENGTH(BUSINESS_ADDRESS)) as ZIP_CODE, COUNT(*) AS NUM_OF_BIZ" 
  q_mean_reviews_by_zip +="         FROM `slalom-de.slalom.composition` WHERE UPPER(DAY_OF_THE_WEEK)='SATURDAY'"
  q_mean_reviews_by_zip +="         GROUP BY ZIP_CODE)"
  q_mean_reviews_by_zip +=")SELECT ZIP_CODE, ROUND(AVG(REVIEW_STARS),2) AS REVIEW_AVG,  FROM `slalom-de.slalom.reviews` rev, `slalom-de.slalom.composition` comp, TOP_5 "
  q_mean_reviews_by_zip +="WHERE rev.BUSINESSID = comp.BUSINESS_ID "
  q_mean_reviews_by_zip +="AND SAFE.SUBSTRING(comp.BUSINESS_ADDRESS,LENGTH(BUSINESS_ADDRESS)-5,LENGTH(BUSINESS_ADDRESS)) = TOP_5.ZIP_CODE "
  q_mean_reviews_by_zip +="AND TOP_5.RNK < 6 "
  q_mean_reviews_by_zip +="GROUP BY ZIP_CODE;"
  #read data from bq and return dataframe
  df = pd.read_gbq(q_mean_reviews_by_zip,project_id=bucket_name,credentials=credentials)

  return df

def load_file_gcs(bucket_name,file_name,data_frame):
  client = storage.Client()
  bucket = client.get_bucket(bucket_name)
  
  #create the file in the bucket
  bucket.blob(file_name).upload_from_string(data_frame.to_csv(sep="|",index=False), 'text/csv')

def run():
  parser = argparse.ArgumentParser()
  parser.add_argument('--output_file',           
                      dest='output_file',
                      required=True,
                      help='Provide output file path')
  parser.add_argument('--bucket_name',           
                      dest='bucket_name',
                      required=True,
                      help='bucket name for output file')
  parser.add_argument('--credentials_file',           
                      dest='credentials_file',
                      required=True,
                      help='Service Account Credentials file path')
  args = parser.parse_args()

  bucket_name = args.bucket_name
  output_file = args.output_file
  credentials_file = args.credentials_file

  #set credentials using the json file
  credentials = set_credentials(credentials_file)

  #execute the query to extract data from bq
  df_mean_reviews_by_zip = get_mean_reviews_by_zip(credentials,bucket_name)

  #create the file in gcs
  load_file_gcs(bucket_name,output_file,df_mean_reviews_by_zip)

if __name__ == '__main__':
  run()

