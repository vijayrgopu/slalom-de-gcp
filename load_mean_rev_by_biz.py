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

def get_mean_reviews_by_biz(credentials,bucket_name):
  q_mean_reviews_by_biz = "WITH BUSINESS_ATTR AS ( SELECT DISTINCT BUSINESS_ID, BUSINESS_NAME FROM `slalom-de.slalom.business_attributes`) "
  q_mean_reviews_by_biz += "SELECT ba.BUSINESS_ID, ba.BUSINESS_NAME, ROUND(AVG(rev.REVIEW_STARS),2) AS REVIEW_RATING" 
  q_mean_reviews_by_biz += " FROM BUSINESS_ATTR ba, `slalom-de.slalom.reviews` rev" 
  q_mean_reviews_by_biz += " WHERE ba.BUSINESS_ID = rev.BUSINESSID"
  q_mean_reviews_by_biz += " GROUP BY BUSINESS_ID, BUSINESS_NAME;"

  #read data from bq and return dataframe
  df = pd.read_gbq(q_mean_reviews_by_biz,project_id=bucket_name,credentials=credentials)

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
  df_mean_reviews_by_biz = get_mean_reviews_by_biz(credentials,bucket_name)

  #create the file in gcs
  load_file_gcs(bucket_name,output_file,df_mean_reviews_by_biz)

if __name__ == '__main__':
  run()
