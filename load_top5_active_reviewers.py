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

def get_top10_active_reviewers(credentials,bucket_name):
  q_top_10_reviewers = "WITH USER_NAMES AS ("
  q_top_10_reviewers += "SELECT" 
  q_top_10_reviewers += "  DISTINCT USER_ID, USER_NAME " 
  q_top_10_reviewers += "FROM `slalom-de.slalom.users`) "
  q_top_10_reviewers += "SELECT * FROM ("
  q_top_10_reviewers += " SELECT USER_NAME, TOT_REVIEWS, RANK() OVER (ORDER BY TOT_REVIEWS DESC) AS RANK_ FROM ("
  q_top_10_reviewers += "   SELECT USER_NAME, COUNT(*) AS TOT_REVIEWS FROM `slalom-de.slalom.reviews` rev, USER_NAMES"
  q_top_10_reviewers += "   WHERE rev.User_Id = USER_NAMES.User_Id" 
  q_top_10_reviewers += "   GROUP BY USER_NAME)) AS TOP_10 WHERE RANK_ < 11"
  #read data from bq and return dataframe
  df = pd.read_gbq(q_top_10_reviewers,project_id=bucket_name,credentials=credentials)

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
  df_top10_active_reviewers = get_top10_active_reviewers(credentials,bucket_name)

  #create the file in gcs
  load_file_gcs(bucket_name,output_file,df_top10_active_reviewers)

if __name__ == '__main__':
  print(__name__)  
  run()


