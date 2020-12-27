import os
import sqlite3
import pandas as pd
import argparse
#import pandas_gbq
from google.oauth2 import service_account

def load_users(conn,credentials):
  #using pandas read the USERS2 table using connection object
  df = pd.read_sql_query("SELECT * FROM USERS2", conn)

  #rename the df column name because bq will not accept spaces
  users_df = df.rename(columns={'User - Years Elite':'User_Years_Elite',
                        'Business - Id':'Business_Id',
                        'User - Compliments Cool':'User_Compliments_Cool',
                        'User - Compliments Cute':'User_Compliments_Cute',
                        'User - Compliments Funny':'User_Compliments_Funny',
                        'User - Compliments Hot':'User_Compliments_Hot',
                        'User - Compliments List':'User_Compliments_List',
                        'User - Compliments More':'User_Compliments_More',
                        'User - Compliments Note':'User_Compliments_Note',
                        'User - Compliments Photos':'User_Compliments_Photos',
                        'User - Compliments Plain':'User_Compliments_Plain',
                        'User - Compliments Profile':'User_Compliments_Profile',
                        'User - Compliments Writer':'User_Compliments_Writer',
                        'User - Fans':'User_Fans','User - Name':'User_Name',
                        'Review - Id':'Review_Id','User - Id':'User_Id',
                        'User - Votes Cool':'User_Votes_Cool',
                        'User - Votes Funny':'User_Votes_Funny',
                        'User - Votes Useful':'User_Votes_Useful',
                        'User - Yelping Since':'User_Yelping_Since'})

  #Load the data to bq using the dataframe method to_gbq
  #Provide table name along with the dataset name, project where the dataset resides, path to the service account key as credentials
  users_df.to_gbq(destination_table='slalom.users',project_id='slalom-de',credentials=credentials,if_exists='append')

def load_business_attributes(conn,credentials):
  df = pd.read_sql_query("SELECT * FROM BUSINESS_ATTRIBUTES", conn)
  ba = df.rename(columns={'Business - Restaurant?':'Business_Restaurant',
                        'Business - Accepts Credit Cards':'Business_Accepts_Credit_Cards',
                        'Business - Accepts Insurance':'Business_Accepts_Insurance',
                        'Business - Ages Allowed':'Business_Ages_Allowed',
                        'Business - Alcohol':'Business_Alcohol',
                        'Business - Attire':'Business_Attire',
                        'Business - BYOB/Corkage':'Business_BYOB_Corkage',
                        'Business - BYOB':'Business_BYOB',
                        'Business - By Appointment Only':'Business_By_Appointment_Only',
                        'Business - Caters':'Business_Caters',
                        'Business - Coat Check':'Business_Coat_Check',
                        'Business - Corkage':'Business_Corkage',
                        'Business - Delivery':'Business_Delivery',
                        'Business - Dietary Restrictions':'Business_Dietary_Restrictions',
                        'Business - Dogs Allowed':'Business_Dogs_Allowed',
                        'Business - Drive-Thru':'Business_Drive_Thru',
                        'Business - Good For Dancing':'Business_Good_For_Dancing',
                        'Business - Good For Groups':'Business_Good_For_Groups',
                        'Business - Good For Kids':'Business_Good_For_Kids',
                        'Business - Good for Kids2':'Business_Good_for_Kids2',
                        'Business - Happy Hour':'Business_Happy_Hour',
                        'Business - Has TV':'Business_Has_TV',
                        'Business - Noise Level':'Business_Noise_Level',
                        'Business - Open 24 Hours':'Business_Open_24_Hours',
                        'Business - Order at Counter':'Business_Order_at_Counter',
                        'Business - Outdoor Seating':'Business_Outdoor_Seating',
                        'Business - Parking':'Business_Parking',
                        'Business - Payment Types':'Business_Payment_Types',
                        'Business - Price Range':'Business_Price_Range',
                        'Business - Smoking':'Business_Smoking',
                        'Business - Take-out':'Business_Take_out',
                        'Business - Takes Reservations':'Business_Takes_Reservations',
                        'Business - Waiter Service':'Business_Waiter_Service',
                        'Business - Wheelchair Accessible':'Business_Wheelchair_Accessible',
                        'Business - Wi-Fi':'Business_Wi_Fi',
                        'Business - Id':'Business_Id',
                        'Business - Categories':'Business_Categories',
                        'Business - Name':'Business_Name',
                        'Business - Neighborhoods':'Business_Neighborhoods',
                        'Business - Open?':'Business_Open'})
  #Load the data to bq using the dataframe method to_gbq
  #Provide table name along with the dataset name, project where the dataset resides, path to the service account key as credentials
  ba.to_gbq(destination_table='slalom.business_attributes',project_id='slalom-de',credentials=credentials,if_exists='append')

def create_connection(sqlite_file):
  conn = sqlite3.connect(sqlite_file)
  return conn

def close_connection(conn):
  conn.close()

def set_credentials(credentials_file):
  #Credentials required to load data to project using the service account
  credentials = service_account.Credentials.from_service_account_file(
    credentials_file,
  )
  return credentials

def run():
  parser = argparse.ArgumentParser()
  parser.add_argument('--sqlite_file',           
                      dest='sqlite_file',
                      required=True,
                      help='Provide Sqlite file path')
  parser.add_argument('--credentials_file',           
                      dest='credentials_file',
                      required=True,
                      help='Service Account Credentials file path')
  args = parser.parse_args()
  
  #Setting variables from parser
  sqlite_file = args.sqlite_file
  credentials_file = args.credentials_file

  #set credentials
  credentials = set_credentials(credentials_file)

  #create connection
  conn = create_connection(sqlite_file)

  #load_users_table_to_bq
  load_users(conn,credentials)

  #load_business_attributes_to_bq 
  load_business_attributes(conn,credentials)

  #close connection
  close_connection(conn)

if __name__ == '__main__':
  run()
