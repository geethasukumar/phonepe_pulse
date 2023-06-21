import json
import pandas as pd
import mysql.connector
import os
import re
from sqlalchemy import create_engine



class phonepe_data:
    def __init__(self,p_data_dir,p_data_type):
        self.data_dir = p_data_dir
        self.data_type = p_data_type

        self.tbl_map_user = 'map_user'
        self.tbl_agg_trans = 'agg_trans'
        self.tbl_agg_user = 'agg_user'
        self.tbl_top_trans = 'top_trans'
        self.tbl_top_user = 'top_user'
        self.tbl_map_trans = 'map_trans'
        

        
    # MySQL DB connect
    def mysql_db_connect(self):
        try:
          
            hostname= "localhost"
            database= "phonepe_data"
            username= "root"
            password= ""

            self.engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}".format(host=hostname, db=database, user=username, pw=password))
        except:
            print('Error mysql_db_connect - MYSQL DB connection failed!!')
            
        
    # Read the folders from the state folder
    def read_file_data(self):
        
        lv_data = []
      
        # iterate over files in
        # that directory
       
        yr_data = []
        prev_state = ''
       
        for root, dirs, files in os.walk(self.data_dir):
            quarter_data = []
            
            file_count = 0;
            for filename in files:
                
                #Open each files in the folder
                file_count = file_count + 1;
                q_data = json.load(open(root+'\\'+filename, "r"))
                
                matches = re.search(r"^(.+?)state\\(.+?)\\(([0-9]+))$", root)
                state = matches.groups()[1]
                
                # set the prev_state, if the previous state data is appended already or if this is the first state to retrieve the data
                if (prev_state == ''):
                    prev_state = state
                
                year = matches.groups()[2]
                q = filename.split('.')[0]
                q_data_details = []
               
                # if there is any error, iterate to next json file or quarter file
                try:
                    if self.data_type == 'agg_trans':

                        # get the transactiondata from the file
                        for items in enumerate(q_data['data']['transactionData']):
                            detailed_data = {items[1]['name']: {'count': items[1]['paymentInstruments'][0]['count'],
                                                                'amount':items[1]['paymentInstruments'][0]['amount']
                                                               }
                                            }
                            q_data_details.append(detailed_data)

                    elif self.data_type == 'agg_user':

                        # get the user data from the file
                        for items in enumerate(q_data['data']['usersByDevice']):
                            detailed_data = {items[1]['brand']: {'count': items[1].get('count',0),
                                                                 'percentage':items[1].get('percentage',0)
                                                                }
                                            }
                            q_data_details.append(detailed_data)
                    
                    elif self.data_type == 'top_trans':

                        # get the user data from the file
                        for items in enumerate(q_data['data']['districts']):
                            
                            detailed_data = {items[1]['entityName']: {'count': items[1]['metric']['count'],
                                                                'amount':items[1]['metric']['amount']
                                                               }
                                            }
                            q_data_details.append(detailed_data)
                            
                    elif self.data_type == 'top_user':

                        # get the user data from the file
                        for items in enumerate(q_data['data']['districts']):
                           
                            detailed_data = {items[1]['name']: {'registeredUsers': items[1]['registeredUsers']}}
                            q_data_details.append(detailed_data)
                            
                            
                              
                    elif self.data_type == 'map_trans':

                        # get the user data from the file
                        for items in enumerate(q_data['data']['hoverDataList']):
                            lv_district_name = items[1]['name'].split(' district')[0]
                            detailed_data = {lv_district_name: {'count': items[1]['metric'][0]['count'],
                                                                'amount':items[1]['metric'][0]['amount']
                                                               }
                                            }
                            q_data_details.append(detailed_data)
                            
                    elif self.data_type == 'map_user':

                        # get the user data from the file
                        
                        detailed_data = json.dumps(q_data['data']['hoverData'])
                    
                        detailed_data = json.loads(detailed_data.replace(' district', ''))
                        q_data_details.append(detailed_data)
                except:
                    next
                    

                # append to the quarter  
                if len(q_data_details): 
                    quarter_data.append({q: q_data_details})
              
                # when all the files data are collected, append the data to the year
                if (file_count == len(files)):
                    yr_data.append({year: quarter_data})
                    quarter_data = []
                    
                # when the current state data retreival for all years is complete, reset the state name and append the data with the state name
                if prev_state != state :
                    lv_data.append({prev_state: yr_data})
                    prev_state=''
                    yr_data = []
                 
        return lv_data
    
    
    # Store the DF to mysql DB
    def store_phone_data_to_db(self, pp_df):
        try:
            self.mysql_db_connect()
            
        except Exception as ex:   
             print(ex)
     
    
        #convert the DF to sql and inserts to mysql DB
        try:
            if self.data_type=='map_user':
                pp_df.to_sql(self.tbl_map_user, self.engine, if_exists='replace', index=False)
                print('Map User Data stored to mysql DB')
            elif self.data_type == 'agg_trans':
                pp_df.to_sql(self.tbl_agg_trans, self.engine, if_exists='replace', index=False)
                print('Aggregated Transaction Data stored to mysql DB')
            elif self.data_type == 'agg_user':
                pp_df.to_sql(self.tbl_agg_user, self.engine, if_exists='replace', index=False)
                print('Aggregated User Data stored to mysql DB')
            elif self.data_type == 'top_trans':
                pp_df.to_sql(self.tbl_top_trans, self.engine, if_exists='replace', index=False)
                print('Top Transaction Data stored to mysql DB')
            elif self.data_type == 'top_user':
                pp_df.to_sql(self.tbl_top_user, self.engine, if_exists='replace', index=False)
                print('Top User Data stored to mysql DB')
            elif self.data_type == 'map_trans':
                pp_df.to_sql(self.tbl_map_trans, self.engine, if_exists='replace', index=False)
                print('Map Transaction Data stored to mysql DB')
            
            
        except:
            print('Error storing the date to mysql for Data ', data_type)
        
       
    
    # convert the dict constructed to a DF
    def convert_dict_to_df(self,phone_data):
        df_data = []
        for st_data in phone_data:
            for name,data in st_data.items():
                for yr_data in data:
                    for yr, yr_details in yr_data.items():
                        for qtr_data in yr_details:
                            for qtr, qtr_details in qtr_data.items():
                               
                                for dt in qtr_details: 
                                    each_data = dict()
                                    for i in dt.keys():
                                        each_data['state'] = name
                                        each_data['year'] = yr
                                        each_data['quarter'] = qtr
                                        each_data['metric']= i
                                        for j in dt[i].keys():
                                            each_data[j]= dt[i][j]

                                        
                                        df_data.append(each_data)
                                        each_data={}
                                       
        
       
        return pd.DataFrame.from_dict(df_data)
    
    
            
# Create object for class phonepe_data for Aggregated Transaction jsons

pp_agg_trans_data_obj = phonepe_data('C:/Users/rgkri/OneDrive/Geetha/GUVI/project/phonepe/pulse/data/aggregated/transaction/country/india/state', 'agg_trans')
pp_agg_trans_data = pp_agg_trans_data_obj.read_file_data() #read the data from file and format the data
pp_agg_trans_df = pp_agg_trans_data_obj.convert_dict_to_df(pp_agg_trans_data) # convert the formatted data to DF
pp_agg_trans_data_obj.store_phone_data_to_db(pp_agg_trans_df) # Store the DF to mysql

# Create object for class phonepe_data for Aggregated user jsons
pp_agg_user_data_obj = phonepe_data('C:/Users/rgkri/OneDrive/Geetha/GUVI/project/phonepe/pulse/data/aggregated/user/country/india/state', 'agg_user')
pp_agg_user_data = pp_agg_user_data_obj.read_file_data() #read the data from file and format the data
pp_agg_user_df = pp_agg_user_data_obj.convert_dict_to_df(pp_agg_user_data)  # convert the formatted data to DF
pp_agg_user_data_obj.store_phone_data_to_db(pp_agg_user_df) # Store the DF to mysql

# Create object for class phonepe_data for Top transaction jsons
pp_top_trans_data_obj = phonepe_data('C:/Users/rgkri/OneDrive/Geetha/GUVI/project/phonepe/pulse/data/top/transaction/country/india/state', 'top_trans')
pp_top_trans_data = pp_top_trans_data_obj.read_file_data() #read the data from file and format the data
pp_top_trans_df = pp_top_trans_data_obj.convert_dict_to_df(pp_top_trans_data)  # convert the formatted data to DF
pp_top_trans_data_obj.store_phone_data_to_db(pp_top_trans_df) # Store the DF to mysql


# Create object for class phonepe_data for Top user jsons
pp_top_user_data_obj = phonepe_data('C:/Users/rgkri/OneDrive/Geetha/GUVI/project/phonepe/pulse/data/top/user/country/india/state', 'top_user')
pp_top_user_data = pp_top_user_data_obj.read_file_data() #read the data from file and format the data
pp_top_user_df = pp_top_user_data_obj.convert_dict_to_df(pp_top_user_data) # convert the formatted data to DF
pp_top_user_data_obj.store_phone_data_to_db(pp_top_user_df) # Store the DF to mysql

# Create object for class phonepe_data for map transaction jsons
pp_map_trans_data_obj = phonepe_data('C:/Users/rgkri/OneDrive/Geetha/GUVI/project/phonepe/pulse/data/map/transaction/hover/country/india/state', 'map_trans')
pp_map_trans_data = pp_map_trans_data_obj.read_file_data() #read the data from file and format the data
pp_map_trans_df = pp_map_trans_data_obj.convert_dict_to_df(pp_map_trans_data) # convert the formatted data to DF
pp_map_trans_data_obj.store_phone_data_to_db(pp_map_trans_df) # Store the DF to mysql

# Create object for class phonepe_data for map user jsons
pp_map_user_data_obj = phonepe_data('C:/Users/rgkri/OneDrive/Geetha/GUVI/project/phonepe/pulse/data/map/user/hover/country/india/state', 'map_user')
pp_map_user_data = pp_map_user_data_obj.read_file_data() # read the data from file and format the data
pp_map_user_df = pp_map_user_data_obj.convert_dict_to_df(pp_map_user_data) # convert the formatted data to DF
pp_map_user_data_obj.store_phone_data_to_db(pp_map_user_df) # Store the DF to mysql

