#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine
import urllib
import datetime
import shutil
import glob

# Paths for files and archive
Path_source = r'\\appsrv07\Python filer\DSV fakturaspecifikationer'
Path_Glob = Path_source + '\*.csv'
Path_archive = r'\\appsrv07\Python filer\DSV fakturaspecifikationer\Arkiv'
Files_in_path = glob.glob(Path_Glob)

# SQL Server data for destination of specifications
Schema = 'dbo'
server_04 = "sqlsrv04"
db_ds = "BKI_Datastore"
params_ds = f"DRIVER={{SQL Server Native Client 11.0}};SERVER={server_04};DATABASE={db_ds};trusted_connection=yes"
con_ds = create_engine('mssql+pyodbc:///?odbc_connect=%s' % urllib.parse.quote_plus(params_ds))

# Other variables
Timestamp = datetime.datetime.now()
Script_name = 'DSV invoices.py'
I = 0
Files_error = []

Cols_int = ['Invoice date','Delivery_Date','Pickup_Date','Account_Code']
Cols_dec = ['Colli','Grossweight','Billing_Weight','LDM','CBM','Exchange_Rate','VAT_Amount','Amount']
Cols_string = ['Filename','Invoice no','Customer','Marks','Collitype','Content','Department','Traffic','Transport','Trip'
               ,'Consignment_Number','Customer_Ref1','Customer_Ref2','Terms','Shipper_Name','Shipper_Address1'
               ,'Shipper_Address2','Shipper_Address3','Shipper_Address4','Shipper_Country','Shipper_Postcode'
               ,'Consignee_Name','Consignee_Address1','Consignee_Address2','Consignee_Address3','Consignee_Address4'
               ,'Consignee_Country','Consignee_Postcode','Equipment1','Equipment2','Equipment_Type','Text','Currency'
               ,'Calculation and texts','Sub Customer','Partner Ref ID','Partner Ref E1']

def concatenate_list_data(list):
    result= ''
    for element in list:
        result += '\n' + str(element)
    return result

# Read each .csv file in folder
for File_name in Files_in_path:
    try:
        File_name_clean = File_name.replace(Path_source ,'' ,1)
        Df_file = pd.read_csv(File_name ,encoding='iso8859_10' ,header=0 ,sep=';' ,thousands=',')
        Df_file.loc[: ,'Filename'] = File_name_clean
        # Convert string and numeric datatypes
        Df_file[Cols_string] = Df_file[Cols_string].astype(str)
        Df_file[Cols_dec] = Df_file[Cols_dec].apply(pd.to_numeric)
        Df_file['Amount'] = Df_file['Amount'].div(100)
        # Fill int NA with placeholder and convert til int        
        Df_file[Cols_int] = Df_file[Cols_int].fillna(99999999)
        Df_file[Cols_int] = Df_file[Cols_int].astype(int)
        # Ensure all NULL values are actual NULL values before insert
        Df_file = Df_file.replace({'nan':None ,'NONE':None ,'NaN':None ,99999999:None})
        # Insert data into SQL table        
        Df_file.to_sql('DSV_fakturaspecifikationer' ,con=con_ds ,schema=Schema ,if_exists='append' ,index=False)
        # Move file to archive folder
        shutil.move(File_name ,Path_archive + File_name_clean)
        # Count number of files successfully read into SQL
        I += 1
    except Exception as e:
        # add file that failed to run to list
        Files_error.append(File_name_clean)
        error_code = e
        print(e)

# Log inserts for succes and errors
# Log success
df_log_suc = pd.DataFrame(data= {'Event': Script_name ,'Note': str(I) + ' fil(er) indlæst' } , index=[0] )

if I > 0:
    df_log_suc.to_sql('Log' ,con=con_ds ,schema=Schema ,if_exists='append' ,index=False)
 

if len(Files_error) > 0:
    # Log errors and insert into email_log
    df_log_err = pd.DataFrame(data= {'Event': Script_name ,'Note': str(len(Files_error)) + f" fil(er) fejlet ved indlæsning \n  Fejl: {error_code}" } , index=[0] )
    df_log_err.to_sql('Log' ,con=con_ds ,schema=Schema ,if_exists='append' ,index=False)
    

