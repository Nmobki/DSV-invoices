#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
from sqlalchemy import create_engine
import urllib
import datetime
import shutil
import glob

# Paths for files and archive
Path_source = r'\\filsrv01\bki\11. Økonomi\04 - Controlling\NMO\3. Logistik\DSV fakturaspecifikationer'
Path_Glob = Path_source + '\*.csv'
Path_archive = r'\\filsrv01\bki\11. Økonomi\04 - Controlling\NMO\3. Logistik\DSV fakturaspecifikationer\Arkiv'
Files_in_path = glob.glob(Path_Glob)

# SQL Server data for destination of specifications
Server = 'sqlsrv04'
Db = 'BKI_Datastore'
Schema = 'dbo'
Params = urllib.parse.quote_plus('DRIVER={SQL Server Native Client 11.0};SERVER=' + Server +';DATABASE=' + Db +';Trusted_Connection=yes')
Engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % Params)

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
               ,'Calculation and texts','Sub Customer']

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
# Fill int NA with placeholder and convert til int        
        Df_file[Cols_int] = Df_file[Cols_int].fillna(99999999)
        Df_file[Cols_int] = Df_file[Cols_int].astype(int)
# Ensure all NULL values are actual NULL values before insert
        Df_file = Df_file.replace({'nan':None ,'NONE':None ,'NaN':None ,99999999:None})
# Insert data into SQL table        
        Df_file.to_sql('DSV_fakturaspecifikationer' ,con=Engine ,schema=Schema ,if_exists='append' ,index=False)
# Move file to archive folder
        shutil.move(File_name ,Path_archive + File_name_clean)
# Count number of files successfully read into SQL
        I += 1
    except:
# add file that failed to run to list
        Files_error.append(File_name_clean)
        pass

# Log inserts for succes and errors
# Log success
df_log_suc = pd.DataFrame(data= {'Event': Script_name ,'Note': str(I) + ' fil(er) indlæst' } , index=[0] )

if I > 0:
    df_log_suc.to_sql('Log' ,con=Engine ,schema=Schema ,if_exists='append' ,index=False)
# Log errors and insert into email_log
df_log_err = pd.DataFrame(data= {'Event': Script_name ,'Note': str(len(Files_error)) + ' fil(er) fejlet ved indlæsning' } , index=[0] )
df_email_err = pd.DataFrame(data= {'Email_til': 'nmo@bki.dk' ,'Email_emne': 'Indlæsning af DSV fakturaspecifikationer fejlet'
     ,'Email_tekst':'Indlæsning af ' + str(len(Files_error)) + ' fakturaspecifikation(er) er fejlet \n'
     + 'Følgende fil(er) er ikke blevet indlæst: \n'
     + concatenate_list_data(Files_error)} ,index=[0])
    


if len(Files_error) > 0:
    df_log_err.to_sql('Log' ,con=Engine ,schema=Schema ,if_exists='append' ,index=False)
    df_email_err.to_sql('Email_log' ,con=Engine ,schema=Schema ,if_exists='append' ,index=False)
    

