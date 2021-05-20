#!/usr/bin/env python3

import os.path
import pandas as pd
from sqlalchemy import create_engine
import urllib
import datetime
import shutil
import numpy as np
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
Files_count = len(Path_Glob)
Files_error = []

Cols_int = ['Invoice date','Delivery_Date','Pickup_Date','Account_Code']
Cols_dec = ['Colli','Grossweight','Billing_Weight','LDM','CBM','Exchange_Rate','VAT_Amount','Amount']
Cols_string = ['Filename','Invoice no','Customer','Marks','Collitype','Content','Department','Traffic','Transport','Trip'
               ,'Consignment_Number','Customer_Ref1','Customer_Ref2','Terms','Shipper_Name','Shipper_Address1'
               ,'Shipper_Address2','Shipper_Address3','Shipper_Address4','Shipper_Country','Shipper_Postcode'
               ,'Consignee_Name','Consignee_Address1','Consignee_Address2','Consignee_Address3','Consignee_Address4'
               ,'Consignee_Country','Consignee_Postcode','Equipment1','Equipment2','Equipment_Type','Text','Currency'
               ,'Calculation and texts','Sub Customer']


for File_name in Files_in_path:
    try:
        Df_file = pd.read_csv(File_name ,header=0 ,sep=';' ,thousands=',')
        Df_file.loc[: ,'Filename'] = File_name.replace(Path_source ,'' ,1)
        
        Df_file[Cols_string] = Df_file[Cols_string].astype(str)
        Df_file[Cols_dec] = Df_file[Cols_dec].apply(pd.to_numeric)
        
        Df_file[Cols_int] = Df_file[Cols_int].fillna(99999999)
        Df_file[Cols_int] = Df_file[Cols_int].astype(int)
    
        Df_file = Df_file.replace({'nan':None ,'NONE':None ,'NaN':None ,99999999:None}) # Convert text NULL values to actual NULL values
        
        I += 1
    except:
        Files_error.append(File_name)
        pass

    Df_file.to_sql('DSV_fakturaspecifikationer' ,con=Engine ,schema=Schema ,if_exists='append' ,index=False) #Insert file into SQL

print(I)
print(Files_error)