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
Path_source = r'\\filsrv01\bki\11. Økonomi\04 - Controlling\NMO\3. Logistik\DSV fakturaspecifikationer\*.csv'
Path_archive = r'\\filsrv01\bki\11. Økonomi\04 - Controlling\NMO\3. Logistik\DSV fakturaspecifikationer\Arkiv'
Files_in_path = glob.glob(Path_source)

# SQL Server data for destination of specifications
Server = 'sqlsrv04'
Db = 'BKI_Datastore'
Schema = 'dbo'
Params = urllib.parse.quote_plus('DRIVER={SQL Server Native Client 11.0};SERVER=' + Server +';DATABASE=' + Db +';Trusted_Connection=yes')
Engine = create_engine('mssql+pyodbc:///?odbc_connect=%s' % Params)

# Other variables
Timestamp = datetime.datetime.now()
Script_name = 'DSV invoices.py'



print(Files_in_path)
print(len(Files_in_path))