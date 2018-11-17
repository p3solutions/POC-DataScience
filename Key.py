#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 17 11:01:11 2018

@author: gnanakiran
"""

import sys
import pandas as pd
import pymysql
import pymssql
from collections import Counter 

#db_type = int(input("Enter '0' for MySQL and '1' for MS-SQL: "))

#host_id = sys.argv[1]
#pno = int(sys.argv[2])
#uid = sys.argv[3]
#pwd = sys.argv[4]
#db_name = sys.argv[5]
#db_type = int(sys.argv[6])

host_id = "ec2-34-213-4-182.us-west-2.compute.amazonaws.com"
pno = 1433
uid = "sa"
pwd = "secret@p3"
db_name = "CLAIMS_SYS"
#
#if db_type == 0:
#cnx = pymysql.connect(host=host_id, port=pno, user=uid, passwd=pwd, db=db_name)
#schema = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME like '%%' and TABLE_SCHEMA = '%s'"%(db_name)
#elif db_type == 1:
cnx = pymssql.connect(host=host_id, port=pno, user=uid, password=pwd, database=db_name)
schema = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME like '%'"
#else:
#    print("ENter 0 or 1")

df_s = pd.read_sql(sql = schema, con = cnx, index_col = None, 
                 coerce_float = True, params = None, parse_dates = None, 
                 columns = None, chunksize = None)

li = df_s['TABLE_NAME']
l=[]

for t in li:
    get_col_tab1 = "select * from %s"%t
    data_tab = pd.read_sql(sql = get_col_tab1, con = cnx)
    data_tab.rename(columns = lambda x: t+'.'+x, inplace = True)
    l.append(data_tab)
    result = pd.concat(l, axis = 1, sort = False)
    
df = pd.read_csv("Match_Pct.csv")
df['T1C1'] = df.apply(lambda x: x['Table_1'] + '.' + x['Column_1'], 1)
df['T2C2'] = df.apply(lambda x: x['Table_2'] + '.' + x['Column_2'], 1)
#df_new = df[['T1C1', 'T2C2', 'Match_Pct']]
pdf = pd.DataFrame([])
for idx, item in df.iterrows():
    indx = list(df['T2C2']).index(str(df['T1C1'][idx]))
    pdf = pdf.append(pd.DataFrame({'Match_Pct_2': df.iloc[indx][5]}, index=[0]), ignore_index=True)
data = pd.concat([df, pdf], axis=1)
data_1 = data[['Match_Pct', 'Match_Pct_2']]
#result_2 = pd.concat([df[['Table_1', 'Column_1', 'Table_2', 'Column_2']], data_1], axis=1, sort=False)
result_2 = pd.concat([df[['Table_1', 'Column_1', 'Table_2', 'Column_2', 'T1C1', 'T2C2']], data_1], axis=1, sort=False)

#result['Prediction'] = np.where(int(result['Match_Pct'])<100 & int(result['Match_Pct_2'])==100, 'Foreign', 'no')
result_2['Match_Pct'] = result_2['Match_Pct'].astype(int)
result_2['Match_Pct_2'] = result_2['Match_Pct_2'].astype(int)
def Prediction(row):
    if(row['Match_Pct'] < 100 & row['Match_Pct_2'] == 100):
        return 'Primary Key'
    elif(row['Match_Pct'] == 100 & row['Match_Pct_2'] < 100):
        return 'Foreign Key'
    elif(row['Match_Pct'] < 100 & row['Match_Pct_2'] < 100):
        return '-'
    elif(row['Match_Pct'] == 100 & row['Match_Pct_2'] == 100):
        u = row['T1C1']
        if(any(result[u].duplicated())):
            return 'Foreign Key'
        else:
            return 'Primary Key'
result_3 = pd.concat([result_2[['Table_1','Column_1','Table_2','Column_2']], result_2.apply(lambda row: Prediction(row), axis = 1)], axis = 1, sort = False)
result_3.to_csv("Relation.csv")