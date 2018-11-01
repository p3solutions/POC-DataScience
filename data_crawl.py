#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 22 11:22:52 2018

@author: gnanakiran
"""

import sys
import pandas as pd
import pymysql
import pymssql
from collections import Counter 

#db_type = int(input("Enter '0' for MySQL and '1' for MS-SQL: "))

host_id = sys.argv[1]
pno = int(sys.argv[2])
uid = sys.argv[3]
pwd = sys.argv[4]
db_name = sys.argv[5]
db_type = int(sys.argv[6])

if db_type == 0:
    cnx = pymysql.connect(host=host_id, port=pno, user=uid, passwd=pwd, db=db_name)
    schema = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME like '%%' and TABLE_SCHEMA = '%s'"%(db_name)
elif db_type == 1:
    cnx = pymssql.connect(host=host_id, port=pno, user=uid, password=pwd, database=db_name)
    schema = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME like '%'"
else:
    print("ENter 0 or 1")

df = pd.read_sql(sql = schema, con = cnx, index_col = None, 
                 coerce_float = True, params = None, parse_dates = None, 
                 columns = None, chunksize = None)

li = df['TABLE_NAME']
l=[]

for t in li:
    get_col_tab1 = "select * from %s"%t
    data_tab = pd.read_sql(sql = get_col_tab1, con = cnx)
    data_tab.rename(columns = lambda x: t+'.'+x, inplace = True)
    l.append(data_tab)
    result = pd.concat(l, axis = 1, sort = False)

def match_pct(col_one, col_two):
    intersection = list((Counter(col_one) & Counter(col_two)).elements())
#    intersection = len(set(col_one) & set(col_two))
    if len(col_one) == 0:
        sim_score = 0
    else:
        sim_score = (len(intersection)/len(col_one)) * 100
    return sim_score


sim_df = pd.DataFrame()

for i in result:
    for j in result:
        col_pair = (i,j)
        if (len(result[i].dropna()) > 20):
            sim_df.loc[col_pair]= match_pct(result[i].dropna().sample(frac = 0.05), result[j].dropna())
        else:
            sim_df.loc[col_pair]= match_pct(result[i].dropna(), result[j].dropna())
        
sim_df.to_csv('matrix.csv')

df_a = pd.read_csv("matrix.csv")
df_a = df_a.set_index(df_a.columns[0])
df_a = df_a.stack().reset_index()

df_1 = df_a.iloc[:,0]
df_2 = df_a.iloc[:,1]
df_x = df_a.join(df_1.str.split('.', 1, expand=True).rename(columns={0:'Table_1', 1:'Column_1'}))[['Table_1','Column_1']]
df_y = df_a.join(df_2.str.split('.', 1, expand=True).rename(columns={0:'Table_2', 1:'Column_2'}))[['Table_2','Column_2',0]]
df_match = pd.concat([df_x, df_y], axis = 1, sort = False)
df_match.rename(columns={0: 'Match_Pct'}, inplace=True)

df_match.to_csv("Match_Pct.csv")
print("File 'Match_Pct.csv' is successfully saved in the folder")
        
        
        
        
        
        
