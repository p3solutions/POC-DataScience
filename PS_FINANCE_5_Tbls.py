import sys
import pandas as pd
import pymysql
import pymssql
from collections import Counter 
host_id = 'ec2-34-213-4-182.us-west-2.compute.amazonaws.com'
pno = 1433
uid = 'sa'
pwd = 'secret@p3'
db_name = 'PS_Finance'
db_type = 1
if db_type == 0:
    cnx = pymysql.connect(host=host_id, port=pno, user=uid, passwd=pwd, db=db_name)
    schema = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME like '%%' and TABLE_SCHEMA = '%s'"%(db_name)
elif db_type == 1:
    cnx = pymssql.connect(host=host_id, port=pno, user=uid, password=pwd, database=db_name)
    schema = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME like '%'"
else:
    print("ENter 0 or 1")
name_five = ['PS_ASSET_LOCATION', 
               'PS_BI_HDR',
               'PS_BI_LINE',
               'PS_BOOK',
               'PS_BOOK_HIST'
              ]
li = name_five
l = []
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
sim_df.to_csv('matrix_PS_FINANCE.csv')
df_a = pd.read_csv("matrix.csv")
df_a = df_a.set_index(df_a.columns[0])
df_a = df_a.stack().reset_index()
df_1 = df_a.iloc[:,0]
df_2 = df_a.iloc[:,1]
df_x = df_a.join(df_1.str.split('.', 1, expand=True).rename(columns={0:'Table_1', 1:'Column_1'}))[['Table_1','Column_1']]
df_y = df_a.join(df_2.str.split('.', 1, expand=True).rename(columns={0:'Table_2', 1:'Column_2'}))[['Table_2','Column_2',0]]
df_match = pd.concat([df_x, df_y], axis = 1, sort = False)
df_match.rename(columns={0: 'Match_Pct'}, inplace=True)
df_match.to_csv("Match_PS_FINANCE_5.csv")
print("File 'Match_PS_FINANCE_5.csv' is successfully saved in the folder")










