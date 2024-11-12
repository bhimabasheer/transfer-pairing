#!/usr/bin/env python
# coding: utf-8

# In[6]:


import pandas as pd
import sqlalchemy as db
import argparse,string,re
from sqlalchemy import func,event,text
import datetime
from urllib.parse import quote_plus


# In[7]:


connection_string = (
    'Driver={ODBC Driver 17 For SQL Server};'
    'SERVER=149.83.43.4\SQL191,1445;'
    'Database=StageForSSIS;'
    'UID=StandSSISWorker;'
    'PWD=Stand@102004;'
    'Trusted_Connection=no;'
)
connection_uri = f"mssql+pyodbc:///?odbc_connect={quote_plus(connection_string)}"
engine = db.create_engine(connection_uri, fast_executemany=True)
# In[ ]:


# In[8]:


@event.listens_for(engine, 'before_cursor_execute')
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    print("FUNC call")
    if executemany:
        cursor.fast_executemany = True
        cursor.commit()

connection = engine.connect()
metadata = db.MetaData()


# In[9]:


col_def={'Client Name': db.Text(),
 'Ticker': db.Text(),
 'ISIN': db.Text(),
 'Sedol': db.Text(),
 'Submitter Name': db.Text(),
 'Phone': db.Text(),
 'E-mail': db.Text(),
 'Address 1': db.Text(),
 'Address 2': db.Text(),
 'City': db.Text(),
 'State': db.Text(),
 'ZIP': db.Text(),
 'Country': db.Text(),
 'Account Number': db.Text(),
 'Account Name': db.Text(),
 'Account_uuid':db.Text(),
 'Tax ID': db.Text(),
 'CUSIP': db.Text(),
 'P / S': db.Text(),
 'Currency': db.Text(),
 'FileName': db.Text(),
 'MSD Client Name': db.Text(),
 'Price': db.Numeric(19, 4),
 'Quantity':db.Numeric(19,4),
 'Principal':db.Numeric(19, 4),
 'Running Balance':db.Numeric(19, 4),
 'Trade Date':db.DATE()}


# In[12]:


update_df=pd.read_sql_table('UpdatedData',con=engine)
update_df.set_index('TransId',drop=False,inplace=True)
table_flagb=engine.dialect.has_table(tablename='UpdatedData'+'_BKP',connection=connection)


# In[15]:


if table_flagb:
    upb=db.Table('UpdatedData'+'_BKP', metadata, autoload=True, autoload_with=engine)
    engine.execute("""drop table UpdatedData_BKP""")
update_df.to_sql('UpdatedData'+'_BKP', con=engine, if_exists='append',index=False,dtype=col_def)    


# In[18]:


table_flag=engine.dialect.has_table(tablename='UpdatedData',connection=connection)
if table_flag:
    up=db.Table('UpdatedData', metadata, autoload=True, autoload_with=engine)
    dl=up.delete()
    connection.execute(dl)


# In[19]:


Pair_df=update_df.loc[(update_df['PairID']!='')& update_df['P / S'].isin(['TI','R','FR','UR','TO','D','FD','UD'])]
Pair_df.loc[Pair_df['P / S'].isin(['TI','R','FR','UR']),'PairID']=Pair_df['PairID'].str.split(',').str[0]
Pair_account=Pair_df.groupby(['PairID'])


# In[168]:


max_tid=max(update_df['TransId'])+1
for Pair_name, Pair_group in Pair_account:
    print(Pair_name)
    trans_in=Pair_group[Pair_group['P / S'].isin(['TI','R','FR','UR'])].iloc[0]
    TO_bid=list(update_df.loc[(update_df['AccountsId']==trans_in['AccountsId'])&(update_df['P / S']=='B')]['TransId'])[0]
    TO_bqty=list(update_df.loc[(update_df['AccountsId']==trans_in['AccountsId'])&(update_df['P / S']=='B')]['Quantity'])[0]
    TO_bdate=list(update_df.loc[(update_df['AccountsId']==trans_in['AccountsId'])&(update_df['P / S']=='B')]['Trade Date'])[0]
    
    for pair_ind,pair_row in Pair_group.iterrows():
                       
        #print(pair_row['P / S'])
        if pair_row['P / S']in ['TO','D','FD','UD']:
            
            match_list=eval(pair_row['MatchUp'])
            #print(match_list)
            TO_aid=pair_row['AccountsId']
            TO_cid=pair_row['Client Name']
            for mid,m_qty in match_list:
                O_qty=list(update_df.loc[update_df['TransId']==mid]['Quantity'])[0]
                #O_qty=O_qty['qty'][0]
                m_type=list(update_df.loc[update_df['TransId']==mid]['P / S'])[0]
                m_date=list(update_df.loc[update_df['TransId']==mid]['Trade Date'])[0]
                
                #m_type=m_type['section_cd'][0]
                if  m_type=='B':
                    update_df.loc[update_df['TransId']==mid,'Quantity']=O_qty-m_qty
                    update_df.loc[update_df['TransId']==TO_bid,'Quantity']=TO_bqty+m_qty

                    update_df.loc[update_df['TransId']==TO_bid,'PairID']=Pair_name
                    
                   
                    #print('ist')
                elif m_type!='B':
                    m_price=list(update_df.loc[update_df['TransId']==mid]['Price'])[0]
                    if m_price==0:
                        m_net_amt=0
                    else:
                        m_net_amt=m_qty*m_price
                    update_df.loc[update_df['TransId']==mid,'Quantity']=O_qty-m_qty
                    p_series=pd.DataFrame.from_dict({'P / S':[m_type],'Quantity':m_qty,
                                                      'Trade Date':m_date,'AccountsId':trans_in['AccountsId'],
                                                       'Client Name':TO_cid,'PairID':Pair_name,
                                                    'Price':m_price,'Principal':m_net_amt,'TransId':max_tid,
                                                     'Account Number':trans_in['Account Number'],
                                                     'Account Name':trans_in['Account Name'],
                                                      'Submitter Name': trans_in['Account Name'],
                                                         'Phone': trans_in['Phone'],
                                                         'E-mail': trans_in['E-mail'],
                                                         'Address 1': trans_in['Address 1'],
                                                         'Address 2': trans_in['Address 2'],
                                                         'City': trans_in['City'],
                                                         'State': trans_in['State'],
                                                         'ZIP': trans_in['ZIP'],
                                                         'Country': trans_in['Country'],
                                                                                      'Tax ID': trans_in['Tax ID'],
                                                         'CUSIP': trans_in['CUSIP'],
                                                         'Currency': trans_in['Currency']

                                                     })
                    update_df=pd.concat([update_df,p_series]).reset_index(drop=True)
                    
                    #print('2st',p_series)
                max_tid=max_tid+1

                

# In[7]:
for index, row in update_df.iterrows():


    special_char=set(string.punctuation) 
    acc_num_set=set(str(row['Account Number']))
    acc_nam_set=set(str(row['Account Name']))
    if row['AcNoSplChr']==1 or row['AcNoSplChr']==True:
       
        #update_df.loc[index,'AcNoSplChr']=1
        update_df.loc[index,'Account Number']=re.sub('[{}]'.format(string.punctuation),'',row['Account Number'])
    if row['AcNameSplChr']==1 or row['AcNameSplChr']==True:
        #update_df.loc[index,'AcNameSplChr']=1
        update_df.loc[index,'Account Name']=re.sub('[{}]'.format(string.punctuation),'',row['Account Name'])
    #principal=row['Quantity']*row['Price']
    #if round(row['Principal'],2)!=round(principal, 2) and row['P / S'] in ['P','S']:
        #update_df.loc[index,['Principal']]= round(principal, 2)

# In[ ]:


update_df.to_sql('UpdatedData', con=engine, if_exists='append',index=False,dtype=col_def)
connection.close()
print('completed')


# In[ ]:




