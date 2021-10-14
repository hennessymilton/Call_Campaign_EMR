import pandas as pd
import pyodbc
import sys
import query_emrr
import query_ptr

def Query(database, sql, query_name):
      DB = {'servername': 'EUS1PCFSNAPDB01',
            'database': database}
      # create the connection
      try:
            conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + DB['servername'] + ';DATABASE=' + DB['database'] + ';Trusted_Connection=yes') 
      except pyodbc.OperationalError:
            print("""Didn\'t connect to they server""")
            sys.exit(1)
      print('''Connected to Server \t {}'''.format(query_name))
      try:
            df = pd.read_sql(sql, conn)
            return df
      except pyodbc.ProgrammingError:
            print('Can\'t run query\'s on this table right now')

def PTR():
      df = Query('DWWorking', query_ptr.sql, 'Project Tracking')
      df['Today\'s Targeted charts'] =df['Today\'s Targeted charts'].replace(0,1)
      df = df.groupby(['Project Type']).agg({'Today\'s Targeted charts':'sum', 'QA Completed':'sum', 'SB.EMR Remote':'sum'})
      df['coef'] = df['QA Completed'] / df['Today\'s Targeted charts'] * df['SB.EMR Remote']
      df = df.sort_values(by= ['coef'], ascending=True).reset_index().round()#.astype(int)
      df['rank'] = range(0, len(df))
      df['bin'] = pd.qcut(df['rank'], 3,  labels= range(1,4)) #,labels=["good", "medium", "bad"])
      df['bin'] = df['bin'].astype(int)
      df = df[['Project Type', 'bin']]
      return df

def EMR_output(): 
      df = Query('DWWorking', query_emrr.sql, 'Base Table')
      return df