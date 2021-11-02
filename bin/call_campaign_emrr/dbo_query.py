import pandas as pd
import pyodbc
import query_emrr
import query_ptr

def Query(database, sql, query_name):
      servername = 'EUS1PCFSNAPDB01'
      # create the connection
      try:
            conn = pyodbc.connect(f"""
                  DRIVER={{SQL Server}};
                  SERVER={servername};
                  DATABASE={database};
                  Trusted_Connection=yes""",
                  autocommit=True) 
      except pyodbc.OperationalError:
            print("""Couldn\'t connect to server""")
            Query(database, sql, query_name)
      else:
            print(f'''Connected to Server \t {query_name}''')
            df = pd.read_sql(sql, conn)
            return df

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
if __name__ == '__main__':
      df = EMR_output()
      df1 = df[(df['OutreachID'] == 26132574) | (df['OutreachID'] == 26135744)]
      print(df1)
      