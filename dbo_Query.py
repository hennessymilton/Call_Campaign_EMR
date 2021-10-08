import pandas as pd
import pyodbc
import sys

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
      df = pd.read_sql(sql, conn)
      return df
query = 'SELECT * FROM Call_Campaign WHERE LOAD_Date = CAST(GETDATE() AS DATE)'
Query('DWWorking', query, 'Base Table')