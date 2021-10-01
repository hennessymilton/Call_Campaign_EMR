import pandas as pd
import pyodbc
import sys

def Query(database, sql):
      DB = {'servername': 'EUS1PCFSNAPDB01',
            'database': database}
      # create the connection
      conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + DB['servername'] + ';DATABASE=' + DB['database'] + ';Trusted_Connection=yes') 

      df = pd.read_sql(sql, conn)
      return df