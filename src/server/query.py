import pandas as pd
import pyodbc

def query(servername, database, sql, query_name):
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
            query(servername, database, sql, query_name)
      else:
            print(f'''Connected to Server \t {query_name}''')
            df = pd.read_sql(sql, conn)
            return df