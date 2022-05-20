import pandas as pd

import server.active_chartfinder
import server.call_campaignV4
import server.connections

server_name = 'EUS1PCFSNAPDB01'
database    = 'DWWorking'
table       = 'Call_Campaign'
dwworking   = server.connections.MSSQL(server_name, database)
dw_engine   = dwworking.create_engine()

def update_table():
    # Save Table
    # pull dw_ops table
    cc_query = server.call_campaignV4.emrr()
    v3 = pd.read_sql(cc_query, dw_engine)
    # validate active inventory in cf
    check = server.active_chartfinder.sql()
    check_df = pd.read_sql(check, dw_engine)
    # keep active orgs
    cc = v3.merge(check_df, on='OutreachID', how='inner')
    return cc