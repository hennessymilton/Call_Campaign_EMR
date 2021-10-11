import pandas as pd
import time
from datetime import date, timedelta, datetime

from dbo_Query import Query
# from Bus_day_calc import newPath
startTime_1 = time.time()

today = date.today()

def PTR():
    # query db
    sql = """
            SELECT 
                  A.[Project Year]
            , A.[Audit Type]
            , A.[Project Type]
            , A.[Client Project]
            , A.[Project ID]
            , A.[Today's Targeted charts]
            , A.[Targeted charts]
            , A.[QA Completed]
            , A.[SB.EMR Remote]
            -- , A.[AV.EMR Remote]
            FROM DWWorking.Prod.Project_Tracking_Report_V2 AS A
            WHERE A.[Insert Date] = CAST(GETDATE() AS DATE)
                  AND [Project Status] IN ('New', 'In Process')
                  AND A.[Project Due Date] >= CAST(GETDATE() AS DATE)
                  and A.[Net Charts] > 0
            """

    df = Query('DWWorking', sql, 'Project Tracking')
    df['Today\'s Targeted charts'] =df['Today\'s Targeted charts'].replace(0,1)
      # df = df.groupby(['Project Type']).agg({'Today\'s Targeted charts':'sum', 'QA Completed':'sum'})
    df = df.groupby(['Project Type']).agg({'Today\'s Targeted charts':'sum', 'QA Completed':'sum', 'SB.EMR Remote':'sum'})
      
    df['coef'] = df['QA Completed'] / df['Today\'s Targeted charts'] * df['SB.EMR Remote']
    df = df.sort_values(by= ['coef'], ascending=True).reset_index().round()#.astype(int)
    df['rank'] = range(0, len(df))
    df['bin'] = pd.qcut(df['rank'], 3,  labels= range(1,4)) #,labels=["good", "medium", "bad"])
    df['bin'] = df['bin'].astype(int)
    df = df[['Project Type', 'bin']]
    return df

# df = PTR()
# print(df)

# executionTime_1 = (time.time() - startTime_1)
# print("-----------------------------------------------")
# print('Time: ' + str(executionTime_1))
# print("-----------------------------------------------")