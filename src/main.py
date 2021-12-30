import pandas as pd
import numpy as np
from datetime import datetime
import time

import pipeline.agent_assignment
import pipeline.score
from pipeline.etc import next_business_day, time_check, table_drops, daily_piv

import server.call_campaign
import server.project_tracking
import server.secret
import server.query
# from server.query import query

today = datetime.today()
tomorrow = next_business_day(today)
startTime_1 = time.time()
file = f'{today.strftime("%Y-%m-%d")}.csv'

servername  = server.secret.servername
database    = server.secret.database

def main():
    ### Get tables ###
    cc_query = server.call_campaign.emrr()
    cc = server.query.query(servername, database, cc_query, 'Base Table')
    time_check(startTime_1, 'EMR_output')
    # Save Table
    table_drops("push",'extract', cc, file)

    # Project Tracking Report
    pt_query = server.project_tracking.ptr()
    pt = server.query.query(servername, database, pt_query, 'Project Tracking')
    pt_scored = pipeline.score.pt_score(pt)

    # Add score to campaign
    df0 = pd.merge(cc, pt_scored, on=['Project Type'], how='left')
    time_check(startTime_1, 'Finished Query')

    ### Transform ###
    # Remove special status
    for i in ['PNP','ReSchedule','Scheduled','Scheduled','Research']:
        df0 = df0[df0['Outreach Status'] != i]

    # Remove names/date/note outside of specific list
    # Agent name and note will refer to last note
    names = table_drops('pull','table_drop','NA','Coordinator_m.csv')
    df = pipeline.agent_assignment.agent_assignment(df0, names)

    ### Calculate age from DaysSinceCreation & DaysSinceLC
    df_clean = pipeline.score.age(df)

    scored = pipeline.score.cc_score_deduplicate(df_clean)

    ### add columns to the end
    cols_at_end = ['bin_agg','togo_agg','coef','bin_coef','age_avg', 'audit_sort','rank']
    scored = scored[[c for c in scored if c not in cols_at_end] + [c for c in cols_at_end if c in scored]]

    ### test if zero agents were added to market, fill agent with the least amount 
    piv = daily_piv(scored).reset_index()
    backfill = str(piv['Name'].iloc[-1])
    scored['Name'] = scored['Name'].fillna(backfill)
    piv = daily_piv(scored).reset_index()

    # Save final product
    table_drops('push','load',scored,file)
    table_drops('push','load', piv,'Coordinator_Pivot.csv')
    print(daily_piv(scored))

### Upload files
if __name__ == '__main__':
    main()
    