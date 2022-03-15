import pandas as pd
import numpy as np
from datetime import datetime
import time

import pipeline.agent_assignment
import pipeline.score
from pipeline.etc import next_business_day, time_check, table_drops, daily_piv
from pipeline.outreach_status import outreach_status

import server.call_campaign
import server.call_campaignV2
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
    try:
        cc = pd.read_csv(f'data/extract/{file}')
    except:
        # Save Table
        cc_query = server.call_campaignV2.emrr()
        cc = server.query.query(servername, database, cc_query, 'Base Table')
        time_check(startTime_1, 'EMR_output')
        table_drops("push",'extract', cc, file)

    cc.columns = cc.columns.str.replace('/ ','')
    cc = cc.rename(columns=lambda x: x.replace(' ', "_"))
    cc.drop(columns='top_org', inplace=True)
    cc['OutreachID'] = cc['OutreachID'].astype(str)

    status_log_raw = pd.read_csv(f'data/table_drop/status_log.csv')
    cc_status = outreach_status(cc, status_log_raw)
    # Project Tracking Report
    # pt_query = server.project_tracking.ptr()
    # pt = server.query.query(servername, database, pt_query, 'Project Tracking')
    # pt_scored = pipeline.score.pt_score(pt)

    # Add score to campaign
    # df0 = pd.merge(cc, pt_scored, on=['Project Type'], how='left')
    # time_check(startTime_1, 'Finished Query')

    ### Transform ###
    # Remove special status
    status = ['PNP','ReSchedule','Scheduled', 'Research', 'Past Due', 'ROI Research'] # 'ROI Research'???
    rm_status = cc_status[~cc_status['Outreach_Status'].isin(status)].copy()

    def add_col(df):
        ### Calculate age from DaysSinceCreation & DaysSinceLC
        cols = ['InsertDate', 'Last_Call']
        df[cols] = df[cols].apply(pd.to_datetime, errors='coerce')

        df['DaysSinceCreation'] = (tomorrow - df['InsertDate']).dt.days

        df['age'] = (tomorrow - df['Last_Call']).dt.days

        f1 = df['Last_Call'].isna()
        df.age = np.where(f1, df.DaysSinceCreation, df.age)

        audit_sort  = {'RADV':1, 'HEDIS':2, 'Medicaid Risk':3, 'Specialty':4,  'ACA':5, 'Medicare Risk':6}
        df['audit_sort'] = df['Audit_Type'].map(audit_sort)
        # use map 
        f1 = df.audit_sort <=2
        df['sla'] = np.where(f1, 3, 4)
        f1 = df.audit_sort > 3
        df['sla'] = np.where(f1, 8, df.sla)
        f1 = df.sla >= df.age
        df['meet_sla'] = np.where(f1, 1,0)
        # togo charts
        bucket_amount = 10
        labels = list(([x for x in range(bucket_amount)]))
        df['age_bin'] = pd.cut(df.age, bins=bucket_amount, labels=labels)
        df.age_bin = df.age_binS.astype(int)
        # no call flag
        f1 = df.Last_Call.isna()
        df['no_call'] = np.where(f1, 1,0)

        f1 = df['Outreach_Status'] == 'PNP Released'
        df['pend'] = np.where(f1, 0, 1)
        return df

    add_sla = add_col(rm_status)

    def Skills(df):
        df['Skill'] = 'none'
        df['Outreach_Status'] = df['Outreach_Status'].str.strip()

        def general(df):
            f1 = df['Outreach_Status'] == 'Unscheduled'
            df.Skill = np.where(f1, 'Unscheduled', df.Skill)
            return df
        
        def escalated(df):
            ls = ['Acct Mgmt Research', 'Escalated', 'PNP Released']
            f1 = df['Outreach_Status'].isin(ls)
            df.Skill = np.where(f1, 'Escalated', df.Skill)
            return df
        f = general(df)
        return escalated(f)

    Skilled = Skills(add_sla)

    scored = pipeline.score.stack_inventory(Skilled, 'PhoneNumber')

    piv = daily_piv(scored).reset_index()
    # Save final product
    table_drops('push','load',scored,file)
    table_drops('push','load', piv,'Coordinator_Pivot.csv')
    print(daily_piv(scored))

### Upload files
if __name__ == '__main__':
    main()
    