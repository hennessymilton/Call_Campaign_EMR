import pandas as pd
import numpy as np

def clean(cc, today):
    cc.columns = cc.columns.str.replace('/ ','')
    cc = cc.rename(columns=lambda x: x.replace(' ', "_"))
    # cc.drop(columns='top_org', inplace=True)
    cc['DueDate'] = pd.to_datetime(cc['DueDate'], format='%Y%m%d', errors='coerce')
    cc['InsertDate'] = pd.to_datetime(cc['InsertDate'], format='%Y%m%d', errors='coerce')
    cc['Last_Call'] = pd.to_datetime(cc['Last_Call'], format='%Y%m%d', errors='coerce')
    cc['OutreachID'] = cc['OutreachID'].astype(str)

    ### Transform ###
    # Remove special status
    status = ['PNP','ReSchedule','Scheduled', 'Research', 'Past Due', 'ROI Research'] # 'ROI Research'???
    rm_status = cc[~cc['Outreach_Status'].isin(status)].copy()
    return add_col(rm_status, today)

def Last_Call(df, today):
    # create table of unique dates
    lc_df = df[df.Last_Call.notna()].Last_Call.unique().tolist()
    business_dates = pd.DataFrame(lc_df, columns=['Last_Call'])
    # calculate true business days from tomorow 
    business_dates['age'] = business_dates.Last_Call.apply(lambda x: len(pd.bdate_range(x, today)))
    business_dates['age'] -= 1
    lc = df.merge(business_dates, on='Last_Call', how='left')

    f1 = lc.Last_Call.isna()
    lc.age = np.where(f1, lc.DaysSinceCreation, lc.age)
    return lc

def add_col(df, today):
    ### Calculate age from DaysSinceCreation & DaysSinceLC
    cols = ['InsertDate', 'Last_Call']
    for c in cols:
        df[c] = pd.to_datetime(df[c]).dt.date

    df['DaysSinceCreation'] = (today - df['InsertDate']).dt.days

    df = Last_Call(df, today)

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
    df.age_bin = df.age_bin.astype(int)
    # no call flag
    f1 = df.Last_Call.isna()
    df['no_call'] = np.where(f1, 1,0)

    f1 = df['Outreach_Status'] == 'PNP Released'
    df['pend'] = np.where(f1, 1, 0)
    # temp projects
    f1 = df['Project_Type'].isin(['Advantasure'])
    df['temp_project'] = np.where(f1, 1, 0)
    return df

