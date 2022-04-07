import numpy as np

def skills(df):
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