import pandas as pd

def outreach_status(df, status):
    s = status[['Status','Group ID or Org #']].copy()
    s.columns = ['status','orgs']

    s['orgs_clean'] = s['orgs'].apply(
                    lambda x: ''.join([c for c in x if c.isdigit() or c == ',' or c.isspace()]) ).str.replace(',',' ')

    clean = pd.DataFrame(s['orgs_clean'].str.split(' ').tolist())
    clean['status'] = s.status
    melt = clean.melt(id_vars='status')[['status','value']]
    
    melt.columns = ['status','OutreachID']
    ls = melt.dropna(subset=['OutreachID'])

    return df.merge(ls, on=['OutreachID'], how='left')
