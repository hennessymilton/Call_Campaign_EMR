import pandas as pd
import numpy as np

def outreach_status(df, status):
    s = status[['Status','Group ID or Org #']].copy()
    s.columns = ['status','orgs']

    s['orgs_clean'] = s['orgs'].apply(
                    lambda x: ''.join([c for c in x if c.isdigit() or c == ',' or c.isspace()]) ).str.replace(',',' ')

    clean = pd.DataFrame(s['orgs_clean'].str.split(' ').tolist())
    clean['status'] = s.status
    melt = clean.melt(id_vars='status')[['status','value']]
    
    melt.columns = ['status','OutreachID']
    ls = melt.replace('', np.nan).dropna(subset=['OutreachID'])
    ls.OutreachID = ls.OutreachID.astype(int)
    return pd.merge(df, ls, on=['OutreachID'], how='left')
