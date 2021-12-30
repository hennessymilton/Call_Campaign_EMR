import pandas as pd
import numpy as np
from datetime import datetime
today = datetime.today()

### Calculate age from DaysSinceCreation & DaysSinceLC
def age(df):
    cols = ['InsertDate', 'Project Due Date', 'Last Call Date']
    df[cols] = df[cols].apply(pd.to_datetime, errors='coerce')

    df['DaysSinceCreation'] = round((today - df['InsertDate'])/np.timedelta64(1,'D'))
    df['DaysSinceLC'] = round((today - df['Last Call Date'])/np.timedelta64(1,'D'))

    df['Last Call Date'] = df['Last Call Date'].dt.strftime('%Y-%m-%d')

    filter1 = df['DaysSinceLC'] > df['DaysSinceCreation']
    df['Age'] = np.where(filter1, df['DaysSinceCreation'], df['DaysSinceLC'])
    return df

### Ranking ###
#   Medicare Risk   = 1
#   Medicaid Risk   = 2
#   RADV            = 3
#   HEDIS           = 4
#   Specialty       = 6
#   ACA             = 17

def cc_score_deduplicate(df):
    audit_sort = {3:0, 2:1, 4:2, 6:3,  17:4, 1:5}
    df['audit_sort'] = df['Audit Type'].map(audit_sort)

    df2 = df.groupby(['Phone Number']).agg({'bin':'mean', 'ToGo Charts':'sum', 'Age':'mean'}).rename(columns={'bin':'bin_agg', 'ToGo Charts':'togo_agg','Age':'age_avg'}).reset_index()
    skilled = pd.merge(df,df2, on='Phone Number', how='left')

    skilled['coef'] = skilled['bin_agg'] / skilled['togo_agg']
    skilled['coef'] = skilled['coef'].fillna(max(skilled['coef']))
    skilled['bin_coef'] = pd.qcut(skilled['coef'], 3, labels= range(1,4))
    skilled['bin_coef'] = skilled['bin_coef'].astype(int)
    df_rank = skilled.sort_values(by = ['Phone Number', 'audit_sort', 'bin']).reset_index(drop= True)

    # Parent / Child
    df_rank['Unique_Phone'] = 'Child'
    df_unique = df_rank.drop_duplicates(['Phone Number']).reset_index(drop = True)
    df_unique['Unique_Phone'] = 'Parent'
    df_unique = df_unique.sort_values(by = ['audit_sort','bin_coef', 'age_avg'], ascending=[True, True, False]).reset_index(drop= True)

    df_unique['rank'] = range(0, len(df_unique))

    # Add Unique ORGs to Rank list 
    df_full = df_unique.append(df_rank)
    df_clean = df_full.drop_duplicates(['OutreachID']).reset_index(drop= True)

    ### Piped ORGs attached to phone numbers
    df_clean['OutreachID'] = df_clean['OutreachID'].astype(str)
    df_clean['Matches'] = df_clean.groupby(['Phone Number'])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
    return df_clean

def pt_score(df):
      df['Today\'s Targeted charts'] =df['Today\'s Targeted charts'].replace(0,1)
      df = df.groupby(['Project Type']).agg({'Today\'s Targeted charts':'sum', 'QA Completed':'sum', 'SB.EMR Remote':'sum'})
      df['coef'] = df['QA Completed'] / df['Today\'s Targeted charts'] * df['SB.EMR Remote']
      df = df.sort_values(by= ['coef'], ascending=True).reset_index().round()#.astype(int)
      df['rank'] = range(0, len(df))
      df['bin'] = pd.qcut(df['rank'], 3,  labels= range(1,4)) #,labels=["good", "medium", "bad"])
      df['bin'] = df['bin'].astype(int)
      df = df[['Project Type', 'bin']]
      return df