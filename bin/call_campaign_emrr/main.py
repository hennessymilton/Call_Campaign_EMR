import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
from dbo_query import PTR, EMR_output
from etc_function import next_business_day, time_check, table_drops, daily_piv
import time
today = datetime.today()
tomorrow = next_business_day(today)
startTime_1 = time.time()
file = str(tomorrow.strftime("%Y-%m-%d") + '.csv')
### Get tables ###
df00 = EMR_output()
time_check(startTime_1, 'EMR_output')
table_drops("push",'extract',df00,file)

df0 = pd.merge(df00, PTR(), on=['Project Type'])

names = table_drops('pull','table_drop','NA','Coordinator_m.csv')
# --------------------------------------------------------------------------- 
time_check(startTime_1, 'Load Query')
# --------------------------------------------------------------------------- 
### Transform ###
df0 = df0[df0['Outreach Status'] != 'PNP']
df0 = df0[df0['Outreach Status'] != 'ReSchedule']
df0 = df0[df0['Outreach Status'] != 'Scheduled']
df0 = df0[df0['Outreach Status'] != 'Research']
## Remove names/notedate/note outside of specific list
## Agent name and note will refer to last note
filter1 = df0['AgentName'].isin(names['Name'].unique())
filter2 = df0['AgentName'] == 'NA'
df0['AgentName']    = np.where(filter1, df0['AgentName'], 'NA')
df0['NoteDate']     = np.where(filter2, 'NA', df0['NoteDate'])
df0['Note']         = np.where(filter2, 'NA', df0['Note'])
df0 = df0.drop(['CF Username'], axis=1)
### New name column will refer to assigment
name = names[['Name', 'Agent ID','CF Username','Market']]	

df = pd.merge(df0, name, on=['Market'])
# --------------------------------------------------------------------------- 
time_check(startTime_1, 'Add Names')
# --------------------------------------------------------------------------- 

### Calculate age from DaysSinceCreation & DaysSinceLC
df['InsertDate'] = pd.to_datetime(df['InsertDate'])
df['Project Due Date'] = pd.to_datetime(df['Project Due Date'])
df['Last Call Date'] = pd.to_datetime(df['Last Call Date'])
df['DaysSinceCreation'] = round((today - df['InsertDate'])/np.timedelta64(1,'D'))
df['DaysSinceLC'] = round((today - df['Last Call Date'])/np.timedelta64(1,'D'))
df['InsertDate'] = df['InsertDate'].dt.strftime('%Y-%m-%d')
df['Last Call Date'] = df['Last Call Date'].dt.strftime('%Y-%m-%d')
df['Project Due Date'] = df['Project Due Date'].dt.strftime('%Y-%m-%d')
# Age #
filter1 = df['DaysSinceLC'] > df['DaysSinceCreation']
df['Age'] = np.where(filter1, df['DaysSinceCreation'], df['DaysSinceLC'])
# --------------------------------------------------------------------------- 
time_check(startTime_1, 'Age Calc')
# --------------------------------------------------------------------------- 

### Ranking ###
#   Medicare Risk   = 1
#   Medicaid Risk   = 2
#   RADV            = 3
#   HEDIS           = 4
#   Specialty       = 6
#   ACA             = 17
audit_sort = {3:0, 2:1, 4:2, 6:3,  17:4, 1:5}
df['audit_sort'] = df['Audit Type'].map(audit_sort)

df2 = df.groupby(['Phone Number']).agg({'bin':'mean', 'ToGo Charts':'sum', 'Age':'mean'}).rename(columns={'bin':'bin_agg', 'ToGo Charts':'togo_agg','Age':'age_avg'}).reset_index()
df = pd.merge(df,df2, on='Phone Number')

df['coef'] = df['bin_agg'] / df['togo_agg']
df['bin_coef'] = pd.qcut(df['coef'], 3, labels= range(1,4))
df['bin_coef'] = df['bin_coef'].astype(int)
df_rank = df.sort_values(by = ['Phone Number', 'audit_sort', 'bin']).reset_index(drop= True)
# --------------------------------------------------------------------------- 
time_check(startTime_1, 'Create Bins for rank')
# --------------------------------------------------------------------------- 

# Parent / Child
df_rank['Unique_Phone'] = 'Child'
df_unique = df_rank.drop_duplicates(['Phone Number']).reset_index(drop = True)
df_unique['Unique_Phone'] = 'Parent'
df_unique = df_unique.sort_values(by = ['audit_sort','bin_coef', 'age_avg'], ascending=[True, True, False]).reset_index(drop= True)

df_unique['rank'] = range(0, len(df_unique))

# Add Unique ORGs to Rank list 
df_full = df_unique.append(df_rank)
df_clean = df_full.drop_duplicates(['OutreachID']).reset_index(drop= True)
time_check(startTime_1, 'Parent/Child')

### Piped ORGs attached to phone numbers
df_clean['OutreachID'] = df_clean['OutreachID'].astype(str)
df_clean['Matches'] = df_clean.groupby(['Phone Number'])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
time_check(startTime_1, 'Matches column')

### add columns to the end
cols_at_end = ['bin_agg','togo_agg','coef','bin_coef','age_avg', 'audit_sort','rank']
df_clean = df_clean[[c for c in df_clean if c not in cols_at_end] 
                    + [c for c in cols_at_end if c in df_clean]]

### Upload files
table_drops('push','load',df_clean,file)
table_drops('push','load',daily_piv(df_clean).reset_index(),'Coordinator_Pivot.csv')
print(daily_piv(df_clean))