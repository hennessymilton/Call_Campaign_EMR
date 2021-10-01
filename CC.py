import pandas as pd
import numpy as np
import pyodbc
from datetime import date, timedelta, datetime
from dbo_Query import Query
from dbo_PTR import PTR
from dbo_EMR import EMR_output
from Bus_day_calc import next_business_day, Next_N_BD, daily_piv, map_piv, newPath, time_check, x_Bus_Day_ago
import time
today1 = datetime.today()
tomorrow = next_business_day(today1)
startTime_1 = time.time()

### Get tables ###
df00 = EMR_output()
time_check(startTime_1, 'EMR_output')
path = newPath('dump','Extract')
df00.to_csv(path + tomorrow.strftime("%Y-%m-%d") + '.csv')

df0 = pd.merge(df00, PTR(), on=['Project Type'])

path_name = newPath('Table_Drop','')
names = pd.read_csv(path_name + 'Coordinators.csv', sep=',')
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
name = names[['State', 'Name', 'Agent ID','CF Username']]	

df = pd.merge(df0, name, on=['State'])
# --------------------------------------------------------------------------- 
time_check(startTime_1, 'Add Names')
# --------------------------------------------------------------------------- 

### Calculate age from DaysSinceCreation & DaysSinceLC
df['InsertDate'] = pd.to_datetime(df['InsertDate'])
df['Project Due Date'] = pd.to_datetime(df['Project Due Date'])
df['Last Call Date'] = pd.to_datetime(df['Last Call Date'])
df['DaysSinceCreation'] = ((today1 - df['InsertDate'])/np.timedelta64(1,'D')).round()
df['DaysSinceLC'] = ((today1 - df['Last Call Date'])/np.timedelta64(1,'D')).round()
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
path = newPath('dump','Transform')
df_clean.to_csv(path + tomorrow.strftime("%Y-%m-%d") + '.csv')
path = newPath('dump','Load')
df_clean.to_csv(path + tomorrow.strftime("%Y-%m-%d") + '.csv')
print(df_clean)

