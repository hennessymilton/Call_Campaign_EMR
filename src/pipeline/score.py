import pandas as pd
import numpy as np
from datetime import datetime, timedelta
today = datetime.today()
tomorrow = (today + timedelta(days = 1))

# def rank(df, grouping='PhoneNumber'):
#     df.sort_values(['Skill', grouping,'meet_sla', 'has_call', 'togo_bin', 'age']
#                ,ascending=[True, True, True, True, False, False], inplace=True)
#     df['overall_rank'] = 1
#     df['overall_rank'] = df.groupby(['Skill', grouping,])['overall_rank'].cumsum()
#     f1 = df.overall_rank == 1
#     df['parent'] = np.where(f1, 1, 0)
#     df.sort_values(['Skill', 'parent','meet_sla', 'has_call', 'togo_bin', 'age']
#                 ,ascending=[True, True, True, True, True, False, False], inplace=True)
#     df.Score = 1
#     df.Score = df.groupby(['Skill', 'parent'])['Score'].cumsum()
#     df['Matchees'] = df.groupby([grouping])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
#     return df

def rank(df, new_col, groups=list, rank_cols=dict):
    sort_columns = groups + [*rank_cols.keys()]
    ascending    = [True] * len(groups) + [*rank_cols.values()]
    
    df.sort_values(sort_columns,ascending=ascending, inplace=True)
    df[new_col] = 1
    df[new_col] = df.groupby(groups)[new_col].cumsum()
    return df

def stack_inventory(df, grouping):
    rank_cols = {'pend':True,'meet_sla':True,'no_call':False, 'togo_bin':False, 'age':False}

    # group by phone number or msid & rank highest value org
    df = rank(df,'overall_rank',['Skill', grouping], rank_cols)

    # top overall per group = parent
    f1 = df.overall_rank == 1
    df['parent'] = np.where(f1, 1, 0)

    # re-rank parent orgs
    df = rank(df,'Score' , ['Skill','parent'], rank_cols)
    df.OutreachID = df.OutreachID.astype(str)
    df['Matchees'] = df.groupby([grouping])['OutreachID'].transform(lambda x : '|'.join(x)).apply(lambda x: x[:3000])
    return df

# def pt_score(df):
#       df['Today\'s Targeted charts'] =df['Today\'s Targeted charts'].replace(0,1)
#       df = df.groupby(['Project Type']).agg({'Today\'s Targeted charts':'sum', 'QA Completed':'sum', 'SB.EMR Remote':'sum'})
#       df['coef'] = df['QA Completed'] / df['Today\'s Targeted charts'] * df['SB.EMR Remote']
#       df = df.sort_values(by= ['coef'], ascending=True).reset_index().round()#.astype(int)
#       df['rank'] = range(len(df))
#       df['bin'] = pd.qcut(df['rank'], 3,  labels= range(1, 4))
#       df['bin'] = df['bin'].astype(int)
#       df = df[['Project Type', 'bin']]
#       return df