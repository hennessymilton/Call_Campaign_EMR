import pandas as pd

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