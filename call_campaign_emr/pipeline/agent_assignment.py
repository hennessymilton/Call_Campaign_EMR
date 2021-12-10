import pandas as pd
import numpy as np

def agent_assignment(df0, names):
    ## Remove names/notedate/note outside of specific list
    ## Agent name and note will refer to last note
    filter1 = df0['AgentName'].isin(names['Name'].unique())
    filter2 = df0['AgentName'] == 'NA'
    df0['AgentName']    = np.where(filter1, df0['AgentName'], 'NA')
    df0['NoteDate']     = np.where(filter2, 'NA', df0['NoteDate'])
    df0['Note']         = np.where(filter2, 'NA', df0['Note'])
    df0 = df0.drop(['CF Username'], axis=1)
    ### New name column will refer to assigment
    name = names[['Name', 'Agent ID','CF Username','Market']].drop_duplicates().reset_index(drop=True)
    df = pd.merge(df0, name, on=['Market'], how='left')
    return df