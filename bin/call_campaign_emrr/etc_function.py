from datetime import date, timedelta, datetime
from pathlib import Path
import pandas as pd
import holidays
import time

### Input/output static tables ###
def table_drops(push_pull,dir,table,name):
    table_path = Path("data/{}".format(dir))
    if push_pull == 'push':
        table.to_csv(table_path / name, sep=',',index=False)
    else:
        return pd.read_csv(table_path / name, sep=',',low_memory=False)

def time_check(start, comment):
    executionTime_1 = round(time.time() - start, 2)
    print("-----------------------------------------------")
    print(comment + '\n' +'Time: ' + str(executionTime_1))
    print("-----------------------------------------------")

def daily_piv(df):
    df = df[df['Unique_Phone'] == 'Parent'] #, 'State'
    u = df.pivot_table(index =['Name'], values =['OutreachID','togo_agg'], aggfunc = {'OutreachID':'count','togo_agg':'sum'}, margins=True,margins_name= 'TOTAL')
    u['Chart/ID'] = u['togo_agg'] / u['OutreachID']
    return u.sort_values(by='OutreachID', ascending=False)

### CIOX Business Calender
today = date.today()
HOLIDAYS_US = holidays.US(years= today.year)
HOLIDAYS_CIOX = dict(zip(HOLIDAYS_US.values(), HOLIDAYS_US.keys()))
del_list = ("Washington\'s Birthday", 'Juneteenth National Independence Day','Columbus Day','Veterans Day')
for i in del_list:
    HOLIDAYS_CIOX.pop(i)

ONE_DAY = timedelta(days=1)

def next_business_day(start):
    next_day = start + ONE_DAY
    while next_day.weekday() in holidays.WEEKEND or next_day in HOLIDAYS_CIOX:
        next_day += ONE_DAY
    return next_day

def last_business_day(start):
    next_day = start - ONE_DAY
    while next_day.weekday() in holidays.WEEKEND or next_day in HOLIDAYS_CIOX:
        next_day -= ONE_DAY
    return next_day

def x_Bus_Day_ago(N):
    B10 = []
    seen = set(B10)
    i = today

    while len(B10) < N:
        item = last_business_day(i)
        if item not in seen:
            seen.add(item)
            B10.append(item)
        i -= timedelta(days=1)
    return B10[-1]

def Next_N_BD(start, N):
    B10 = []
    seen = set(B10)
    i = 0

    while len(B10) < N:
        def test(day):
            d = start + timedelta(days=day)
            return next_business_day(d)
        item = test(i).strftime("%m/%d/%Y")
        if item not in seen:
            seen.add(item)
            B10.append(item)
        i += 1
    return B10