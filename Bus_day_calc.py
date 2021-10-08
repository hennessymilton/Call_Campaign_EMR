from datetime import date, timedelta, datetime
import holidays
import pandas as pd
import numpy as np
import time
import os

today = date.today()
FivDay = today + timedelta(days=7)
### Get Next Business day
ONE_DAY = timedelta(days=1)
HOLIDAYS_US = holidays.US()

def next_business_day(start):
    next_day = start + ONE_DAY
    while next_day.weekday() in holidays.WEEKEND or next_day in HOLIDAYS_US:
        next_day += ONE_DAY
    return next_day

def last_business_day(start):
    next_day = start - ONE_DAY
    while next_day.weekday() in holidays.WEEKEND or next_day in HOLIDAYS_US:
        next_day -= ONE_DAY
    return next_day

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
# print(Next_N_BD(today, 10))

def time_check(start, comment):
    executionTime_1 = round(time.time() - start,2)
    print('''
-----------------------------------------------
{}\nTime: {} Seconds
-----------------------------------------------'''.format(comment,executionTime_1))

def daily_piv(df):
    df = df[df['Unique_Phone'] == 'Parent']
    u = df.pivot_table(index =['Name'], values =['OutreachID','togo_agg'], aggfunc = {'OutreachID':'count','togo_agg':'sum'}, margins=True,margins_name= 'TOTAL')
    u['Chart/ID'] = u['togo_agg'] / u['OutreachID']
    return u.sort_values(by='OutreachID', ascending=False)

def newPath(Subdir, Subdir2):
    absolutepath = os.path.abspath(__file__)
    fileDirectory = os.path.dirname(absolutepath)
    newPath = os.path.join(fileDirectory +'\\'+ str(Subdir) +'\\'+ str(Subdir2) +'\\')
    return newPath