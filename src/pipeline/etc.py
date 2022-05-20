from datetime import date, timedelta, datetime
from pathlib import Path
import pandas as pd
import time
from datetime import date, timedelta, datetime
from pandas.tseries.holiday import AbstractHolidayCalendar, Holiday, nearest_workday, \
    USMartinLutherKingJr, USPresidentsDay, USMemorialDay, USLaborDay, USThanksgivingDay
from dataclasses import dataclass
import time
startTime_1 = time.time()

### Input/output static tables ###
def table_drops(push_pull,dir,table,name):
    table_path = Path(f"data/{dir}")
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
    try:
        u = df.pivot_table(index =['Skill'], values =['OutreachID','ToGoCharts'], aggfunc = {'OutreachID':'count','ToGoCharts':'sum'}, margins=True,margins_name= 'TOTAL')
        u['Chart/ID'] = u['ToGoCharts'] / u['OutreachID']
    except:
        u = print('pivot broke')
    return u

### CIOX Business Calender

class CioxHoliday(AbstractHolidayCalendar):
    rules = [
        Holiday('NewYearsDay', month=1, day=1, observance=nearest_workday),
        USMartinLutherKingJr,
        USPresidentsDay,
        USMemorialDay,
        Holiday('USIndependenceDay', month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=nearest_workday)
    ]

ciox_holidays = CioxHoliday()
today = date.today()
ONE_DAY = timedelta(days=1)

def next_business_day(start):
    next_day = start + ONE_DAY
    holidays = ciox_holidays.holidays(today, today + timedelta(days=1 * 365)).values
    while next_day.weekday() >= 5 or next_day in holidays:
        next_day += ONE_DAY
    return next_day

def last_business_day(start):
    next_day = start - ONE_DAY
    holidays = ciox_holidays.holidays(today, today + timedelta(days=1 * 365)).values
    while next_day.weekday() >= 5 or next_day in holidays:
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
        item = test(i).strftime("%Y-%m-%d")
        if item not in seen:
            seen.add(item)
            B10.append(item)
        i += 1
    return B10

@dataclass
class Business_Days:
    date_format     : str       = '%Y-%m-%d'
    yesterday       : datetime  = x_Bus_Day_ago(1)
    yesterday_str   : str       = x_Bus_Day_ago(1).strftime(date_format)
    today           : datetime  = date.today()
    today_str       : str       = date.today().strftime(date_format)
    now             : datetime  = time.time()
    tomorrow        : datetime  = next_business_day(today)
    tomorrow_str    : str       = next_business_day(today).strftime(date_format)

