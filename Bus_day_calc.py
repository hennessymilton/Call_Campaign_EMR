from datetime import date, timedelta, datetime
import holidays
import pandas as pd
import numpy as np
import time
import os

today = date.today() #-timedelta(days=1)
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
# print(Next_N_BD(today, 10))

def time_check(start, comment):
    executionTime_1 = round(time.time() - start,2)
    print("-----------------------------------------------")
    print(comment + '\n' +'Time: ' + str(executionTime_1))
    print("-----------------------------------------------")

def daily_piv(df):
    df = df[df['Unique_Phone'] == 1]
    u = df.pivot_table(index =['Daily_Priority', 'Daily_Groups','NewID'], columns ='Skill', values ='PhoneNumber', aggfunc = ['count'], margins=True,margins_name= 'TOTAL')
    return print(u)
def map_piv(df):
    u = df.pivot_table(index =['Daily_Groups'], columns ='Skill', values ='PhoneNumber', aggfunc = ['count'])
    return print(u)

def newPath(Parent, Look):
    absolutepath = os.path.abspath(__file__)
    fileDirectory = os.path.dirname(absolutepath)
    parentDirectory = os.path.dirname(fileDirectory)
    newPath = os.path.join(parentDirectory, str(Parent) + '\\'+ str(Look) +'\\')
    return newPath

def date_list_split(ls, numSplit):
    splits = np.array_split(ls, numSplit)
    return splits

def create_dir(dir):
    absolutepath = os.path.abspath(__file__)
    fileDirectory = os.path.dirname(absolutepath)
    parentDirectory = os.path.dirname(fileDirectory)
    newPath = os.path.join(parentDirectory, dir)
    if not os.path.isdir(newPath):
        os.makedirs(newPath, mode = 0o666)
    else:
        print('dir already created')

def full_dir():
    create_dir("dump")
    def sub_dir(sub):
        subPath = newPath('dump',sub)
        if not os.path.isdir(subPath):
            os.makedirs(subPath, mode = 0o666)
        else:
            print('dir already created')
    sub_dir('Assignment_Map')
    sub_dir('Call_Campaign')
    sub_dir('Group_Rank')
    sub_dir('Project Tracking')
    create_dir("Table_Drop")

# full_dir()