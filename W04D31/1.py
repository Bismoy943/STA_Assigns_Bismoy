import pandas as pd
import numpy as np
import csv
import datetime as dt
from datetime import date,time,timedelta

csvFrame=pd.read_csv("D:/Trading/Backtesting_Course/testdata/NIFTY_F1.csv",parse_dates=[['YYYYMMDD','TIME']])
print(csvFrame.head())
csvFrame=csvFrame.drop('TICKER',axis=1)
print(csvFrame.head())
csvFrame.YYYYMMDD_TIME=csvFrame.YYYYMMDD_TIME-timedelta(hours=0,minutes=1)
print(csvFrame.head())
csvFrame=csvFrame.set_index('YYYYMMDD_TIME')
csvFrame=csvFrame.between_time(start_time='09:15:00',end_time='15:30:00',include_end=False)
#1
csvFrame15=csvFrame.resample('15T').agg({'OPEN':'first','HIGH':'max','LOW':'min','CLOSE':'last'})
print(csvFrame15.head())

#4
print(csvFrame15['CLOSE']-csvFrame15['OPEN'])
#5
yyyymmdd=[]
openlist=[]
highlist=[]
lowlist=[]
closelist=[]
volumelist=[]

with open("D:/Trading/Backtesting_Course/testdata/NiftyFutures-1D.csv","r") as nifty_future:
    firstline = True
    for line in nifty_future:
        print(line)
        if firstline:
            firstline = False
            continue
        x = line.rstrip("\n").split(",")
        for i in range(len(x)):
            if i == 0:
                yyyymmdd.append(dt.datetime.strptime(x[i], '%Y-%m-%d'))
            elif i == 1:
                openlist.append(float(x[i]))
            elif i == 2:
                highlist.append(float(x[i]))
            elif i == 3:
                lowlist.append(float(x[i]))
            elif i == 4:
                closelist.append(float(x[i]))
            elif i==5:
                volumelist.append(int(x[i]))
nifty_future.close()

list_of_tuples=list(zip(yyyymmdd,openlist,highlist,lowlist,closelist,volumelist))
df=pd.DataFrame(list_of_tuples,columns=['DATE','OPEN','HIGH','LOW','CLOSE','VOLUME'])
turnover=[]

for i in df.index:
    turnover.append(((df['HIGH'][i]+df['LOW'][i])/2)*df['VOLUME'][i])

updated_tuple=list(zip(yyyymmdd,openlist,highlist,lowlist,closelist,volumelist,turnover))
dfnew=pd.DataFrame(updated_tuple,columns=['DATE','OPEN','HIGH','LOW','CLOSE','VOLUME','TURNOVER'])
print(dfnew)


