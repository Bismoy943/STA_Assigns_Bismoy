import pandas as pd
import numpy as np
import csv
import datetime as dt
from datetime import date,time,timedelta

yyyymmdd=[]
openlist=[]
highlist=[]
lowlist=[]
closelist=[]
volumelist=[]

with open("D:/Trading/Backtesting_Course/testdata/NiftyFutures-1D.csv","r") as nifty_future:
    firstline = True
    for line in nifty_future:
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
print(df)


for index,row in df.iterrows():
    print(row['DATE'],row['CLOSE'])
