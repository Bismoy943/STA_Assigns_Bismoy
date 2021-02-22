import pandas as pd
import numpy as np
import csv
import backtrader as bt
import datetime as dt
from datetime import date,time,timedelta

csvFrame=pd.read_csv("D:/Trading/Backtesting_Course/testdata/banknifty2018.csv",parse_dates=[['DATE','TIME']])
#print(csvFrame.head())

csvFrame.DATE_TIME=csvFrame.DATE_TIME-timedelta(hours=0,minutes=1)
csvFrame=csvFrame.set_index('DATE_TIME')
#csvFrame=csvFrame.between_time(start_time='09:15:00',end_time='15:30:00',include_end=False)
#csvFrame.dropna(inplace=True)
#csvFrame.to_csv("D:/Trading/Backtesting_Course/testdata/banknifty2018_1min.csv")



#print(csvFrame.head())
csvFrame=csvFrame.resample('15T',axis=0).agg({'OPEN':'first','HIGH':'max','LOW':'min','CLOSE':'last'})
csvFrame=csvFrame.between_time(start_time='09:15:00',end_time='15:30:00',include_end=False)
csvFrame.dropna(inplace=True)
#print(csvFrame.head())
csvFrame.to_csv("D:/Trading/Backtesting_Course/testdata/banknifty2018_15min.csv")





