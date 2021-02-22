import numpy as np
import pandas as pd
import talib as ta
import csv
import datetime as dt
from datetime import time,date,timedelta
import backtrader as bt

csvFrame=pd.read_csv("D:/Trading/Backtesting_Course/testdata/NIFTY_F1.csv",parse_dates=[['YYYYMMDD','TIME']])
#print(csvFrame.head())
csvFrame=csvFrame.drop('TICKER',axis=1)
#print(csvFrame.head())
csvFrame.YYYYMMDD_TIME=csvFrame.YYYYMMDD_TIME -timedelta(hours=0,minutes=1)
#print(csvFrame.head())
csvFrame=csvFrame.set_index('YYYYMMDD_TIME')
#print(csvFrame.tail())
csvFrame=csvFrame.resample('5T',axis=0).agg({'OPEN':'first','HIGH':'max','LOW':'min','CLOSE':'last'})
csvFrame=csvFrame.between_time(start_time='09:15:00',end_time='15:30:00',include_end=False)
csvFrame.dropna(inplace=True)
#print(csvFrame.tail())
csvFrame.to_csv("D:/Trading/Backtesting_Course/testdata/NIFTY_F1_5min.csv")