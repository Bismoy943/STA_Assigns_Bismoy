'''
import pandas as pd
import time
from influxdb import InfluxDBClient
import datetime as dt
from datetime import timedelta

csvFrame=pd.read_csv("D:/Trading/Backtesting_Course/testdata/BANKNIFTY_F1.csv",parse_dates=[['DATE','TIME']])
print(csvFrame.head())
csvFrame.DATE_TIME=csvFrame.DATE_TIME - timedelta(hours=0,minutes=1)
print(csvFrame.head())
csvFrame=csvFrame.set_index('DATE_TIME')
csvFrame=csvFrame.between_time(start_time='09:15:00',end_time='15:30:00',include_end=False)
print(csvFrame.head())
print(csvFrame.tail())
csvFrame.to_csv("D:/Trading/Backtesting_Course/testdata/BANKNIFTY_F1_final.csv",index=True)
'''