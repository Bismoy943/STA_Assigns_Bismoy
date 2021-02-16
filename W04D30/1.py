import pandas as pd
import numpy as np
import csv
from datetime import datetime,date,time,timedelta

csvFrame=pd.read_csv("D:/Trading/Backtesting_Course/testdata/NIFTY_F1.csv",parse_dates=[['YYYYMMDD','TIME']])
#i
print(csvFrame.head())

csvFrame=csvFrame.drop('TICKER',axis=1)
print(csvFrame.head())
#ii
csvFrame.YYYYMMDD_TIME=csvFrame.YYYYMMDD_TIME - timedelta(hours=0,minutes=1)
print(csvFrame.head())
#iii
csvFrame=csvFrame.rename(columns={"YYYYMMDD_TIME":"DATE"})
print(csvFrame.head())

#iv

csvFrame.to_csv("D:/Trading/Backtesting_Course/testdata/NIFTY_F1_updated.csv",index=False)

