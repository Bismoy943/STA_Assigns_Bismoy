import pandas as pd
import numpy as np
import csv
import talib as ta

csvFrame=pd.read_csv("D:/Trading/Backtesting_Course/testdata/NiftyFutures-1D.csv",parse_dates=['YYYYMMDD'])
newdf=csvFrame.describe()
#print(help(csvFrame.describe))
'''
print("Minimum in Open,high,low,close:\n",newdf.loc['min'])
print("25th percentile in Open,high,low,close:\n",newdf.loc['25%'])
print("Median in Open,high,low,close:\n",newdf.loc['50%'])
print("75th in Open,high,low,close:\n",newdf.loc['75%'])
print("Maximum in Open,high,low,close:\n",newdf.loc['max'])
'''
open=csvFrame['OPEN'].squeeze().to_numpy()
high=csvFrame['HIGH'].squeeze().to_numpy()
low=csvFrame['LOW'].squeeze().to_numpy()
close=csvFrame['CLOSE'].squeeze().to_numpy()
print("Minimum open={}\nMinimum high={}\nMinimum low={}\nMinimum close={}"
      .format(np.min(open),np.min(high),np.min(low),np.min(close)))
print("25th Percentile open={}\n25th Percentile high={}\n25th Percentile low={}\n25th Percentile close={}"
      .format(round(np.percentile(open,25),1),round(np.percentile(high,25),1),round(np.percentile(low,25),1),round(np.percentile(close,25),1)))

print("Median open={}\nMedian high={}\nMedian low={}\nMedian close={}"
      .format(round(np.percentile(open,50),1),round(np.percentile(high,50),1),round(np.percentile(low,50),1),round(np.percentile(close,50),1)))

print("75th Percentile open={}\n75th Percentile high={}\n75th Percentile low={}\n75th Percentile close={}"
      .format(round(np.percentile(open,75),1),round(np.percentile(high,75),1),round(np.percentile(low,75),1),round(np.percentile(close,75),1)))

print("Maximum open={}\nMaximum high={}\nMaximum low={}\nMaximum close={}"
      .format(np.max(open),np.max(high),np.max(low),np.max(close)))


rsi=ta.RSI(close,timeperiod=14)
print("RSI14 looks like:",rsi)
if rsi[len(rsi) - 1] < 20:
    print("Nifty is oversold as RSI is {}".format(rsi[len(rsi) - 1]))
else:
    print("Nifty is not oversold as RSI is {}".format(rsi[len(rsi) - 1]))








