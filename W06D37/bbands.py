import pandas as pd
import datetime as dt
from datetime import date,time,timedelta
import matplotlib.pyplot as plt

csvFrame=pd.read_csv("D:/Trading/Backtesting_Course/testdata/NIFTY_F1.csv",parse_dates=[['YYYYMMDD','TIME']])
csvFrame=csvFrame.drop('TICKER',axis=1)
csvFrame.YYYYMMDD_TIME=csvFrame.YYYYMMDD_TIME-timedelta(hours=0,minutes=1)
csvFrame=csvFrame.set_index('YYYYMMDD_TIME')


#print(csvFrame.head()

'''
MB=20 day SMA
UB=20 day SMA+(20 day SD*2)
LB=20 day SMA -(20 day SD*2)
'''

csvFrame=csvFrame.resample('5T',axis=0).agg({'OPEN':'first','HIGH':'max','LOW':'min','CLOSE':'last'})
csvFrame=csvFrame.between_time(start_time='09:15:00',end_time='15:30:00',include_end=False)
csvFrame.dropna(inplace=True)
csvFrame['SMA1']=csvFrame['CLOSE'].rolling(window=3750).mean()
csvFrame['SMA2']=csvFrame['CLOSE'].rolling(window=15000).mean()
csvFrame['middleband']=csvFrame['CLOSE'].rolling(window=1500).mean()
csvFrame['upperband']=csvFrame['CLOSE'].rolling(window=1500).mean()+csvFrame['CLOSE'].rolling(window=1500).std()*2
csvFrame['lowerband']=csvFrame['CLOSE'].rolling(window=1500).mean()-csvFrame['CLOSE'].rolling(window=1500).std()*2
fig,ax=plt.subplots()
ax.plot(csvFrame.index,csvFrame['CLOSE'],label='CLOSE PRICE')
#ax.plot(csvFrame['SMA1'],'g--',label='50 DMA')
#ax.plot(csvFrame['SMA2'],'r--',label='200 DMA')
ax.plot(csvFrame['middleband'],'g--',label='middle')
ax.plot(csvFrame['upperband'],'y--',label='upper')
ax.plot(csvFrame['lowerband'],'y--',label='lower')
plt.legend()
plt.show()
