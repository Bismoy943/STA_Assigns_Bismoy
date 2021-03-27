import pandas as pd
import time
from influxdb import InfluxDBClient
import datetime as dt
from datetime import timedelta

df=pd.read_csv("D:/Trading/Backtesting_Course/testdata/BANKNIFTY_F1.csv",parse_dates=[['DATE','TIME']])
db=InfluxDBClient("127.0.0.1",8086,"admin","admin")
db.create_database("bnfdata")
print(df.head())

df.DATE_TIME=df.DATE_TIME-timedelta(hours=0,minutes=1)
print(df.head())
df.DATE_TIME=(df.DATE_TIME-dt.datetime(1970,1,1)).dt.total_seconds()
df.DATE_TIME=df.DATE_TIME.astype('int64')
print(df.head())

lines=["FNO_DATA"
       +",TICKER="+str(df['TICKER'][d])
       +" "
       +"OPEN="+str(df['OPEN'][d])+","
       +"HIGH="+str(df['HIGH'][d])+","
       +"LOW="+str(df['LOW'][d])+","
       +"CLOSE="+str(df['CLOSE'][d])+","
       +"VOLUME="+str(df['VOLUME'][d])+","
       +"OI="+str(df['OI'][d])
       +" "+str(df['DATE_TIME'][d]) for d in range(len(df))]
#print(lines)
client_write_start_time=time.perf_counter()
db.write_points(lines,database="bnfdata",time_precision='s',batch_size=1000,protocol='line')
client_write_end_time=time.perf_counter()
print("Start Time:",client_write_start_time)
print("End Time:",client_write_end_time)
print("Total Time:",client_write_end_time-client_write_start_time)

#csvFrame.YYYYMMDD_TIME=csvFrame.YYYYMMDD_TIME-timedelta(hours=0,minutes=1)
#csvFrame=csvFrame.set_index('YYYYMMDD_TIME')
