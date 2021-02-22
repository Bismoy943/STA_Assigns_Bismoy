import numpy as np
import pandas as pd
import backtrader as bt
import datetime as dt
from datetime import time,date,timedelta

class FirstStrategy(bt.Strategy):

    params=(
        ('exitbars',5),
    )

    def log(self,txt):
        print(txt)
    def __init__(self):
        self.dataclose1m=self.datas[1].close
        self.date_time1m=self.datas[1].datetime
        self.datahigh15m=self.datas[0].high
        self.datalow15m=self.datas[0].low
        self.date_time15m=self.datas[0].datetime


    def next(self):
        self.log("Close 1min:"+str(self.dataclose1m[0]))
        self.log("Date_time 1min"+str(self.date_time1m[0]))
        self.log("High 15min:"+str(self.datahigh15m[0]))
        self.log("Low 15min:"+str(self.datalow15m[0]))
        self.log("Date_time 15min:"+str(self.date_time15m[0]))


if __name__=='__main__':
    cerebro=bt.Cerebro()
    cerebro.addstrategy(FirstStrategy)
    datapath="D:/Trading/Backtesting_Course/testdata/banknifty2018_15min.csv"
    datapath2="D:/Trading/Backtesting_Course/testdata/banknifty2018_1min.csv"
    data=bt.feeds.GenericCSVData(
        dataname=datapath,
        fromdate=dt.datetime(2011, 1, 1),
        todate=dt.datetime(2021, 1, 1),
        datetime=0,
        timeframe=bt.TimeFrame.Minutes,
        compression=1,
        dtformat=('%Y-%m-%d %H:%M:%S'),
        open=1,
        high=2,
        low=3,
        close=4,
        volume=None,
        openinterest=None,
        reverse=False,
        header=0

    )
    data2=bt.feeds.GenericCSVData(
        dataname=datapath2,
        fromdate=dt.datetime(2011,1,1),
        todate=dt.datetime(2021,1,1),
        datetime=0,
        timeframe=bt.TimeFrame.Minutes,
        compression=1,
        dtformat=('%Y-%m-%d %H:%M:%S'),
        open=1,
        high=2,
        low=3,
        close=4,
        volume=None,
        openinterest=None,
        reverse=False,
        header=0
    )
    cerebro.adddata(data)
    cerebro.adddata(data2)
    cerebro.addsizer(bt.sizers.FixedSize,stake=25)
    cerebro.broker.setcommission(commission=0.001)
    cerebro.broker.set_cash(1000000.00)
    print("Starting portfolio value:"+str(cerebro.broker.getvalue()))
    cerebro.run()
    print("Final portfolio value:"+str(cerebro.broker.getvalue()))

