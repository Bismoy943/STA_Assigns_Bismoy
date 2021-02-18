import math
import statistics
import numpy as np
import pandas as pd
import scipy.stats
import backtrader as bt
import datetime as dt

data=[13589.5,13546,13566,13789.4]
print("Statistics example of fmean:",statistics.fmean(data))

class FirstStrategy(bt.Strategy):
    def log(self,txt):
        print(txt)
    def __init__(self):
        self.dataclose=self.datas[0].close
        self.candletracker=0
        self.x=len(list(self.dataclose))
    def next(self):
        self.log("Close:"+str(self.dataclose[0]))
        if len(self)==1:
            print("Previous data unavailabe for strategy")
        if len(self)==2:
            print("Previous data unavailable for strategy")
        if len(self)>=self.x-4:
            print("Strategy cannot be applied for these dates")
        if not self.position:
            if self.dataclose[0]>self.dataclose[-1]:
                if self.dataclose[-1]>self.dataclose[-2]:
                    self.order=self.buy()
                    self.log("BUY ORDER TRIGGERED:"+str(self.dataclose[0]))
                    self.candletracker=0
        else:
            self.candletracker+=1
            if self.candletracker>4:

                self.close()
                self.log("ORDER EXITED:"+str(self.dataclose[0]))
                self.candletracker=0

if __name__=='__main__':
    cerebro=bt.Cerebro()
    cerebro.addstrategy(FirstStrategy)
    datapath2 = "D:/Trading/Backtesting_Course/testdata/Nifty-1D.csv"
    datapath = "D:/Trading/Backtesting_Course/testdata/NiftyFutures-1D.csv"
    data=bt.feeds.GenericCSVData(
        dataname=datapath,
        fromdate=dt.datetime(2011,1,1),
        todate=dt.datetime(2021,1,1),
        datetime=0,
        timeframe=bt.TimeFrame.Days,
        compression=1,
        dtformat=('%Y-%m-%d'),
        open=1,
        high=2,
        low=3,
        close=4,
        volume=5,
        openinterest=None,
        reverse=False,
        header=0
    )
    data2 = bt.feeds.GenericCSVData(
        dataname=datapath2,
        fromdate=dt.datetime(2011, 1, 1),
        todate=dt.datetime(2021, 1, 1),
        datetime=0,
        timeframe=bt.TimeFrame.Days,
        compression=1,
        dtformat=('%Y-%m-%d'),
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
    cerebro.broker.set_cash(1000000.00)
    print("Starting Portfolio value:",cerebro.broker.getvalue())
    cerebro.run()
    print("Final Portfolio value:",cerebro.broker.getvalue())

