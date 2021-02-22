import numpy as np
import pandas as pd
import csv
import datetime as dt
from datetime import date,time,timedelta
import backtrader as bt

class FirstStrategy(bt.Strategy):
    params=(
        ('exitbars',5),
    )
    def log(self,txt):
        print(txt)
    def __init__(self):
        self.dataclose=self.datas[0].close
        self.candletracker=0
        self.sma=bt.indicators.SMA(self.datas[0],period=20)
    def next(self):
        self.log("Close:"+str(self.dataclose[0]))
        if not self.position:
            if self.dataclose[0]>self.dataclose[-1]:
                if self.dataclose[-1]>self.dataclose[-2]:
                    if self.dataclose[0]>self.sma[0]:
                        self.order=self.buy()
                        self.log("BUY ORDER CREATED:"+str(self.dataclose[0]))
                        self.candletracker=0
        else:
            self.candletracker+=1
            print(self.params.exitbars)
            if self.candletracker>(self.params.exitbars-1):
                self.close()
                self.log("ORDER EXITED:"+str(self.dataclose[0]))
                self.candletracker=0





if __name__=='__main__':
    cerebro=bt.Cerebro()
    cerebro.addstrategy(FirstStrategy,exitbars=5)
    datapath="D:/Trading/Backtesting_Course/testdata/NIFTY_F1_5min.csv"
    data=bt.feeds.GenericCSVData(
        dataname=datapath,
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
    cerebro.addsizer(bt.sizers.FixedSize,stake=75)
    cerebro.broker.setcommission(commission=0.001)
    cerebro.broker.set_cash(1000000.00)
    print("Starting portfolio value:",cerebro.broker.getvalue())
    cerebro.run()
    print("Final portfolio value:",cerebro.broker.getvalue())


