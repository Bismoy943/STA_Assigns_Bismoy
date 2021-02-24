import pandas as pd
import numpy as np
import backtrader as bt
import datetime as dt
from datetime import date,time,timedelta
import matplotlib.pyplot as plt
import csv
import talib as ta

class FirstStrategy(bt.Strategy):
    params=(
        ('exitbars',5),
            )

    def log(self,txt):
        print(txt)

    def __init__(self):
        self.datetime1m=self.datas[0].datetime
        self.datetime15m=self.datas[1].datetime
        self.dataclose=self.datas[0].close
        self.dataopen=self.datas[0].open
        self.datahigh=self.datas[0].high
        self.datalow=self.datas[0].low

    def next(self):
        print("1min datetime:",self.datetime1m.datetime())
        print("15 min datetime",self.datetime15m.datetime())
        print("Close1m:"+str(self.dataclose[0]))
        print("Close15m:"+str(self.datas[1].close[0]))

        if not self.position:
            pass



    def stop(self):
        print("Finished")

if __name__ == '__main__':

        cerebro = bt.Cerebro()
        cerebro.addstrategy(FirstStrategy)
        datapath = "D:/Trading/Backtesting_Course/testdata/banknifty2018_1min.csv"
        datapath2="D:/Trading/Backtesting_Course/testdata/banknifty2018_15min.csv"
        data = bt.feeds.GenericCSVData(
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
        data2 = bt.feeds.GenericCSVData(
                dataname=datapath2,
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
        cerebro.adddata(data)
        cerebro.adddata(data2)
        cerebro.addsizer(bt.sizers.FixedSize, stake=25)
        cerebro.broker.setcommission(commission=0.001)
        cerebro.broker.set_cash(1000000.00)
        print("Starting portfolio value:" + str(cerebro.broker.getvalue()))
        cerebro.run()
        print("Final portfolio value:" + str(cerebro.broker.getvalue()))


