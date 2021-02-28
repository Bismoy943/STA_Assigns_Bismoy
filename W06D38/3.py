from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import pandas as pd
import numpy as np
import talib as ta
import matplotlib.pyplot as plt
import csv
import backtrader as bt
import  datetime as dt
from datetime import date,time,timedelta

class FirstStrategy(bt.Strategy):

    params=(
        ('exitbars',5),
        ('malookback',20),
    )

    def log(self,txt):
        print(txt)

    def __init__(self):
        self.dataclose=self.datas[0].close
        self.sma=bt.indicators.SMA(self.datas[0],period=self.params.malookback)


    def notify_order(self, order):
        if order.status in [order.Submitted,order.Accepted]:
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                pass
            if order.issell():
                pass
            self.bar_executed=len(self)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            pass


    def next(self):
        if not self.position:
            if self.dataclose[0]>self.dataclose[-1]:
                if self.dataclose[-1]>self.dataclose[-2]:
                    if self.dataclose[0]>self.sma[0]:
                        self.order=self.buy()
                        self.candletracker=0
        else:
            self.candletracker+=1
            #print(self.params.exitbars)
            if self.candletracker>(self.params.exitbars-1):
                self.close()
                self.candletracker=0


    def stop(self):
        self.log("No of bars to exit:"+str(self.params.exitbars))
        self.log("Final portfolio value:"+str(self.broker.getvalue()))





if __name__=='__main__':
    cerebro=bt.Cerebro()
    #cerebro.addstrategy(FirstStrategy,exitbars=5)
    cerebro.optstrategy(FirstStrategy,exitbars=range(5  ,50),malookback=[5,10,15,20,50,100])
    datapath="D:/Trading/Backtesting_Course/testdata/NIFTY_F1_5min.csv"
    data=bt.feeds.GenericCSVData(
        dataname=datapath,
        fromdate=dt.datetime(2018, 1, 1),
        todate=dt.datetime(2018, 1, 5),
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
    # cerebro.adddata(data2)
    cerebro.addsizer(bt.sizers.FixedSize, stake=75)
    cerebro.broker.setcommission(commission=0.001)
    cerebro.broker.set_cash(1000000.00)
    print("Starting portfolio value:" + str(cerebro.broker.getvalue()))
    cerebro.run()
    #print("Final portfolio value:" + str(cerebro.broker.getvalue()))