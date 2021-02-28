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
        ('exitbars',12),
    )
    def log(self,txt):
        print(txt)
    def __init__(self):
        self.datetime = self.datas[0].datetime
        self.dataclose = self.datas[0].close
        self.dataopen = self.datas[0].open
        self.datahigh = self.datas[0].high
        self.datalow = self.datas[0].low
        self.ema13=bt.indicators.ExponentialMovingAverage(self.datas[0],period=13)
        self.ema34 = bt.indicators.ExponentialMovingAverage(self.datas[0], period=34)
        self.buy1=-1
        self.sell1=-1
        self.candlebuy=0
        self.candlesell=0


    '''
    def notify_order(self, order):
        if order.status in [order.Submitted,order.Accepted]:
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log("Buy Executed:"+str(order.executed.price))
            elif order.issell():
                self.log("Sell Executed:"+str(order.executed.price))
            self.bar_executed=len(self)
        elif order.status in [order.Canceled,order.Margin,order.Rejected]:
            self.log('Order cancelled/Margin not sufficient/Rejected')

    '''
    def next(self):
        self.combined1 = dt.datetime.combine(self.datetime.date(), dt.time(9, 15))
        self.combined2 = dt.datetime.combine(self.datetime.date(), dt.time(15, 00))
        self.combined3 = dt.datetime.combine(self.datetime.date(), dt.time(15, 10))
        self.log("CLOSE:"+str(self.dataclose[0]))
        self.log("OPEN:"+str(self.dataopen[0]))
        self.log("LOW:"+str(self.datalow[0]))
        self.log("HIGH:"+str(self.datahigh[0]))
        self.log("TIME:"+str(self.datetime.datetime()))
        self.log("EMA13:"+str(self.ema13[0]))
        self.log("EMA34:"+str(self.ema34[0]))

        if not self.position:
            if self.datetime.datetime() <self.combined2:
                if (self.datahigh[-1]<self.ema13[0] or self.datahigh[-1] <self.ema34[0]) and (self.dataclose[0]>self.ema13[0] and self.dataclose[0]>self.ema34[0]):
                    self.log(str(self.datetime.datetime())+" BUY TRIGGERED "+str(self.dataclose[0]))
                    self.stoplossbuy=self.datalow[0]
                    self.order=self.buy()
                    self.log("STOPLOSS FOR BUY IS:"+str(self.stoplossbuy))
                    self.buy1=0
                    self.candlebuy=0
                if (self.datalow[-1]>self.ema13[0] or self.datalow[-1] >self.ema34[0]) and (self.dataclose[0]<self.ema13[0] and self.dataclose[0]<self.ema34[0]):
                    self.log(str(self.datetime.datetime())+" SELL TRIGGERED "+str(self.dataclose[0]))
                    self.stoplosssell=self.datahigh[0]
                    self.order=self.sell()
                    self.log("STOPLOSS FOR SELL IS:"+str(self.stoplosssell))
                    self.sell1=0
                    self.candlesell=0
        else:
            if self.buy1==0:
                self.candlebuy+=1
                if self.candlebuy>=self.params.exitbars or self.dataclose[0]<=self.stoplossbuy or self.dataopen[0]<=self.stoplossbuy or self.datalow[0]<=self.stoplossbuy or self.datetime.datetime==self.combined3:
                    self.log(str(self.datetime.datetime())+"BUY EXIT"+str(self.dataclose[0]))
                    self.close()
                    self.log("Updated balance:"+str(cerebro.broker.getvalue()))
            if self.sell1==0:
                self.candlesell+=1
                if self.candlebuy>=self.params.exitbars or self.dataclose[0]>=self.stoplosssell or self.dataopen[0]>=self.stoplosssell or self.datahigh[0]>=self.stoplosssell or self.datetime.datetime==self.combined3:
                    self.log(str(self.datetime.datetime())+"SELL EXIT"+str(self.dataclose[0]))
                    self.close()
                    self.log("Updated balance:"+str(cerebro.broker.getvalue()))







    def stop(self):
        self.log("Finished with the processing")



if __name__=='__main__':
    cerebro=bt.Cerebro()
    cerebro.addstrategy(FirstStrategy,exitbars=5)
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
    #cerebro.broker.setcommission(commission=0.001)
    cerebro.broker.set_cash(1000000.00)
    print("Starting portfolio value:" + str(cerebro.broker.getvalue()))
    cerebro.run()
    print("Final portfolio value:" + str(cerebro.broker.getvalue()))
    cerebro.plot(volume=False)







