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
    )
    def log(self,txt):
        print(txt)
    def __init__(self):
        self.datetime = self.datas[0].datetime
        self.dataclose = self.datas[0].close
        self.dataopen = self.datas[0].open
        self.datahigh = self.datas[0].high
        self.datalow = self.datas[0].low
        self.maxhighmidway = 9999999999
        self.minlowmidway = -9999999999

        self.buy2=-1
        self.sell2=-1

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
        self.combined2 = dt.datetime.combine(self.datetime.date(), dt.time(12, 15))
        self.combined3 = dt.datetime.combine(self.datetime.date(), dt.time(15, 10))
        if self.datetime.datetime() == self.combined1:
            self.log("We are in the opening tick")
            self.maxhighopening = self.datahigh[0]
            self.minlowopening = self.datalow[0]
            self.log("Opening tick high recorded as:"+str(self.maxhighopening))
            self.log("Opening tick low recorded as:"+str(self.minlowopening))
            self.buy1=0
            self.sell1=0
        if self.datetime.datetime() == self.combined2:
            self.log("We are in the midway tick")
            self.maxhighmidway = self.datahigh[0]
            self.minlowmidway = self.datalow[0]
            self.log("Midway tick high recorded as:"+str(self.maxhighmidway))
            self.log("Midway tick low recorded as:"+str(self.minlowmidway))
            self.buy2=0
            self.sell2=0
        if not self.position:

            if (self.dataclose[0]>self.maxhighopening) and (self.buy1==0):
               self.log(str(self.datetime.datetime())+"OPENING BUY TRIGGERED:"+str(self.dataclose[0]))
               self.order=self.buy()
               self.log("STOPLOSS FOR BUY IS :"+str(self.minlowopening))
               self.buy1=self.buy1+1
               self.rangebuy1=self.dataclose[0]-self.minlowopening

            if (self.dataclose[0]>self.maxhighmidway) and (self.buy2==0):
                self.log(str(self.datetime.datetime()) + "MIDWAY BUY TRIGGERED:" + str(self.dataclose[0]))
                self.order = self.buy()
                self.log("STOPLOSS FOR BUY IS :" + str(self.minlowmidway))
                self.buy2 += 1
                self.rangebuy2 = self.dataclose[0] - self.minlowmidway
            if (self.dataclose[0] < self.minlowopening) and (self.sell1 == 0):
               self.log(str(self.datetime.datetime()) + "OPENING SELL TRIGGERED:" + str(self.dataclose[0]))
               self.order = self.sell()
               self.log("STOPLOSS FOR SELL IS :" + str(self.maxhighopening))
               self.sell1 = self.sell1 + 1
               self.rangesell1=self.maxhighopening-self.dataclose[0]

            if (self.dataclose[0] < self.minlowmidway) and (self.sell2 == 0):
               self.log(str(self.datetime.datetime()) + "MIDWAY SELL TRIGGERED:" + str(self.dataclose[0]))
               self.order = self.sell()
               self.log("STOPLOSS FOR SELL IS :" + str(self.maxhighmidway))
               self.sell2 += 1
               self.rangesell2=self.maxhighmidway-self.dataclose[0]
        else:
            if (self.buy1>0) and ((self.dataclose[0]-self.minlowopening)>(2*self.rangebuy1) or (self.datetime.datetime()==self.combined3) or (self.dataclose[0]<=self.minlowopening)):
                self.log(str(self.datetime.datetime())+"OPENING BUY EXIT:"+str(self.dataclose[0]))
                self.close()
                print("Updated balance:" + str(cerebro.broker.getvalue()))
                self.buy1=-1

            if (self.buy2>0) and ((self.dataclose[0]-self.minlowmidway)>(2*self.rangebuy2) or (self.datetime.datetime()==self.combined3) or (self.dataclose[0]<=self.minlowmidway)):
                self.log(str(self.datetime.datetime())+"MIDWAY BUY EXIT:"+str(self.dataclose[0]))
                self.close()
                print("Updated balance:" + str(cerebro.broker.getvalue()))
                self.buy2=-1

            if (self.sell1>0) and ((self.maxhighopening-self.dataclose[0])>(2*self.rangesell1) or (self.datetime.datetime()==self.combined3) or (self.dataclose[0]>=self.maxhighopening)):
                self.log(str(self.datetime.datetime()) + "OPENING SELL EXIT:" + str(self.dataclose[0]))
                self.close()
                print("Updated balance:" + str(cerebro.broker.getvalue()))
                self.sell1=-1

            if (self.sell2>0) and ((self.maxhighmidway-self.dataclose[0])>(2*self.rangesell2) or (self.datetime.datetime()==self.combined3) or (self.dataclose[0]>=self.maxhighmidway)):
                self.log(str(self.datetime.datetime()) + "MIDWAY SELL EXIT:" + str(self.dataclose[0]))
                self.close()
                print("Updated balance:" + str(cerebro.broker.getvalue()))
                self.sell2=-1

            if (self.dataclose[0]>self.maxhighopening) and (self.buy1==0):
               self.log(str(self.datetime.datetime())+"OPENING BUY TRIGGERED:"+str(self.dataclose[0]))
               self.order=self.buy()
               self.log("STOPLOSS FOR BUY IS :"+str(self.minlowopening))
               self.buy1=self.buy1+1
               self.rangebuy1=self.dataclose[0]-self.minlowopening

            if (self.dataclose[0]>self.maxhighmidway) and (self.buy2==0):
                self.log(str(self.datetime.datetime()) + "MIDWAY BUY TRIGGERED:" + str(self.dataclose[0]))
                self.order = self.buy()
                self.log("STOPLOSS FOR BUY IS :" + str(self.minlowmidway))
                self.buy2 += 1
                self.rangebuy2 = self.dataclose[0] - self.minlowmidway
            if (self.dataclose[0] < self.minlowopening) and (self.sell1 == 0):
               self.log(str(self.datetime.datetime()) + "OPENING SELL TRIGGERED:" + str(self.dataclose[0]))
               self.order = self.sell()
               self.log("STOPLOSS FOR SELL IS :" + str(self.maxhighopening))
               self.sell1 = self.sell1 + 1
               self.rangesell1=self.maxhighopening-self.dataclose[0]

            if (self.dataclose[0] < self.minlowmidway) and (self.sell2 == 0):
               self.log(str(self.datetime.datetime()) + "MIDWAY SELL TRIGGERED:" + str(self.dataclose[0]))
               self.order = self.sell()
               self.log("STOPLOSS FOR SELL IS :" + str(self.maxhighmidway))
               self.sell2 += 1
               self.rangesell2=self.maxhighmidway-self.dataclose[0]
    def stop(self):
        self.log("Finished with the processing")



if __name__=='__main__':
    cerebro=bt.Cerebro()
    cerebro.addstrategy(FirstStrategy,exitbars=5)
    datapath="D:/Trading/Backtesting_Course/testdata/NIFTY_F1_5min.csv"
    data=bt.feeds.GenericCSVData(
        dataname=datapath,
        fromdate=dt.datetime(2018, 1, 1),
        todate=dt.datetime(2019, 1, 1),
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







