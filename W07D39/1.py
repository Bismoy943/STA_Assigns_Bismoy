from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import backtrader as bt
import datetime as dt
import pandas as pd
import csv
import numpy as np
import talib as ta
import matplotlib.pyplot as plt
from datetime import datetime,time,date,timedelta


class FirstStrategy(bt.Strategy):

    params=(
        ('exitbars',12),
    )


    def log(self,txt):
        print(txt)


    def notify_order(self, order):
        if order.status in [order.Submitted,order.Accepted]:
            return
        if order.status in [order.Completed]:
            if order.isbuy():
                print("BUY TRIGGERED")
            if order.issell():
                print("SELL TRIGGERED")
            self.bar_executed=len(self)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            pass


    def __init__(self):
        self.sma50=bt.indicators.SMA(self.datas[0],period=50)
        self.sma20=bt.indicators.SMA(self.datas[0],period=20)
        self.datetime = self.datas[0].datetime
        self.dataclose = self.datas[0].close
        self.dataopen = self.datas[0].open
        self.datahigh = self.datas[0].high
        self.datalow = self.datas[0].low
        self.morningtick=datetime.now().replace(hour=9,minute=15,second=0,microsecond=0)
        self.eveningtick=datetime.now().replace(hour=14,minute=50,second=0,microsecond=0)
        self.closetime=datetime.now().replace(hour=15,minute=10,second=0,microsecond=0)
        self.buycounter=0
        self.sellcounter=0



    def next(self):
        self.current_date=self.datetime.date(0)
        self.current_time=self.datetime.time(0)
        self.current_datetime=self.datetime.datetime(0)
        if self.current_time==self.morningtick.time():
            self.buycounter=0
            self.sellcounter=0
        if not self.position:
            if self.current_time>=self.morningtick.time() and self.current_time<=self.eveningtick.time():
                if self.dataclose[0]>self.sma20[0] and self.dataclose[0]>self.sma50[0] and self.buycounter==0:
                    self.log("Entering BUY order:"+str(self.current_datetime)+str(self.dataclose[0]))
                    self.stopbuy=self.datalow[0]
                    self.order=self.buy()
                    self.candletracker=0
                    print("BUY CANDLETRACKER",self.candletracker)
                if self.dataclose[0]<self.sma20[0] and self.dataclose[0]<self.sma50[0] and self.sellcounter==0:
                    self.log("Entering SELL order:"+str(self.current_datetime)+str(self.dataclose[0]))
                    self.stopsell=self.datahigh[0]
                    self.order=self.sell()
                    self.candletracker=0
        else:
            self.candletracker+=1
            self.log("Candle position:"+str(self.candletracker))
            if self.current_time==self.closetime.time():
                self.log("Closing current position:"+str(self.current_datetime)+str(self.dataclose[0]))
                self.close()
                self.log("Updated balance:"+str(cerebro.broker.getvalue()))
            if self.order.isbuy():
                self.buycounter+=1
                print("BUY COUNTER:",self.buycounter)
                if (self.dataclose[0]<=self.stopbuy) or (self.datalow[0]<=self.stopbuy) or (self.dataopen[0]<=self.stopbuy) or (self.candletracker==self.params.exitbars):
                    self.log("Closing buy position:"+str(self.current_datetime)+str(self.dataclose[0]))
                    self.close()
                    self.log("Updated balance:" + str(cerebro.broker.getvalue()))
            if self.order.issell():
                self.sellcounter+=1
                print("SELL COUNTER:",self.sellcounter)
                if (self.dataclose[0]>=self.stopsell) or (self.datahigh[0]>=self.stopsell) or (self.dataopen[0]>=self.stopsell) or (self.candletracker==self.params.exitbars):
                    self.log("Closing sell position:"+str(self.current_datetime)+str(self.dataclose[0]))
                    self.close()
                    self.log("Updated balance:" + str(cerebro.broker.getvalue()))








if __name__=='__main__':
    cerebro=bt.Cerebro()
    cerebro.addstrategy(FirstStrategy)
    datapath="D:/Trading/Backtesting_Course/testdata/resampled_Niftyfutures-5m.csv"
    data=bt.feeds.GenericCSVData(
        dataname=datapath,
        fromdate=dt.datetime(2020, 12, 31,11,15,00),
        todate=dt.datetime(2021, 1, 6),
        datetime=0,
        timeframe=bt.TimeFrame.Minutes,
        compression=1,
        dtformat=('%Y-%m-%d %H:%M:%S'),
        open=1,
        high=2,
        low=3,
        close=4,
        volume=5,
        openinterest=None,
        reverse=False,
        header=0
    )
    cerebro.adddata(data)
    # cerebro.adddata(data2)
    cerebro.addsizer(bt.sizers.FixedSize, stake=75)
    #cerebro.broker.setcommission(commission=0.001)
    cerebro.broker.set_cash(2000000.00)
    print("Starting portfolio value:" + str(cerebro.broker.getvalue()))
    cerebro.run()
    print("Final portfolio value:" + str(cerebro.broker.getvalue()))

