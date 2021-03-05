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


class CommInfo(bt.CommInfoBase):
    params=(
        ('stocklike',False),
        ('commtype',bt.CommInfoBase.COMM_PERC),

    )
    def _getcommission(self, size, price, pseudoexec):
        return abs(size)*price*self.p.commission*self.p.mult


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
                print("BUY TRIGGERED "+str(self.datetime.datetime())+str(" ")+str(self.order.price))
                self.log("Updated balance:" + str(cerebro.broker.getvalue()))
            if order.issell():
                #print("BUY EXIT possible price 1 "+str(self.datetime.datetime())+str(self.sellorder.price))
                print("BUY EXIT possible price " + str(self.datetime.datetime()) +str(" ") + str(self.stoporder.price))
                self.log("Updated balance:" + str(cerebro.broker.getvalue()))
            self.bar_executed=len(self)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            pass


    def __init__(self):
        self.datetime = self.datas[0].datetime
        self.dataclose = self.datas[0].close
        self.dataopen = self.datas[0].open
        self.datahigh = self.datas[0].high
        self.datalow = self.datas[0].low
        self.morningtick=datetime.now().replace(hour=9,minute=15,second=0,microsecond=0)
        self.eveningtick=datetime.now().replace(hour=15,minute=00,second=0,microsecond=0)
        self.closetime=datetime.now().replace(hour=15,minute=14,second=0,microsecond=0)
        self.secondcandlestart=datetime.now().replace(hour=9,minute=30,second=0,microsecond=0)
        self.secondcandlestop=datetime.now().replace(hour=9,minute=44,second=0,microsecond=0)
        self.stoporder=None
        self.sellorder=None




    def next(self):
        self.current_date=self.datetime.date(0)
        self.current_time=self.datetime.time(0)
        self.current_datetime=self.datetime.datetime(0)

        if self.current_time==self.morningtick.time():
            self.highlist=[]
            self.lowlist=[]
            self.buy_counter=0
        if self.current_time>=self.secondcandlestart.time() and self.current_time<=self.secondcandlestop.time():
            self.highlist.append(self.datahigh[0])
            self.lowlist.append(self.datalow[0])
            self.maxhigh=max(self.highlist)
            self.minlow=min(self.lowlist)

            print("Max high",self.maxhigh)
            print("Min low",self.minlow)

        if not self.position:
            if self.current_time>self.secondcandlestop.time() and self.current_time<=self.eveningtick.time() and self.buy_counter==0:
                self.order=self.buy(exectype=bt.Order.StopLimit,price=self.maxhigh,plimit=self.maxhigh+2)
                self.log(self.datetime.datetime())
                self.log("BUY ENTERED:"+str(self.maxhigh))
                self.buy_counter+=1
                self.stoporder=self.sell(exectype=bt.Order.StopLimit,price=self.minlow,plimit=self.minlow-2)


        else:
            if self.dataclose[0]-self.maxhigh>=2*(self.maxhigh-self.minlow):
                self.cancel(self.stoporder)
                self.sellorder=self.close()
                self.log("BUY CLOSE:"+str(self.dataclose[0]))
                self.log("Updated balance:"+str(cerebro.broker.getvalue()))
            if self.current_time==self.closetime.time():
                self.cancel(self.stoporder)
                self.sellorder=self.close()
                self.log("BUY CLOSE:"+str(self.dataclose[0]))
                self.log("Updated balance:" + str(cerebro.broker.getvalue()))
    def stop(self):
        self.log("Done with processing")










if __name__=='__main__':
    cerebro=bt.Cerebro()
    commissions=CommInfo(
        commission=0.001,
        mult=25,
        margin=60000
    )
    cerebro.broker.addcommissioninfo(commissions)
    cerebro.addstrategy(FirstStrategy)
    datapath="D:/Trading/Backtesting_Course/testdata/banknifty2018_1min.csv"
    data=bt.feeds.GenericCSVData(
        dataname=datapath,
        fromdate=dt.datetime(2011, 1, 1),
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
    cerebro.addsizer(bt.sizers.FixedSize, stake=25)
    #cerebro.broker.setcommission(commission=0.001)
    cerebro.broker.set_cash(2000000.00)
    print("Starting portfolio value:" + str(cerebro.broker.getvalue()))
    cerebro.run()
    print("Final portfolio value:" + str(cerebro.broker.getvalue()))

