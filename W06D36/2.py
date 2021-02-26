from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
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
        self.datetime=self.datas[0].datetime
        #self.datetime15m=self.datas[1].datetime
        self.dataclose=self.datas[0].close
        self.dataopen=self.datas[0].open
        self.datahigh=self.datas[0].high
        self.datalow=self.datas[0].low
        self.candlebuy=1
        self.candlesell=1
        self.highlist = []
        self.lowlist = []

    #Initialising other variables along with empty list
    def next(self):
        #print("1min datetime:",self.datetime1m.datetime())
        #print("15 min datetime",self.datetime15m.datetime())
        #print("Close1m:"+str(self.dataclose[0]))
        #print("Close15m:"+str(self.datas[1].close[0]))
        self.log("DATE_TIME:"+str(self.datetime.datetime()))
        self.log("Open:"+str(self.dataopen[0]))
        self.log("Close:"+str(self.dataclose[0]))
        #logging the entries for reference
        #self.log(self.datetime.date())
        #combined1=dt.datetime.combine(self.datetime.date(),dt.time(9,30))
        #combined2=dt.datetime.combine(self.datetime.date(),dt.time(9,45))
        #print(combined1,combined2)
        self.combined1 = dt.datetime.combine(self.datetime.date(), dt.time(9, 30))
        self.combined2 = dt.datetime.combine(self.datetime.date(), dt.time(9, 45))
        self.combined3 = dt.datetime.combine(self.datetime.date(), dt.time(15, 15))
        if self.datetime.datetime()>=self.combined3:
            self.highlist = []
            self.lowlist = []
        #when time>=15:15 making the list as empty
        if (self.datetime.datetime() >= self.combined1) and (self.datetime.datetime() < self.combined2):
            self.highlist.append(self.datahigh[0])
            self.lowlist.append(self.datalow[0])
            self.log("High:"+str(self.datahigh[0]))

            #within 9.30 to 9.45 appending the high and low list

        if not self.position:
            if (self.datetime.datetime()>=self.combined2) and (self.datetime.datetime()<=self.combined3):
                if len(self.highlist)==0:
                    self.log("No positions can be taken now")
                else:
#max high from high list
                    self.maxhigh=max(self.highlist)
                if len(self.lowlist)==0:
                    self.log("No positions can be taken now")
                else:
#min low from lowlist
                    self.minlow=min(self.lowlist)
                print("High list:",self.highlist)
                print("Low list:",self.lowlist)
                self.log("9:30 to 9:45 high:"+str(self.maxhigh))
                self.log("9:30 to 9:45 low:" + str(self.minlow))
                if self.dataclose[0]>self.maxhigh and (self.datetime.datetime()<self.combined3):
                    self.candlebuy = 0
                    self.stoploss=self.minlow
                    self.order=self.buy()
                    self.entry=self.dataclose[0]
                    self.range=self.dataclose[0]-self.minlow
                    self.log("BUY EXECUTED:"+str(self.dataclose[0]))
                    self.log("STOPLOSS:"+str(self.stoploss))
#buy condition if close>maxhigh and time less than 15:15
                if self.dataclose[0]<self.minlow and (self.datetime.datetime()<self.combined3):
                    self.candlesell = 0
                    self.stoploss=self.maxhigh
                    self.order=self.sell()
                    self.entry=self.dataclose[0]
                    self.range=self.maxhigh-self.dataclose[0]
                    self.log("SELL EXECUTED:"+str(self.dataclose[0]))
                    self.log("STOPLOSS:" + str(self.stoploss))
#vice versa sell condition
        else:
            #if already in a position
            if self.candlebuy==0:
                #in buy position different exit conditions
                if (self.dataclose[0]<=self.
                        minlow) or (self.dataopen[0]<=self.minlow) or (self.datahigh[0]<=self.minlow) or (self.datalow[0]<=self.minlow):
                    self.candlebuy+=1
                    self.close()
                    self.log("BUY EXITED:"+str(self.dataclose[0]))
                elif ((self.dataclose[0]-self.entry)>=(2*self.range)) or ((self.dataopen[0]-self.entry)>=(2*self.range)) or ((self.datahigh[0]-self.entry)>=(2*self.range)) or ((self.datalow[0]-self.entry)>=(2*self.range)) or ((self.dataclose[0]-self.entry)>=300) or ((self.dataopen[0]-self.entry)>=300) or ((self.datahigh[0]-self.entry)>=300) or ((self.datalow[0]-self.entry)>=300):
                    self.candlebuy+=1
                    self.close()
                    self.log("BUY EXITED:"+str(self.dataclose[0]))

                elif ((self.datetime.datetime())==(dt.datetime.combine(self.datetime.date(), dt.time(15, 14)))):
                    self.candlebuy+=1
                    self.close()
                    self.log("BUY EXITED:"+str(self.dataclose[0]))
                else:
                    self.log("BUY STILL ACTIVE")
            if self.candlesell==0:
                #in sell position different exit conditions
                if (self.dataclose[0]>=self.maxhigh) or (self.dataopen[0]>=self.maxhigh) or (self.datahigh[0]>=self.maxhigh) or (self.datalow[0]>=self.maxhigh):
                    self.candlesell+=1
                    self.close()
                    self.log("SELL EXITED:"+str(self.dataclose[0]))
                elif ((self.entry-self.dataclose[0])>=(2*self.range)) or ((self.entry-self.dataopen[0])>=(2*self.range)) or ((self.entry-self.datahigh[0])>=(2*self.range)) or ((self.entry-self.datalow[0])>=(2*self.range)) or ((self.entry-self.dataclose[0])>=300) or ((self.entry-self.dataopen[0])>=300) or ((self.entry-self.datahigh[0])>=300) or ((self.entry-self.datalow[0])>=300):
                    self.candlesell+=1
                    self.close()
                    self.log("SELL EXITED:"+str(self.dataclose[0]))
                elif ((self.datetime.datetime())==(dt.datetime.combine(self.datetime.date(), dt.time(15, 14)))):
                    self.candlesell+=1
                    self.close()
                    self.log("SELL EXITED:"+str(self.dataclose[0]))
                else:
                    self.log("SELL STILL ACTIVE")













    def stop(self):
        print("Finished")

if __name__ == '__main__':

        cerebro = bt.Cerebro()
        cerebro.addstrategy(FirstStrategy)
        datapath = "D:/Trading/Backtesting_Course/testdata/banknifty2018_1min.csv"
        #datapath2="D:/Trading/Backtesting_Course/testdata/banknifty2018_15min.csv"
        data = bt.feeds.GenericCSVData(
                dataname=datapath,
                fromdate=dt.datetime(2018,1,1),
                todate=dt.datetime(2019,1,1),
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
        '''
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
        '''
        cerebro.adddata(data)
        #cerebro.adddata(data2)
        cerebro.addsizer(bt.sizers.FixedSize, stake=25)
        cerebro.broker.setcommission(commission=0.001)
        cerebro.broker.set_cash(1000000.00)
        print("Starting portfolio value:" + str(cerebro.broker.getvalue()))
        cerebro.run()
        print("Final portfolio value:" + str(cerebro.broker.getvalue()))


