import pandas as pd
import numpy as np
import backtrader as bt
import datetime as dt
from datetime import date, time, timedelta
import matplotlib.pyplot as plt
import csv
import talib as ta
import math


class FirstStrategy(bt.Strategy):
    params = (
        ('exitbars', 6),
    )

    def log(self, txt):
        print(txt)

    def __init__(self):
        self.datetime = self.datas[0].datetime
        # self.datetime=[bt.utils.date.num2date(date) for date in self.datas[0].datetime]
        self.dataclose = self.datas[0].close
        self.dataopen = self.datas[0].open
        self.datahigh = self.datas[0].high
        self.datalow = self.datas[0].low
        self.candlebuy=-1
        self.candlesell=-1


        csvFrame=pd.read_csv("D:/Trading/Backtesting_Course/testdata/Nifty-1D.csv")
        csvFrame['DAYRANGE']=csvFrame['HIGH']-csvFrame['LOW']
        self.averagerange=sum(csvFrame['DAYRANGE'])/len(csvFrame['DAYRANGE'])
        csvFrame['VARIANCE']=abs(csvFrame['DAYRANGE']-self.averagerange)
        csvFrame['SQUAREVARIANCE']=csvFrame['VARIANCE']*csvFrame['VARIANCE']
        self.st_dev=math.sqrt(sum(csvFrame['SQUAREVARIANCE'])/len(csvFrame['SQUAREVARIANCE']))




    def next(self):

        if not self.position:
            if self.dataclose[0]>(self.dataclose[-1]+(self.averagerange+2*self.st_dev)):
                self.log(str(self.datetime.date())+" :BUY TRIGGERED "+str(self.dataclose[0]))
                self.buy()
                self.candlebuy=0
            '''
            
            if self.dataclose[0]<(self.dataclose[-1]-(self.averagerange+2*self.st_dev)):
                self.log(str(self.datetime.date())+" :SELL TRIGGERED")
                self.sell()
                self.candlesell=0
                self.candlebuy=1
            '''
        else:
            self.candlebuy+=1




            if self.candlebuy>self.params.exitbars:
                self.close()
                self.log(str(self.datetime.date())+":BUY EXITED: "+str(self.dataclose[0]))
                self.candlebuy=0
            '''
            
            if self.candlesell>self.params.exitbars:
                self.close()
                self.log("SELL EXITED")
                self.candlesell=0
            '''





    def stop(self):
        #self.averagerange=sum(self.averagelist)/len(self.averagelist)
        #self.log("Average range per day="+str(self.averagerange))
        self.log("Run completed")



if __name__ == '__main__':
    cerebro = bt.Cerebro()
    cerebro.addstrategy(FirstStrategy)
    datapath = "D:/Trading/Backtesting_Course/testdata/Nifty-1D.csv"
    data = bt.feeds.GenericCSVData(
        dataname=datapath,
        fromdate=dt.datetime(2001, 1, 1),
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
    cerebro.addsizer(bt.sizers.FixedSize, stake=75)
    cerebro.broker.setcommission(commission=0.001)
    cerebro.broker.set_cash(1000000.00)
    print("Starting portfolio value:" + str(cerebro.broker.getvalue()))
    cerebro.run()
    print("Final portfolio value:" + str(cerebro.broker.getvalue()))





