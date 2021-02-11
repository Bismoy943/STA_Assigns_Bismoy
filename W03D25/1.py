from __future__ import (absolute_import,division,print_function,unicode_literals)
import backtrader as bt
import datetime as dt

class FirstStrategy(bt.Strategy):

    def log(self,txt):
        print(txt)

    def __init__(self):
        self.dataclose=self.datas[0].close
        self.x = len(list(self.dataclose))

    def next(self):

        print("Serial No:",len(self))
        if len(self)==1:
            self.log("Close:T-2 is not available for {}st day".format(len(self)))
            self.log("Close:T-1 is not available for {}st day".format(len(self)))
            self.log("Close:T " + str(self.dataclose[0]))
            self.log("Close:T+1 " + str(self.dataclose[1]))
            self.log("Close:T+2 " + str(self.dataclose[2]))
        elif len(self)==2:
            self.log("Close:T-2 is not available for {}nd day".format(len(self)))
            self.log("Close:T-1 is not available for {}nd day".format(len(self)))
            self.log("Close:T " + str(self.dataclose[0]))
            self.log("Close:T+1 " + str(self.dataclose[1]))
            self.log("Close:T+2 " + str(self.dataclose[2]))
        elif len(self)==self.x-2:
            self.log("Close:T-2 " + str(self.dataclose[-2]))
            self.log("Close:T-1 " + str(self.dataclose[-1]))
            self.log("Close:T " + str(self.dataclose[0]))
            self.log("Close:T+1 "+str(self.dataclose[1]))
            self.log("Close:T+2 is not available for {}th day".format(len(self)))
        elif len(self)==self.x-1:
            self.log("Close:T-2 " + str(self.dataclose[-2]))
            self.log("Close:T-1 " + str(self.dataclose[-1]))
            self.log("Close:T " + str(self.dataclose[0]))
            self.log("Close:T+1 is not available for {}th day".format(len(self)))
            self.log("Close:T+2 is not available for {}th day".format(len(self)))

        else:
            self.log("Close:T-2 " + str(self.dataclose[-2]))
            self.log("Close:T-1 " + str(self.dataclose[-1]))
            self.log("Close:T " + str(self.dataclose[0]))
            self.log("Close:T+1 " + str(self.dataclose[1]))
            self.log("Close:T+2 " + str(self.dataclose[2]))

if __name__=='__main__':
    cerebro=bt.Cerebro()
    cerebro.addstrategy(FirstStrategy)
    datapath="D:/Trading/Backtesting_Course/testdata/Nifty-1D.csv"
    datapath2="D:/Trading/Backtesting_Course/testdata/NiftyFutures-1D.csv"
    data=bt.feeds.GenericCSVData(
        dataname=datapath,
        fromdate=dt.datetime(2011,1,1),
        todate=dt.datetime(2018,1,1),
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
    data2 = bt.feeds.GenericCSVData(
        dataname=datapath2,
        fromdate=dt.datetime(2011,1,1),
        todate=dt.datetime(2018,1,1),
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
    print('Starting portfolio value: ',cerebro.broker.getvalue())
    cerebro.run()
    print('Final portfolio value:',cerebro.broker.getvalue())
