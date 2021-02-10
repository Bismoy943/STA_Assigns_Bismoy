from __future__ import (absolute_import,division,print_function,unicode_literals)
import backtrader as bt
import datetime as dt
if __name__=='__main__':
    cerebro=bt.Cerebro()
    datapath="D:/Trading/Backtesting_Course/testdata/banknifty2018.csv"
    data=bt.feeds.GenericCSVData(
        dataname=datapath,
        fromdate=dt.datetime(2018,1,1),
        todate=dt.datetime(2018,12,31),
        datetime=0,
        timeframe=bt.TimeFrame.Days,
        compression=1,
        dtformat=('%Y-%m-%d'),
        open=1,
        high=2,
        low=3,
        close=4,
        volume=5,
        openinterest=6,
        reverse=False,
        header=0
    )
    print('Starting portfolio value: ',cerebro.broker.getvalue())
    cerebro.run()
    print('Final portfolio value:',cerebro.broker.getvalue())
