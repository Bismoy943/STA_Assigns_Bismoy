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
        #self.datetime=[bt.utils.date.num2date(date) for date in self.datas[0].datetime]
        self.dataclose=self.datas[0].close
        self.volume=self.datas[0].volume
        self.dataopen=self.datas[0].open
        self.datahigh=self.datas[0].high
        self.datalow=self.datas[0].low
        self.turnover=self.datas[0].volume*((self.datas[0].high+self.datas[0].low)/2)
        self.dayrange=self.datas[0].high-self.datas[0].low
        self.gap=self.datas[0].open-self.datas[0].close(-1)
        self.voldiff=bt.If(self.volume-self.volume(-1)>0,1,-1)
        self.atr=bt.indicators.ATR(self.datas[0],period=14)
        self.df=pd.DataFrame(columns=['YYYYMMDD','OPEN','HIGH','LOW','CLOSE','VOLUME','TURNOVER',
                                 'DAYRANGE','GAP','ATR','VOLUME_CHANGE'])

    '''
    def _loadline(self, linetokens):
        i = itertools.count(0)

        dttxt = linetokens[next(i)]
        # Format is YYYY-MM-DD
        y = int(dttxt[0:4])
        m = int(dttxt[5:7])
        d = int(dttxt[8:10])

        dtt = dt.datetime(y, m, d)
        dtnum = bt.date2num(dtt)

        self.lines.datetime[0] = dtnum
    '''


    def next(self):
        self.log("Datetime:"+str(self.datetime.date()))
        self.log("Open:"+str(self.dataopen[0]))
        self.log("High:"+str(self.datahigh[0]))
        self.log("Low:"+str(self.datalow[0]))
        self.log("Close:"+str(self.dataclose[0]))
        self.log("Volume:"+str(self.volume[0]))
        self.log("Turnover:"+str(self.dayrange[0]))
        self.log("Gap:"+str(self.gap[0]))
        self.log("Volume Difference:"+str(self.voldiff[0]))
        self.log("ATR:"+str(self.atr[0]))
        self.df=self.df.append({'YYYYMMDD':self.datetime.date(),'OPEN':self.dataopen[0],
                                'HIGH':self.datahigh[0],'LOW':self.datalow[0],
                                'CLOSE':self.dataclose[0],'VOLUME':self.volume[0],
                                'TURNOVER':self.turnover[0],'DAYRANGE':self.dayrange[0],
                                'GAP':self.gap[0],'ATR':self.atr[0],
                                'VOLUME_CHANGE':self.voldiff[0]
                                },ignore_index=True)



        if not self.position:
            pass

    def stop(self):
        self.df=self.df.set_index('YYYYMMDD')
        print(self.df.head())
        fig,ax=plt.subplots()
        ax.plot(self.df.index,self.df['CLOSE'])
        ax.plot(self.df.index,ta.SMA(self.df['CLOSE'],timeperiod=20))
        plt.show()


if __name__=='__main__':
    cerebro=bt.Cerebro()
    cerebro.addstrategy(FirstStrategy)
    datapath="D:/Trading/Backtesting_Course/testdata/NiftyFutures-1D.csv"
    data=bt.feeds.GenericCSVData(
        dataname=datapath,
        fromdate=dt.datetime(2011,1,1),
        todate=dt.datetime(2021,1,1),
        datetime=0,
        timeframe=bt.TimeFrame.Days,
        compression=1,
        dtformat=('%Y-%m-%d'),
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
    cerebro.addsizer(bt.sizers.FixedSize,stake=75)
    cerebro.broker.setcommission(commission=0.001)
    cerebro.broker.set_cash(1000000.00)
    print("Starting portfolio value:"+str(cerebro.broker.getvalue()))
    cerebro.run()
    print("Final portfolio value:"+str(cerebro.broker.getvalue()))
    




