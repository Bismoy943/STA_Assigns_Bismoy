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
            self.log("Order Submitted/Accepted:"+str(self.datetime.datetime(0)))
            return
        if order.status in [order.Completed]:
            order_details=f"{order.executed.price},Cost:{order.executed.value},Comm {order.executed.comm}"
            if order.isbuy():

                if self.sellstop==True and self.closingbell==False:
                    self.log("Time:" + str(self.datetime.datetime(0)))
                    self.log(f"SELL STOP EXECUTED,Price: {order_details}")
                    self.log("Updated balance:" + str(self.broker.getvalue()))
                elif self.closingbell==True:
                    self.log("Time:"+str(self.datetime.datetime(0)))
                    self.log(f"SELL EXITED,Price: {order_details}")
                    self.log("Updated balance:" + str(self.broker.getvalue()))
                else:
                    self.log("Time:" + str(self.datetime.datetime(0)))
                    self.log(f"BUY ENTERED,Price: {order_details}")



                #if self.buystoporder is None:
                    #self.buystoporder = self.sell(exectype=bt.Order.StopLimit, price=self.ORL, plimit=self.ORL - 1)


                '''
                print(str(self.datetime.datetime(0))+" BUY TRIGGERED "+str(self.datetime.datetime())+str(" ")+str(order.executed.price))
                self.log("Max high for {} is {}".format(self.current_date,self.ORH))
                self.log("Max low for {} is {}".format(self.current_date, self.ORL))
                self.log("Updated balance:" + str(cerebro.broker.getvalue()))
                self.buystoporder=self.sell(exectype=bt.Order.StopLimit, price=self.ORL, plimit=self.ORL - 1)
                '''

                '''
                if self.sellstoporder is None:
                    self.buystoporder=self.sell(exectype=bt.Order.StopLimit, price=self.ORL, plimit=self.ORL - 1)
                if self.sellstoporder is not None:
                    self.buyorder=self.buy(exectype=bt.Order.Market,valid=0)
                '''

            elif order.issell():
                if self.closingbell==True:
                    self.log("Time:"+str(self.datetime.datetime(0)))
                    self.log(f"BUY EXITED,Price: {order_details}")
                    self.log("Updated balance:" + str(self.broker.getvalue()))
                else:
                    self.log("Time:" + str(self.datetime.datetime(0)))
                    self.log(f"BUY STOP EXECUTED/SELL EXECUTED,Price: {order_details}")
                    self.log("Updated balance:" + str(self.broker.getvalue()))




                '''
                #print("BUY EXIT possible price 1 "+str(self.datetime.datetime())+str(self.sellorder.price))
                print(str(self.datetime.datetime(0))+" BUY EXIT " + str(self.datetime.datetime()) +str(" ") + str(order.executed.price))
                self.log("Max high for {} is {}".format(self.current_date, self.ORH))
                self.log("Max low for {} is {}".format(self.current_date, self.ORL))
                self.log("Updated balance:" + str(cerebro.broker.getvalue()))
                self.sellstoporder=self.buy(exectype=bt.Order.StopLimit,price=self.ORH,plimit=self.ORH+1)
                '''
                '''
                if self.buystoporder is None:

                    self.sellstoporder=self.buy(exectype=bt.Order.StopLimit, price=self.ORH, plimit=self.ORH + 1)
                if self.buystoporder is not None:
                    self.sellorder=self.sell(exectype=bt.Order.Market,valid=0)
                '''


            self.bar_executed=len(self)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log("Order Cancelled/Margin/Rejected "+str(self.datetime.datetime()))
            pass






    def __init__(self):
        self.datetime = self.datas[0].datetime
        self.datetime5m = self.datas[1].datetime
        self.datetime15m = self.datas[2].datetime
        self.dataclose = self.datas[0].close
        self.dataclose5m = self.datas[1].close
        self.dataclose15m = self.datas[2].close
        self.dataopen = self.datas[0].open
        self.dataopen5m = self.datas[1].open
        self.dataopen15m = self.datas[2].open
        self.datahigh = self.datas[0].high
        self.datahigh5m = self.datas[1].high
        self.datahigh15m = self.datas[2].high
        self.datalow = self.datas[0].low
        self.datalow5m = self.datas[1].low
        self.datalow15m = self.datas[2].low
        self.morning_start=datetime.now().replace(hour=9,minute=15,second=0,microsecond=0)
        self.evening_end=datetime.now().replace(hour=14,minute=55,second=0,microsecond=0)
        self.time_stop=datetime.now().replace(hour=15,minute=00,second=0,microsecond=0)
        #self.secondcandle_start=datetime.now().replace(hour=9,minute=30,second=0,microsecond=0)
        self.rangecandle_stop=datetime.now().replace(hour=10,minute=45,second=0,microsecond=0)
        self.current_date=None
        self.current_time=None
        self.ORH=None
        self.ORL=None
        self.buyorder=None
        self.sellorder=None
        self.buystoporder=None
        self.sellstoporder=None
        self.bought_today=False
        self.buystop=False
        self.sellplaced=False
        self.sellstop=False
        self.closingbell=False
        self.ind=0





    def next(self):
        self.current_date=self.datetime.date(0)
        self.currrent_date5m=self.datetime5m.date(-1)
        self.currrent_date15m = self.datetime15m.date(-1)
        self.current_time=self.datetime.time(0)
        self.current_time5m=self.datetime5m.time(-1)
        self.current_time15m=self.datetime15m.time(-1)
        self.current_datetime=self.datetime.datetime(0)
        self.current_datetime5m=self.datetime5m.datetime(-1)
        self.current_datetime15m=self.datetime15m.datetime(-1)
        '''
        self.log("1 min:"+str(self.current_datetime))
        self.log("5 min:"+str(self.current_datetime5m))
        #self.log("15 min:"+str(self.current_datetime15m))
        self.log("1 min close:"+str(self.dataclose[0]))
        self.log("5 min close:"+str(self.dataclose5m[-1]))
        '''


        #print(self.current_date)
        #print(self.current_time)
        if (self.current_time.minute) % 5 == 0:
            self.ind = -1
        if (self.current_time.minute) % 5 == 4:
            self.ind = -5
        if (self.current_time.minute) % 5 == 3:
            self.ind = -4
        if (self.current_time.minute) % 5 == 2:
            self.ind = -3
        if (self.current_time.minute) % 5 == 1:
            self.ind = -2

        if self.current_time==self.morning_start.time():
            self.highlist=[]
            self.lowlist=[]
            self.buy_counter=0
            self.buy_exit=0
            self.bought_today=False
            self.buyorder = None
            self.sellorder = None
            self.buystoporder = None
            self.sellstoporder = None
            self.buystop=False
            self.sellstop=False
            self.closingbell=False
            self.sellplaced=False

        if self.current_time>=self.morning_start.time() and self.current_time<self.rangecandle_stop.time():
            self.highlist.append(self.datahigh[0])
            self.lowlist.append(self.datalow[0])
            self.ORH=max(self.highlist)
            self.ORL=min(self.lowlist)
        if self.current_time==datetime.now().replace(hour=10,minute=45,second=0,microsecond=0).time():
            print("Time:",self.current_datetime)
            print("ORH:",self.ORH)
            print("ORL:",self.ORL)





        '''
        
        print("Buy Order:",self.buyorder)
        print("Sell Order:", self.sellorder)
        print("Buy Stop order:",self.buystoporder)
        print("Sell Stop order:",self.sellstoporder)
        '''

        if not self.position:
            #self.buyorder=None
            #self.sellorder=None
            #print("Current DateTime:",self.current_datetime)


            if self.current_time==self.time_stop.time():
                for open_order in [o for o in self.broker.orders if o.status < 4]:
                    self.cancel(open_order)







            if self.current_time>=self.rangecandle_stop.time() and self.current_time<=self.evening_end.time():
                if self.buyorder is None and self.bought_today==False:


                    self.buyorder=self.buy(data=self.datas[0],exectype=bt.Order.Stop,price=self.ORH+1,valid=0)
                    self.bought_today=True


                    
                #self.buy_counter+=1



        else:

            #print("Current DateTime:", self.current_datetime)
            #print("Buy Order:", self.buyorder)
            #print("Sell Order:", self.sellorder)
            










            





            if self.current_time==self.time_stop.time():
                for open_order in [o for o in self.broker.orders if o.status < 4]:
                    self.cancel(open_order)

                self.closingbell=True
                self.close(data=self.datas[0])


                self.buyorder = None
                self.sellorder = None
                self.buystoporder = None
                self.sellstoporder = None




                self.log("exit done:")
                self.log("Updated balance:"+str(self.broker.getvalue()))
            elif self.dataclose[self.ind] < self.ORL and self.bought_today == True and self.buystop == False and self.closingbell==False and (self.datahigh[0]<self.ORH and self.datahigh[-1]<self.ORH and self.datahigh[-2]<self.ORH and self.datahigh[-3]<self.ORH and self.datahigh[-4]<self.ORH ):
                self.buystop = True
                print("1m close:",self.dataclose[self.ind])
                print("5m close:",self.dataclose5m[-1])
                self.close(data=self.datas[0])
                #self.buystop = False
                self.sellplaced = True
                self.sellorder = self.sell(data=self.datas[0], exectype=bt.Order.Market, valid=0)

            elif self.dataclose[self.ind] > self.ORH and self.sellplaced == True and self.sellstop == False and self.closingbell==False and (self.datalow[0]>self.ORL and self.datalow[-1]>self.ORL and self.datalow[-2]>self.ORL and self.datalow[-3]>self.ORL and self.datalow[-4]>self.ORL ):
                self.sellstop = True
                self.close(data=self.datas[0])



    def stop(self):
        self.log("Done with processing")










if __name__=='__main__':
    cerebro=bt.Cerebro()
    commissions=CommInfo(
        commission=0.001,
        mult=25,
        margin=95000
    )
    cerebro.broker.addcommissioninfo(commissions)
    cerebro.addstrategy(FirstStrategy)
    datapath="D:/Trading/Backtesting_Course/testdata/banknifty2018_1min.csv"
    datapath2="D:/Trading/Backtesting_Course/testdata/banknifty2018_5min.csv"
    datapath3="D:/Trading/Backtesting_Course/testdata/banknifty2018_15min.csv"
    data=bt.feeds.GenericCSVData(
        dataname=datapath,
        fromdate=dt.datetime(2011, 1, 1),
        todate=dt.datetime(2020, 1, 1),
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
    data2 = bt.feeds.GenericCSVData(
        dataname=datapath2,
        fromdate=dt.datetime(2011, 1, 1),
        todate=dt.datetime(2020, 1, 1),
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
    data3 = bt.feeds.GenericCSVData(
        dataname=datapath3,
        fromdate=dt.datetime(2011, 1, 1),
        todate=dt.datetime(2020, 1, 1),
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
    cerebro.adddata(data2)
    cerebro.adddata(data3)
    cerebro.addsizer(bt.sizers.FixedSize, stake=1)
    #cerebro.broker.setcommission(commission=0.001)
    cerebro.broker.set_cash(1000000.00)
    print("Starting portfolio value:" + str(cerebro.broker.getvalue()))
    cerebro.run()
    print("Final portfolio value:" + str(cerebro.broker.getvalue()))

