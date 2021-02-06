from datetime import date
from nsepy import get_history as gh
import talib as ta
import numpy as np
nifty50=gh(symbol="NIFTY",start=date(2010,12,1),end=date(2021,2,3),index=True)
open=list(nifty50['Open'])
high=list(nifty50['High'])
low=list(nifty50['Low'])
close=list(nifty50['Close'])
volume=list(nifty50['Volume'])

ema40list=list(ta.EMA(np.array(close),timeperiod=40))


def strategy(open,high,low,close,volume):
    buyselllist=[]
    for i in range(len(close)):
        if close[i]>ema40list[i]:
            buyselllist.append(1)
        elif close[i]<ema40list[i]:
            buyselllist.append(-1)
        else:
            buyselllist.append(0)
    return buyselllist


buyselllistt=strategy(open,high,low,close,volume)
print("Strategy results:",strategy(open,high,low,close,volume))

def strategypoints(open,high,low,close,volume,buyselllistt):
    sellflag=False
    buyflag=False
    netpointssell=0
    netpointsbuy=0
    buyentry=[]
    sellentry=[]
    for i in range(len(buyselllistt)):
        if buyselllistt[i]==0:
            #print("No trade taken")
            continue
        elif buyselllistt[i]==-1:
            if sellflag==False:
                sellentry.append(close[i])
                #print("Sell active and price is ",close[i])
                netpointssell=netpointssell+0
                if len(buyentry)!=0:
                    netpointsbuy=netpointsbuy+(close[i]-close[i-1])
                sellflag=True
                buyflag=False
                #print("Net points sell",netpointssell)
                #print("Net points buy",netpointsbuy)
                continue
            if sellflag==True:
                sellentry.append(close[i])
                netpointssell=netpointssell+(close[i-1]-close[i])
                #print("Sell active and price is ",close[i])
                #print("Net points sell", netpointssell)
                #print("Net points buy", netpointsbuy)
        elif buyselllistt[i]==1:
            if buyflag==False:
                #print("Buy active and price is ",close[i])
                buyentry.append(close[i])
                netpointsbuy=netpointsbuy+0
                if len(sellentry)!=0:
                    netpointssell=netpointssell+(close[i-1]-close[i])
                buyflag=True
                sellflag=False
                #print("Net points sell", netpointssell)
                #print("Net points buy", netpointsbuy)
                continue
            if buyflag==True:
                buyentry.append(close[i])
                netpointsbuy=netpointsbuy+(close[i]-close[i-1])
                #print("Buy active and price is ",close[i])
                #print("Net points sell", netpointssell)
                #print("Net points buy", netpointsbuy)

    print("Total buy points=",netpointsbuy)
    print("Total sell points=",netpointssell)


strategypoints(open,high,low,close,volume,buyselllistt)





