
'''
import numpy as np
import talib
sonu=[31.24,24.35,16.04,18.35]
ndarray=np.array(sonu)
print(ndarray)
sma2=talib.SMA(ndarray, timeperiod=2)
print(sma2)
'''

import talib as ta
import numpy as np
import config as con

#i
def overbought(close):
    rsi=ta.RSI(np.array(close),timeperiod=14)
    print("RSI14 looks like:",rsi)
    if rsi[len(rsi) - 1] > 80:
        print("Stock is overbought as RSI is {}".format(rsi[len(rsi) - 1]))
    else:
        print("Stock is not overbought as RSI is {}".format(rsi[len(rsi) - 1]))

overbought(con.close)

#ii
def oversold(close):
    rsi=ta.RSI(np.array(close),timeperiod=14)
    print("RSI14 looks like:",rsi)
    if rsi[len(rsi) - 1] < 20:
        print("Stock is oversold as RSI is {}".format(rsi[len(rsi) - 1]))
    else:
        print("Stock is not oversold as RSI is {}".format(rsi[len(rsi) - 1]))

oversold(con.close)

#iii
def sma(close,timeperiod):
    smaupdated=ta.SMA(np.array(close),timeperiod=timeperiod)
    return smaupdated
print("SMA 5 is as follows:",sma(con.close,5).tolist())
print("SMA 20 is as follows:",sma(con.close,20).tolist())

#iv
sma5=sma(con.close,5)
sma20=sma(con.close,20)
sma5list=list(sma5)
sma20list=list(sma20)

def longshortlist(sma5list,sma20list):
    longorshort=[]
    for i in range(len(sma5list)):
        if sma5list[i]>sma20list[i]:
            longorshort.append(1)
        elif sma5list[i]<sma20list[i]:
            longorshort.append(-1)
        else:
            longorshort.append(0)
    return longorshort

longorshortlist=longshortlist(sma5list,sma20list)






