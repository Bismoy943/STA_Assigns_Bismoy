import talib as ta
import numpy as np
import config as con
import talib as ta
import numpy as np
import config as con

# vi
rsi = ta.RSI(np.array(con.close), timeperiod=14)
adx = ta.ADX(np.array(con.high), np.array(con.low), np.array(con.close), timeperiod=14)


def buysell(rsi, adx):
    buyselllist = []
    for i in range(len(rsi)):
        if adx[i] > 50 and rsi[i] < 20:
            buyselllist.append(1)
        elif adx[i] > 50 and rsi[i] > 80:
            buyselllist.append(-1)
        else:
            buyselllist.append(0)
    return buyselllist

print("Buy sell list:",buysell(rsi,adx))
# vii
def atr(high, low, close):
    atrlist = ta.ATR(high, low, close, timeperiod=14)
    return atrlist


print("ATR list:",atr(np.array(con.high), np.array(con.low), np.array(con.close)))

# viii

upperband, middleband, lowerband = ta.BBANDS(np.array(con.close), timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)
def longshortsignals(upperband,middleband,lowerband,close):
    signallist=[]
    for i in range(len(upperband)):
        if close[i]>upperband[i]:
            signallist.append(-1)
        elif close[i]<lowerband[i]:
            signallist.append(1)
        else:
            signallist.append(0)
    return upperband.tolist(),middleband.tolist(),lowerband.tolist(),signallist

print("BBands and signals list:",longshortsignals(upperband,middleband,lowerband,np.array(con.close)))






