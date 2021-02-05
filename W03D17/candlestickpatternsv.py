import talib as ta
import numpy as np
import config as con

def twocrows(open,high,low,close):
    return ta.CDL2CROWS(open, high, low, close)

def threecrows(open,high,low,close):
    return ta.CDL3BLACKCROWS(open,high,low,close)

def threewhitesoldiers(open,high,low,close):
    return ta.CDL3WHITESOLDIERS(open, high, low, close)

def hammer(open,high,low,close):
    return ta.CDLHAMMER(open, high, low, close)

def harami(open,high,low,close):
    return ta.CDLHARAMI(open, high, low, close)

def marubozu(open,high,low,close):
    return ta.CDLMARUBOZU(open, high, low, close)

def piercing(open,high,low,close):
    return ta.CDLPIERCING(open, high, low, close)

def shootingstar(open,high,low,close):
    return ta.CDLSHOOTINGSTAR(open, high, low, close)

def spinningtop(open,high,low,close):
    return ta.CDLSPINNINGTOP(open, high, low, close)






#print(threewhitesoldiers(np.array(con.open),np.array(con.high),np.array(con.low),np.array(con.close)))

#print(threecrows(np.array(con.open),np.array(con.high),np.array(con.low),np.array(con.close)))