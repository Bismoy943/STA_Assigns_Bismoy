import backtrader as bt
import datetime as dt
import talib as ta
import numpy as np

datapath = "D:/Trading/Backtesting_Course/testdata/Nifty-1D.csv"
datapath2 = "D:/Trading/Backtesting_Course/testdata/NiftyFutures-1D.csv"
openlist=[]
highlist=[]
lowlist=[]
closelist=[]
yyyymmdd=[]

with open("D:/Trading/Backtesting_Course/testdata/Nifty-1D.csv","r") as nifty_spot:
    firstline=True
    for line in nifty_spot:
        print(line)
        if firstline:
            firstline=False
            continue
        x=line.rstrip("\n").split(",")
        for i in range(len(x)):
            if i==0:
                yyyymmdd.append(dt.datetime.strptime(x[i],'%Y-%m-%d'))
            elif i==1:
                openlist.append(float(x[i]))
            elif i==2:
                highlist.append(float(x[i]))
            elif i==3:
                lowlist.append(float(x[i]))
            elif i==4:
                closelist.append(float(x[i]))
nifty_spot.close()
print("Date list:",yyyymmdd)
print("Open list:",openlist)
print("High list:",highlist)
print("Low list:",lowlist)
print("Close list:",closelist)
rsi=ta.RSI(np.array(closelist),timeperiod=14)
if rsi[len(rsi)-1]<20:
    print("Nifty is oversold")
elif rsi[len(rsi)-1]>80:
    print("Nifty is overbought")
else:
    print("Nifty is neither oversold or overbought")


