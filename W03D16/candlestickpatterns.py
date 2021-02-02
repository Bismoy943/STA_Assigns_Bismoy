def insidebar(open,high,low,close,volume):
    insidebarlist=[False]
    for i in range(len(high)):
        if i==len(high)-1:
            break
        elif high[i+1]<high[i] and low[i+1]>low[i]:
            insidebarlist.append(True)
        else:
            insidebarlist.append(False)
    return insidebarlist

def bullishengulfing(open,high,low,close,volume):
    bullishengulfinglist=[False]
    for i in range(len(open)):
        if i==len(open)-1:
            break
        elif open[i+1]<=close[i] and close[i+1]>open[i] and close[i+1]>open[i+1] and close[i]<open[i]:
            bullishengulfinglist.append(True)
        else:
            bullishengulfinglist.append(False)
    return bullishengulfinglist

def bearishengulfing(open,high,low,close,volume):
    bearishengulfinglist=[False]
    for i in range(len(open)):
        if i==len(open)-1:
            break
        elif open[i+1]>=close[i] and close[i+1]<open[i] and close[i+1]<open[i+1] and close[i]>open[i]:
            bearishengulfinglist.append(True)
        else:
            bearishengulfinglist.append(False)
    return bearishengulfinglist

def harami(open,high,low,close,volume):
    haramilist=[False]
    for i in range(len(high)):
        if i==len(high)-1:
            break
        elif high[i+1] < high[i] and high[i+1] < open[i] and low[i+1] > close[i] and low[i+1] > low[i] and open[i+1] < close[i+1]:
            haramilist.append(True)
        elif high[i+1] < high[i] and high[i+1] < close[i] and low[i+1] > open[i] and low[i+1] > low[i] and open[i+1] > close[i+1]:
            haramilist.append(True)
        else:
            haramilist.append(False)
    return haramilist

def risingsun(open,high,low,close,volume):
    risingsunlist=[False]
    for i in range(len(low)):
        if i==len(low)-1:
            break
        elif low[i+1] < low[i] and low[i+1] < close[i] and open[i+1] < close[i] and high[i+1] < high[i] and close[i+1] < open[i] and high[i+1] < open[i] and open[i+1] < close[i+1]:
            risingsunlist.append(True)
        else:
            risingsunlist.append(False)
    return  risingsunlist

def darkcloud(open,high,low,close,volume):
    darkcloudlist=[False]
    for i in range(len(low)):
        if i==len(low)-1:
            break
        elif low[i+1] > low[i] and low[i+1] > open[i] and close[i+1] > open[i] and high[i+1] > high[i] and open[i+1] > close[i] and high[i+1] > close[i] and open[i+1] > close[i+1]:
            darkcloudlist.append(True)
        else:
            darkcloudlist.append(False)
    return darkcloudlist


