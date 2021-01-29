open=[14629.6,14473.95,14398.7,14580,14732.75]
high=[14652.5,14479.95,14619.75,14703.95,14798.3]
low=[14413.6,14281.65,14388.9,14553.7,14558.8]
close=[14493.3,14316.1,14605.3,14683.2,14632.7]
volume=[701000,834000,718000,726000,1130000]

mdlist=[open,high,low,close,volume]

#2
def funcn1():
    for i in range(len(mdlist)):
        print(mdlist[i])

funcn1()

def funcn2():
    dayslist=[]
    for i in range(len(mdlist)):
        for j in range(len(mdlist)):
            dayslist.append(mdlist[j][i])
        print("Day{} list as:".format(i+1),dayslist)
        dayslist=[]

funcn2()

#3
def turnover(open,high,low,close,volume):
    turnover=[]
    for i in range(len(high)):
        turnover.append(((high[i]+low[i])/2)*volume[i])
    return turnover

print("Turnover list:",turnover(open,high,low,close,volume))

#Q1 1
def sumvolume(open,high,low,close,volume):
    sumvol = 0
    for i in volume:
        sumvol=sumvol+i
    print("Sum total of volume=",sumvol)
#2
def averagevolume(open,high,low,close,volume):
    sumvol = 0
    for i in volume:
        sumvol=sumvol+i
    print("Average of volume=",(sumvol/len(volume)))

sumvolume(open,high,low,close,volume)
averagevolume(open,high,low,close,volume)

#3
def averageclosing(open,high,low,close,volume):
    sumclose = 0
    for i in close:
        sumclose=sumclose+i
    print("Average of close=",(sumclose/len(close)))

averageclosing(open,high,low,close,volume)

#4
def hlclose(open,high,low,close,volume):
    print("List of closing price:",close)
    print("Highest close:",max(close))
    print("Lowest close:",min(close))

hlclose(open,high,low,close,volume)

#5
def hlopen(open,high,low,close,volume):
    print("List of opening price:",open)
    print("Highest open:",max(open))
    print("Lowest open:",min(open))

hlopen(open,high,low,close,volume)

#6
print("Highest high price from the week:",max(high))
#7
print("Lowest low price from the week:",min(low))

#8
def pricerange(high,low):
    price_range=[]
    for i in range(len(high)):
        price_range.append(round(high[i]-low[i],2))
    return price_range

print("Price range list for all days:",pricerange(high,low))

#9

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

print("Inside bar list:",insidebar(open,high,low,close,volume))
print("Bullish engulfing list:",bullishengulfing(open,high,low,close,volume))
print("Bearish engulfing list:",bearishengulfing(open,high,low,close,volume))
print("Harami list:",harami(open,high,low,close,volume))
print("Rising sun list:",risingsun(open,high,low,close,volume))
print("Dark cloud cover list:",darkcloud(open,high,low,close,volume))


#10
last30closingprices = [13558.15, 13567.85, 13682.7, 13740.7, 13760.55, 13328.4,
                       13466.3, 13601.1, 13749.25, 13873.2, 13932.6, 13981.95, 13981.75, 14018.5,
                       14132.9, 14199.5, 14146.25, 14137.35, 14347.25, 14484.75, 14563.45,
                       14564.85, 14595.6, 14433.7, 14281.3, 14521.15, 14644.7, 14590.35,
                       14371.9, 14238.9]

def smalist(last30closingprices):
    smalist=[0,0,0,0]
    for i in range(len(last30closingprices)):
        if i>3:
            smalist.append(round((sum(last30closingprices[i-4:i+1]))/5,2))
    return smalist

print("Nifty sma5 list as follows:",smalist(last30closingprices))




