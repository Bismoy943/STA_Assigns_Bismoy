d1=[14458.5,14458.95,14250,14287.25,13200000]
d2=[14380.5,14586.3,14364.25,14570,11000000]
d3=[14581,14670.45,14524.1,14645.15,8850000]
d4=[14711.05,14765.45,14508.3,14598.65,9920000]
d5=[14593,14625.9,14356,14380.15,16400000]
bullishengulfing=[False]
bearishengulfing=[False]
insidebar=[False]
harami=[False]
risingsun=[False]
darkcloudcover=[False]
mdlist=[d1,d2,d3,d4,d5]
sumvol=0
sumavgclosing=0
listclosingprice=[]
listvolume=[]
openlist=[]
closelist=[]
len1=len(mdlist)
for i in range(len1):
    openlist.append(mdlist[i][0])
    closelist.append(mdlist[i][3])
    len2=len(mdlist[i])
    for j in range(len2):
        if j==len2-2:
            listclosingprice.append(mdlist[i][j])
            sumavgclosing=sumavgclosing+mdlist[i][j]
        if j==len2-1:
            listvolume.append(mdlist[i][j])
            sumvol=sumvol+mdlist[i][j]

print("Sum of volume:",sumvol)
print("Average of closing price:",sumavgclosing/len1)
print("Highest of closing price in list {} is {}"
      .format(listclosingprice,max(listclosingprice)))
print("Lowest of closing price in list {} is {}"
      .format(listclosingprice,min(listclosingprice)))
print("Highest of volume in list {} is {}".format(listvolume,max(listvolume)))
print("Lowest of volume in list {} is {}".format(listvolume,min(listvolume)))
print("Sorted volume list:",sorted(listvolume))


for i in range(len1):

    if i!=len1-1:

        op = mdlist[i][0]
        hp = mdlist[i][1]
        lp = mdlist[i][2]
        cp = mdlist[i][3]
        oc = mdlist[i+1][0]
        hc = mdlist[i+1][1]
        lc = mdlist[i+1][2]
        cc = mdlist[i+1][3]
        if hc < hp and lc > lp:
            insidebar.append(True)
            bullishengulfing.append(False)
            bearishengulfing.append(False)
            harami.append(False)
            risingsun.append(False)
            darkcloudcover.append(False)
        elif oc <= cp and cc > op:
            insidebar.append(False)
            bullishengulfing.append(True)
            bearishengulfing.append(False)
            harami.append(False)
            risingsun.append(False)
            darkcloudcover.append(False)
        elif oc >= cp and cc < op:
            insidebar.append(False)
            bullishengulfing.append(False)
            bearishengulfing.append(True)
            harami.append(False)
            risingsun.append(False)
            darkcloudcover.append(False)
        elif hc < hp and hc < op and lc > cp and lc > lp and oc<cc:
            insidebar.append(False)
            bullishengulfing.append(False)
            bearishengulfing.append(False)
            harami.append(True)
            risingsun.append(False)
            darkcloudcover.append(False)
        elif hc < hp and hc < cp and lc > op and lc > lp and oc>cc:
            insidebar.append(False)
            bullishengulfing.append(False)
            bearishengulfing.append(False)
            harami.append(True)
            risingsun.append(False)
            darkcloudcover.append(False)
        elif lc < lp and lc < cp and oc < cp and hc < hp and cc < op and hc < op and oc<cc:
            insidebar.append(False)
            bullishengulfing.append(False)
            bearishengulfing.append(False)
            harami.append(False)
            risingsun.append(True)
            darkcloudcover.append(False)
        elif lc > lp and lc > op and cc > op and hc > hp and oc > cp and hc > cp and oc>cc:
            insidebar.append(False)
            bullishengulfing.append(False)
            bearishengulfing.append(False)
            harami.append(False)
            risingsun.append(False)
            darkcloudcover.append(True)
        else:
            insidebar.append(False)
            bullishengulfing.append(False)
            bearishengulfing.append(False)
            harami.append(False)
            risingsun.append(False)
            darkcloudcover.append(False)
    else:
        print("Done with current iteration")
print("Bullish engulfing:",bullishengulfing)
print("Bearish engulfing:",bearishengulfing)
print("Inside bar:",insidebar)
print("Harami:",harami)
print("Rising sun:",risingsun)
print("Dark cloud cover:",darkcloudcover)
print("Open Prices list:",openlist)
print("Close Prices list:",closelist)
print("Difference in close and open prices:",[x-y for (x,y) in zip(closelist,openlist)])




