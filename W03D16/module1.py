def turnover(open,high,low,close,volume):
    turnover=[]
    for i in range(len(high)):
        turnover.append(((high[i]+low[i])/2)*volume[i])
    return turnover

#Q1 1
def sumvolume(open,high,low,close,volume):
    sumvol = 0
    for i in volume:
        sumvol=sumvol+i
    return sumvol
#2
def averagevolume(open,high,low,close,volume):
    sumvol = 0
    for i in volume:
        sumvol=sumvol+i
    return (sumvol/len(volume))

def averageclosing(open,high,low,close,volume):
    sumclose = 0
    for i in close:
        sumclose=sumclose+i
    return (sumclose/len(close))

def pricerange(high,low):
    price_range=[]
    for i in range(len(high)):
        price_range.append(round(high[i]-low[i],2))
    return price_range

def smalist(last30closingprices):
    smalist=[]
    for i in range(len(last30closingprices)):
        if i<=3:
            smalist.append(0)
        if i>3:
            smalist.append(round((sum(last30closingprices[i-4:i+1]))/5,2))
    return smalist


def returnpivots(pdo,pdh,pdl,pdc):
    p=(pdh+pdl+pdc)/3
    r1=(2*p)-pdl
    r2=p+(pdh-pdl)
    r3=r1+(pdh-pdl)
    s1=(2*p)-pdh
    s2=p-(pdh-pdl)
    s3=s1-(pdh-pdl)
    listpivots=[p,r1,r2,r3,s1,s2,s3]
    return listpivots
