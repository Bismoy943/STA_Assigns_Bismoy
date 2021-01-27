d1=[14458.5, 14458.95, 14250, 14287.25, 13200000, 189479070000.0]
d2=[14380.5, 14586.3, 14364.25, 14570, 11000000, 159228025000.0]
d3=[14581, 14670.45, 14524.1, 14645.15, 8850000, 129185883750.00002]
d4=[14711.05, 14765.45, 14508.3, 14598.65, 9920000, 145197800000.0]
d5=[14593, 14625.9, 14356, 14380.15, 16400000, 237651580000.0]

sumvol=0
counter=0
counterclosing=0
sumavgclosing=0
listclosingprice=[]
listvolume=[]
openlist=[]
closelist=[]
listhighprice=[]
listlowprice=[]
price_range=[]

mdlist=[d1,d2,d3,d4,d5]
i=0
j=0

while i<len(mdlist):
    print("Day {} list:".format(i+1),mdlist[i])
    i=i+1

i=0
j=0

while i < len(mdlist):
    while j < len(mdlist[i]):
        if j == len(mdlist[i]) - 2:
            sumvol = sumvol + mdlist[i][j]
            counter += 1
        if j == len(mdlist[i]) - 3:
            listclosingprice.append(mdlist[i][j])
            sumavgclosing = sumavgclosing + mdlist[i][j]
            counterclosing += 1
        if j == 0:
            openlist.append(mdlist[i][j])
        if j == 1:
            listhighprice.append(mdlist[i][j])
        if j == 2:
            listlowprice.append(mdlist[i][j])
        j=j+1
    i=i+1

print(listclosingprice)
highestclose = listclosingprice[0]
lowestclose = listclosingprice[0]
highestopen = openlist[0]
lowestopen = openlist[0]
highesthigh = listhighprice[0]
lowestlow = listlowprice[0]

i=0
j=0

while i < len(listclosingprice):
    if listclosingprice[i] > highestclose:
        highestclose = listclosingprice[i]
    if listclosingprice[i] < lowestclose:
        lowestclose = listclosingprice[i]
    i=i+1

i=0
j=0

while i < len(openlist):
    if openlist[i] > highestopen:
        highestopen = openlist[i]
    if openlist[i] < lowestopen:
        lowestopen = openlist[i]
    i=i+1

i=0
j=0

while i < len(listhighprice):
    if listhighprice[i] > highesthigh:
        highesthigh = listhighprice[i]
    i=i+1

i=0
j=0

while i < len(listlowprice):
    if listlowprice[i] < lowestlow:
        lowestlow = listlowprice[i]
    i=i+1

i=0
j=0

while i < len(mdlist):
    price_range.append(mdlist[i][1] - mdlist[i][2])
    i=i+1

# 1
print("Sum of volume=", sumvol)
# 2
print("Average of volume=", sumvol / counter)
# 3
print("Average closing price=", sumavgclosing / counterclosing)

# 4
print("List of closing prices:", listclosingprice)
print("Highest closing price:", highestclose)
print("Lowest closing price:", lowestclose)

# 5
print("List of opening prices:", openlist)
print("Highest opening price:", highestopen)
print("Lowest opening price:", lowestopen)

# 6
print("List of high prices:", listhighprice)
print("Highest high:", highesthigh)

# 7
print("List of low prices:", listlowprice)
print("Lowest low:", lowestlow)

# 8
print("Daywise price range:", price_range)

# 9 already done in previous assignment
# 10

last30closingprices = [13558.15, 13567.85, 13682.7, 13740.7, 13760.55, 13328.4,
                       13466.3, 13601.1, 13749.25, 13873.2, 13932.6, 13981.95, 13981.75, 14018.5,
                       14132.9, 14199.5, 14146.25, 14137.35, 14347.25, 14484.75, 14563.45,
                       14564.85, 14595.6, 14433.7, 14281.3, 14521.15, 14644.7, 14590.35,
                       14371.9, 14238.9]
sma5days = []
i=0
while i < len(last30closingprices):
    if i > 4:
        sma5days.append(round((last30closingprices[i - 1] + last30closingprices[i - 2]
                               + last30closingprices[i - 3] + last30closingprices[i - 4]
                               + last30closingprices[i - 5]) / 5, 2))
    i=i+1

print("Moving average from 6th day onwards looks like :", sma5days)

i=0
while i<len(sma5days):
    print("SMA{}:".format(i+1),sma5days[i])
    i+=1







