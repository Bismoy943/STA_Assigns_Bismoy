d1=[14458.5,14458.95,14250,14287.25,13200000]
d2=[14380.5,14586.3,14364.25,14570,11000000]
d3=[14581,14670.45,14524.1,14645.15,8850000]
d4=[14711.05,14765.45,14508.3,14598.65,9920000]
d5=[14593,14625.9,14356,14380.15,16400000]
d1updated=[]
d2updated=[]
d3updated=[]
d4updated=[]
d5updated=[]
for i in range(0,len(d1)+1):
    if i==len(d1):
        d1updated.append(((d1[1]+d1[2])/2)*d1[4])
    else:
        d1updated.append(d1[i])

print("Day one List:",d1updated)

for i in range(0,len(d2)+1):
    if i==len(d2):
        d2updated.append(((d2[1]+d2[2])/2)*d2[4])
    else:
        d2updated.append(d2[i])
print("Day two List:",d2updated)

for i in range(0,len(d3)+1):
    if i==len(d3):
        d3updated.append(((d3[1]+d3[2])/2)*d3[4])
    else:
        d3updated.append(d3[i])
print("Day three List:",d3updated)

for i in range(0,len(d4)+1):
    if i==len(d4):
        d4updated.append(((d4[1]+d4[2])/2)*d4[4])
    else:
        d4updated.append(d4[i])
print("Day four List:",d4updated)

for i in range(0,len(d5)+1):
    if i==len(d5):
        d5updated.append(((d5[1]+d5[2])/2)*d5[4])
    else:
        d5updated.append(d5[i])
print("Day five List:",d5updated)

mdupdated=[d1updated,d2updated,d3updated,d4updated,d5updated]
for i in range(0,len(mdupdated)):
    print("Day {} list :".format(i+1),mdupdated[i])

