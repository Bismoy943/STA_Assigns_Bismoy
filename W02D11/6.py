def returnpivots(pdo,pdh,pdl,pdc):
    p=(pdh+pdl+pdc)/3
    r1=(2*p)-pdl
    r2=p+(pdh-pdl)
    r3=r1+(pdh-pdl)
    s1=(2*p)-pdh
    s2=p-(pdh-pdl)
    s3=s1-(pdh-pdl)
    listpivots=[r3,r2,r1,p,s1,s2,s3]
    return listpivots

pivotlist=returnpivots(14237.95,14237.95,13929.3,13967.5)
print("Pivots are:",pivotlist)