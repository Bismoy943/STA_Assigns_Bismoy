pdo=float(input("Enter previous day open:"))
pdl=float(input("Enter previous day low:"))
pdh=float(input("Enter previous day high:"))
pdc=float(input("Enter previous day close:"))
P=(pdh+pdl+pdc)/3
R1=(2*P) -pdl
R2=P+(pdh-pdl)
R3=R1+(pdh-pdl)
S1=(2*P)-pdh
S2=P-(pdh-pdl)
S3=S1-(pdh-pdl)
print("Pivot:{} Resistance1:{} Resistance2:{} Resistance3:{} Support1:{} Support2:{} Support3:{}"
      .format(P,R1,R2,R3,S1,S2,S3))