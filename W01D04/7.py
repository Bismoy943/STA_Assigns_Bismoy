cp=float(input("Enter current closing price:"))
cp50=float(input("Enter closing price 50 bars ago:"))
roc=((cp/cp50)-1.0)*100
print("Rate of change=",roc)