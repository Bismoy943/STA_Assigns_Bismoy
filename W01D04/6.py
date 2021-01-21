sma20=float(input("Enter 20 day sma:"))
sd20=float(input("Enter 20 day sd:"))
mb=sma20
ub=sma20+(2*sd20)
lb=sma20-(2*sd20)
print("Middle band:{}\nUpper band:{}\nLower band:{}".format(mb,ub,lb))

