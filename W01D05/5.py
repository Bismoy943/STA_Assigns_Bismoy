cp=float(input("Enter today's closing price:"))
sma20=float(input("Enter 20 ma:"))
sma50=float(input("Enter 50 ma:"))
if cp>sma20 and cp>sma50:
    if sma20>sma50:
        print("Go long")
elif cp<sma20 and cp<sma50:
    if sma20<sma50:
        print("Go Short")
else:
    print("Dont do anything")
    