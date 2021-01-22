todaycp=float(input("Enter today's closing price of Nifty:"))
highprice52wk=float(input("Enter last 52 week high price:"))
if todaycp>highprice52wk:
    print("Today's closing price higher than last 52 week high price")
else:
    print("Today's closing price is lower than last 52 week high price")
