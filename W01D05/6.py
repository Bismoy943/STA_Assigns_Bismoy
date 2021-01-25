op=float(input("Enter open price of previous bar"))
hp=float(input("Enter high price of previous bar"))
lp=float(input("Enter low price of previous bar"))
cp=float(input("Enter close price of previous bar"))
oc=float(input("Enter open price of current bar"))
hc=float(input("Enter high price of current bar"))
lc=float(input("Enter low price of current bar"))
cc=float(input("Enter close price of current bar"))

if hc<hp and lc>lp:
    print("Current bar is inside bar")
elif oc<=cp and cc>op:
    print("Current bar is bullish engulfing")
elif oc>=cp and cc<op:
    print("Current bar is bearish engulfing")
elif hc<hp and hc<op and lc>cp and lc>lp and oc<cc:
    print("Current bar is bullish harami")
elif hc<hp and hc<cp and lc>op and lc>lp and oc>cc:
    print("Current bar is bearish harami")
elif lc<lp and lc<cp and oc<cp and hc<hp and cc<op and hc<op and oc<cc:
    print("Current bar is rising sun")
elif lc>lp and lc>op and cc>op and hc>hp and oc>cp and hc>cp and oc>cc:
    print("Current bar is dark cloud")
else:
    print("Current bar is some other candlesticks pattern")