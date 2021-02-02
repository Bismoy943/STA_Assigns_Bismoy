import candlestickpatterns as cp

open=[14629.6,14473.95,14398.7,14580,14732.75]
high=[14652.5,14479.95,14619.75,14703.95,14798.3]
low=[14413.6,14281.65,14388.9,14553.7,14558.8]
close=[14493.3,14316.1,14605.3,14683.2,14632.7]
volume=[701000,834000,718000,726000,1130000]


print("Inside bar list:",cp.insidebar(open,high,low,close,volume))
print("Bullish engulfing list:",cp.bullishengulfing(open,high,low,close,volume))
print("Bearish engulfing list:",cp.bearishengulfing(open,high,low,close,volume))
print("Harami list:",cp.harami(open,high,low,close,volume))
print("Rising sun list:",cp.risingsun(open,high,low,close,volume))
print("Dark cloud cover list:",cp.darkcloud(open,high,low,close,volume))