open_price=14581
high_price=14670
low_price=14524.4
close_price=14634
volume=8840000
turnover=2500000
print(open_price)
print(high_price)
print(low_price)
print(close_price)
print(volume)
print(turnover)
print("Open:{}\nHigh:{}\nLow:{}\nClose:{}\nVolume:{}\nTurnover:{}"
.format(open_price,high_price,low_price,close_price,volume,turnover))
print(type(open_price))
print(type(high_price))
print(type(low_price))
print(type(close_price))
print(type(volume))
print(type(turnover))
from datetime import date
today=str(date.today())
print(open_price,low_price,high_price,close_price,volume,turnover,today)
open_price=str(open_price)
high_price=str(high_price)
low_price=str(low_price)
close_price=str(close_price)
volume=str(volume)
turnover=str(turnover)
print(open_price,type(open_price),high_price,type(high_price),low_price,type(low_price),
      close_price,type(close_price),volume,type(volume),turnover,type(turnover))
true_value=True
false_value=False
print(true_value,false_value)