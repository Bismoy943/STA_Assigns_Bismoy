from datetime import date
from nsepy import get_history as gh
nifty50=gh(symbol="NIFTY",start=date(2020,10,27),end=date(2021,2,3),index=True)
open=list(nifty50['Open'])
high=list(nifty50['High'])
low=list(nifty50['Low'])
close=list(nifty50['Close'])
volume=list(nifty50['Volume'])


