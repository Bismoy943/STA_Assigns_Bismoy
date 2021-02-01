from datetime import date
from nsepy import get_history
from nsepy import get_index_pe_history
from nsepy.derivatives import get_expiry_date
data=get_history(symbol="SBIN",start=date(2019,1,1),end=date(2019,1,31))
vix = get_history(symbol="INDIAVIX",
            start=date(2021,1,1),
            end=date(2021,1,29),
            index=True)
#print(get_expiry_date(year=2021,month=1,index="NIFTY"),type(get_expiry_date(year=2021,month=1,index="NIFTY")))

