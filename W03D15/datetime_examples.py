import datetime
import pytz
print("Year={},month={},day={}".format(datetime.date.today().year,datetime.date.today().month,
                                       datetime.date.today().day))
now=datetime.datetime.now()
a=datetime.date.fromtimestamp(1612201246.702716)
print(a)

t1=datetime.date(year=2021,month=1,day=30)
t2=datetime.date(year=2021,month=1,day=31)
t3=t1-t2
print(t3,type(t3))

print("Local time:",now.strftime("%Y/%m/%d,%H:%M:%S"))
timezonenewyork=pytz.timezone('America/New_York')
timeinnewyork=datetime.datetime.now(timezonenewyork)
print("Time in new york:",timeinnewyork.strftime("%Y-%m-%d,%H:%M:%S"))
