op=float(input("Enter yesterday open price:"))
hp=float(input("Enter yesterday high price:"))
lp=float(input("Enter yesterday low price:"))
cp=float(input("Enter yesterday close price:"))
oc=float(input("Enter today open price:"))
hc=float(input("Enter today high price:"))
lc=float(input("Enter today low price:"))
cc=float(input("Enter today close price:"))

#mdlist=[[op,hp,lp,cp],[oc,hc,lc,cc]]
def is_inside_bar(op,hp,lp,cp,oc,hc,lc,cc):
    if hc < hp and lc > lp:
        print("Today's candle fulfills the inside bar pattern")
    else:
        print("Today's candle doesn't fulfill the inside bar pattern")



def is_bullish_engulfing(op,hp,lp,cp,oc,hc,lc,cc):
    if oc <= cp and cc > op:
        print("Today's candle fulfills the bullish engulfing pattern")
    else:
        print("Today's candle doesn't fulfill the bullish engulging pattern")


def is_bearish_engulfing(op,hp,lp,cp,oc,hc,lc,cc):
    if oc >= cp and cc < op:
        print("Today's candle fulfills the bearish engulfing pattern")
    else:
        print("Today's candle doesn't fulfill the bearish engulging pattern")

def is_harami(op,hp,lp,cp,oc,hc,lc,cc):
    if hc < hp and hc < op and lc > cp and lc > lp and oc < cc:
        print("Today's candle fulfills the harami pattern")
    elif hc < hp and hc < cp and lc > op and lc > lp and oc > cc:
        print("Today's candle fulfills the harami pattern")
    else:
        print("Today's candle doesn't fulfill the harami pattern")

def is_rising_sun(op,hp,lp,cp,oc,hc,lc,cc):
    if lc < lp and lc < cp and oc < cp and hc < hp and cc < op and hc < op and oc < cc:
        print("Today's candle fulfills the rising sun pattern")
    else:
        print("Today's candle doesn't fulfill the rising sun pattern")

def is_darkcloud_cover(op,hp,lp,cp,oc,hc,lc,cc):
    if lc > lp and lc > op and cc > op and hc > hp and oc > cp and hc > cp and oc > cc:
        print("Today's candle fulfills the dark cloud cover pattern")
    else:
        print("Today's candle doesn't fulfill the dark cloud cover pattern")

is_inside_bar(op,hp,lp,cp,oc,hc,lc,cc)
is_bullish_engulfing(op,hp,lp,cp,oc,hc,lc,cc)
is_bearish_engulfing(op,hp,lp,cp,oc,hc,lc,cc)
is_harami(op,hp,lp,cp,oc,hc,lc,cc)
is_rising_sun(op,hp,lp,cp,oc,hc,lc,cc)
is_darkcloud_cover(op,hp,lp,cp,oc,hc,lc,cc)

