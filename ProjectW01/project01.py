maxcp5=14644.7
maxcp120b5=14595.6
cv=16400000
cp=14380.15
pcp=14598.65
cv1=8170000
cv2=4130000
cv3=8980000
cv4=6500000
cv5=12000000
cv6=1800000
sma5v=(cv2+cv3+cv4+cv5+cv6)/5

if maxcp5>maxcp120b5 and cv>sma5v and cp>pcp:
    print("Short term breakout")
else:
    print("No short term breakout")