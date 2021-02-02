import module1 as md


last30closingprices = [13558.15, 13567.85, 13682.7, 13740.7, 13760.55, 13328.4,
                       13466.3, 13601.1, 13749.25, 13873.2, 13932.6, 13981.95, 13981.75, 14018.5,
                       14132.9, 14199.5, 14146.25, 14137.35, 14347.25, 14484.75, 14563.45,
                       14564.85, 14595.6, 14433.7, 14281.3, 14521.15, 14644.7, 14590.35,
                       14371.9, 14238.9]
open=[14629.6,14473.95,14398.7,14580,14732.75]
high=[14652.5,14479.95,14619.75,14703.95,14798.3]
low=[14413.6,14281.65,14388.9,14553.7,14558.8]
close=[14493.3,14316.1,14605.3,14683.2,14632.7]
volume=[701000,834000,718000,726000,1130000]

print("Turnover list data:",md.turnover(open,high,low,close,volume))
print("Sum of volume:",md.sumvolume(open,high,low,close,volume))
print("Average of volume",md.averagevolume(open,high,low,close,volume))
print("Average closing:",md.averageclosing(open,high,low,close,volume))
print("Price Range:",md.pricerange(high,low))
print("5 days sma list:",md.smalist(last30closingprices))
print("Pivots list:",md.returnpivots(14237.95,14237.95,13929.3,13967.5))