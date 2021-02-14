import ProjectW04.project04 as pw04

newobj=pw04.PositionSizer(100000.00)
print("No of lots needed for the Nifty trade is :",newobj.calculate_lot_size(15,75,30))
