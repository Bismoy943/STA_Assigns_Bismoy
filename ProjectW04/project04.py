import math
class PositionSizer:
    def __init__(self,total_equity):
        self.total_equity=total_equity
    def calculate_lot_size(self,percentage_risk,lot_units,slpoints):
        risk_amount=(percentage_risk*self.total_equity)/100
        risk_per_lot=slpoints*lot_units
        if risk_amount<risk_per_lot:
            return 0
        else:
            return math.floor(risk_amount/risk_per_lot)


