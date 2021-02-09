class water_heaters:
    def __init__(self,instrumenttype):
        self.instrumenttype=instrumenttype


class geysers:
    def __init__(self,instumenttype,workmode):
        water_heaters.__init__(self,instumenttype)
        self.workmode=workmode
    def heating(self):
        print("{} help in {} the water".format(self.instrumenttype,self.workmode))

g1=geysers("Geysers","heating")
g1.heating()
