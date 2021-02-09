class super_intelligence_devices:
    def __init__(self,devicetype):
        self.devicetype=devicetype


class computers(super_intelligence_devices):
    def __init__(self,devicetype,config):
        super_intelligence_devices.__init__(self,devicetype)
        self.config=config
    def computation(self):
        print("Your {} of {} config does fast processing".format(self.devicetype,self.config))


c1=computers("Laptop","Core i7")
c1.computation()