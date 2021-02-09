class heavy_machinery:
    def __init__(self,type):
        self.type=type

class bulldozers(heavy_machinery):
    def __init__(self,type,variant):
        heavy_machinery.__init__(self,type)
        self.variant=variant
    def digging(self):
        print("{} can do shallow digging and ditching".format(self.variant))


b1=bulldozers("Heavy","Bulldozers")
b1.digging()
