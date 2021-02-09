class fluid_conveyance_devices:
    def __init__(self,devicetype):
        self.devicetype=devicetype


class pipes(fluid_conveyance_devices):
    def __init__(self,devicetype):
        fluid_conveyance_devices.__init__(self,devicetype)
    def fluidtransfer(self):
        print("{} facilitate transfer of fluid via them".format(self.devicetype))


p1=pipes("Pipes")
p1.fluidtransfer()






