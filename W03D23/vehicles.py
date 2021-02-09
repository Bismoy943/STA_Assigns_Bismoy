class vehicles:
    def __init__(self,vehicletype):
        self.vehicletype=vehicletype

    def manufacturer(self,manufacturer):
        print("This "+str(self.vehicletype)+" vehicle is made by "+manufacturer)


class cars(vehicles):

    
    def __init__(self,vehicletype,carname):
        vehicles.__init__(self,vehicletype)
        self.carname=carname
    def accelerate(self):
        print("Please press accelerator in your "+str(self.carname))
    def breaking(self):
        print("Please press break in your "+str(self.carname))
    def clutching(self):
        print("Please press clutch in your "+str(self.carname))

car1=cars("4 wheeler","Audi SX6")
car1.manufacturer("Audi")
car1.accelerate()
car1.breaking()
car1.clutching()

