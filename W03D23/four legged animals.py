class four_legged_animals:
    def __init__(self,leg):
        self.leg=leg
class dogs(four_legged_animals):
    def __init__(self,leg,animaltype):
        four_legged_animals.__init__(self,leg)
        self.animaltype=animaltype
    def bark(self):
        print("{} is a {} legged animal and it will bark".format(self.animaltype,self.leg))

d1=dogs(4,"Dog")
d1.bark()
