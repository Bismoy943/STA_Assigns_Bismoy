class primates:
    def __init__(self,mammaltype):
        self.mammaltype=mammaltype




class apes(primates):
    def __init__(self,mammaltype,build):
        primates.__init__(self,mammaltype)
        self.build=build
    def communicate(self):
        print(str(self.mammaltype)+" can communicate and they have "+self.build+" build")

apes1=apes("Apes","decent")
apes1.communicate()