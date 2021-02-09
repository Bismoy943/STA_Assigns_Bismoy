class compact_computers:
    def __init__(self,type):
        self.type=type
class smartphones(compact_computers):
    def __init__(self,type,model):
        compact_computers.__init__(self,type)
        self.model=model
    def streaming(self):
        print("You can do live stream using your {} {}".format(self.model,self.type))


s1=smartphones("tablet","note10")
s1.streaming()