class Config(object):
    chunkSize = 4
    masterLoc = "50051"
    chunkserverLocs = ["50052", "50053", "50054", "50055", "50056"]
    rootDir = "root"

class Status(object):
    def __init__(self, v, e):
        self.v = v
        self.e = e
        if self.e:
            print(self.e)