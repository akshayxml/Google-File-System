import os
import time
import uuid
import grpc
import random
import gfs_pb2
import gfs_pb2_grpc
from common import Status
from concurrent import futures
from common import Config
from collections import OrderedDict

class loadBalancer():
    def __init__(self):
        self.locs = Config.chunkserverLocs
        self.csLoad = {}
        for cs in self.locs:
            self.csLoad[cs] = 0
        
    def getLocs(self):
        locs = []
        for loc in sorted(self.csLoad.items(), key=lambda x: x[1]):
            self.csLoad[loc[0]] += 1
            locs.append(loc[0])
            if(len(locs) == 3):
                return locs
        return locs
    
    def decreaseLoad(self, loc):
        self.csLoad[loc] -= 1
    
class Chunk(object):
    def __init__(self):
        self.locs = []

class File(object):
    def __init__(self, filePath):
        self.filePath = filePath
        self.chunks = OrderedDict()

class MasterServer(object):
    def __init__(self):
        self.files = {}
        self.locs = Config.chunkserverLocs
        self.loadBalancer = loadBalancer()

    def getLatestChunk(self, filePath):
        latestChunkHandle = list(self.files[filePath].chunks.keys())[-1]
        return latestChunkHandle

    def getChunkLocs(self, chunkHandle, filePath):
        return self.files[filePath].chunks[chunkHandle].locs

    def getChunkHandle(self):
        return str(uuid.uuid1())

    def listFiles(self, filePath):
        fileList = []
        for fp in self.files.keys():
            if fp.startswith(filePath):
                fileList.append(fp)
        return fileList

    def createFile(self, filePath):
        if filePath in self.files:
            return Status(-1, "ERROR: File exists already: {}".format(filePath))

        self.files[filePath] = File(filePath)
        return self.createChunk(filePath, -1)

    def appendFile(self, filePath):
        if filePath not in self.files:
            return None, None, Status(-1, "ERROR: file {} doesn't exist".format(filePath))

        latestChunkHandle = self.getLatestChunk(filePath)
        locs = self.getChunkLocs(latestChunkHandle, filePath)
        status = Status(0, "Success")
        return latestChunkHandle, locs, status
    
    def createChunk(self, filePath, prevChunkHandle):
        if filePath not in self.files:
            return Status(-2, "ERROR: New chunk file doesn't exist: {}".format(filePath))

        latestChunk = None if prevChunkHandle == -1 else self.getLatestChunk(filePath)
        
        if prevChunkHandle != -1 and latestChunk != prevChunkHandle:
            return Status(-3, "ERROR: New chunk already created: {} : {}".format(filePath, chunkHandle))

        chunkHandle = self.getChunkHandle()
        self.files[filePath].chunks[chunkHandle] = Chunk()

        locs = self.loadBalancer.getLocs()
        for loc in locs:
            self.files[filePath].chunks[chunkHandle].locs.append(loc)

        return chunkHandle, locs, Status(0, "New Chunk Created")
    
    def getChunkSpace(self, filePath):
        try:
            latestChunkHandle = self.getLatestChunk(filePath)
            path = os.path.join(Config.rootDir, self.files[filePath].chunks[latestChunkHandle].locs[0])
            chunkSpace = Config.chunkSize - os.stat(os.path.join(path, latestChunkHandle)).st_size
        except Exception as e:
            return None, Status(-1, "ERROR: " + str(e))
        else:
            return str(chunkSpace), Status(0, "")

    def readFile(self, filePath, offset, numbytes):
        if filePath not in self.files:
            return Status(-1, "ERROR: file {} doesn't exist".format(filePath))

        startChunk = offset // Config.chunkSize
        allChunks = list(self.files[filePath].chunks.keys())
        print(allChunks)
        if startChunk > len(allChunks):
            return Status(-1, "ERROR: Offset is too large")

        startOffset = offset % Config.chunkSize

        if numbytes == -1:
            endOffset = Config.chunkSize
            endChunk = len(allChunks) - 1
        else:
            endOffset = offset + numbytes - 1
            endChunk = endOffset // Config.chunkSize
            endOffset = endOffset % Config.chunkSize

        allChunkHandles = allChunks[startChunk:endChunk+1]
        ret = []
        for idx, chunkHandle in enumerate(allChunkHandles):
            if idx == 0:
                stof = startOffset
            else:
                stof = 0
            if idx == len(allChunkHandles) - 1:
                enof = endOffset
            else:
                enof = Config.chunkSize - 1
            loc = self.files[filePath].chunks[chunkHandle].locs[0]
            ret.append(chunkHandle + "*" + loc + "*" + str(stof) + "*" + str(enof - stof + 1))
        ret = "|".join(ret)
        return Status(0, ret)

    def deleteFile(self, filePath):
        if filePath not in self.files:
            return Status(-1, "ERROR: file {} doesn't exist".format(filePath))

        chunksToDel = []
        for handle, chunk in self.files[filePath].chunks.items():
            for loc in chunk.locs:
                self.loadBalancer.decreaseLoad(loc)
                os.remove(os.path.join(Config.rootDir, os.path.join(loc, handle)))
            chunksToDel.append(chunk)

        for chunk in chunksToDel:
            del chunk
        del self.files[filePath]

        return Status(0, "SUCCESS: file {} is deleted".format(filePath))

class MasterServicer(gfs_pb2_grpc.MasterServicer):
    def __init__(self, master):
        self.master = master

    def ListFiles(self, request, context):
        filePath = request.st
        print("Command List {}".format(filePath))
        fpls = self.master.listFiles(filePath)
        st = "|".join(fpls)
        return gfs_pb2.String(st=st)

    def CreateFile(self, request, context):
        filePath = request.st
        print("Command Create {}".format(filePath))
        chunkHandle, locs, status = self.master.createFile(filePath)

        if status.v != 0:
            return gfs_pb2.String(st=status.e)

        st = chunkHandle + "|" + "|".join(locs)
        return gfs_pb2.String(st=st)

    def AppendFile(self, request, context):
        filePath = request.st
        print("Command Append {}".format(filePath))
        latestChunkHandle, locs, status = self.master.appendFile(filePath)
        if status.v != 0:
            return gfs_pb2.String(st=status.e)
            
        chunkRemSpace, status = self.master.getChunkSpace(filePath)
        
        if status.v != 0:
            return gfs_pb2.String(st=status.e)

        st = chunkRemSpace + "|"+ latestChunkHandle + "|" + "|".join(locs)
        return gfs_pb2.String(st=st)

    def CreateChunk(self, request, context):
        filePath, prevChunkHandle = request.st.split("|")
        print("Command CreateChunk {} {}".format(filePath, prevChunkHandle))
        chunkHandle, locs, status = self.master.createChunk(filePath, prevChunkHandle)
        st = chunkHandle + "|" + "|".join(locs)
        return gfs_pb2.String(st=st)

    def ReadFile(self, request, context):
        filePath, offset, numbytes = request.st.split("|")
        print("Command ReadFile {} {} {}".format(filePath, offset, numbytes))
        status = self.master.readFile(filePath, int(offset), int(numbytes))
        return gfs_pb2.String(st=status.e)

    def DeleteFile(self, request, context):
        filePath = request.st
        print("Command Delete {}".format(filePath))
        status = self.master.deleteFile(filePath)
        return gfs_pb2.String(st=status.e)

def serve():
    master = MasterServer()

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    gfs_pb2_grpc.add_MasterServicer_to_server(MasterServicer(master=master), server)
    server.add_insecure_port('[::]:'+Config.masterLoc)
    server.start()
    print('Master serving at '+Config.masterLoc)
    try:
        while True:
            time.sleep(2000)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()
