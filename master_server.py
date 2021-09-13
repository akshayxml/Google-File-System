import os
import time
import uuid
import grpc
import random
import gfs_pb2
import gfs_pb2_grpc
from common import Status
from concurrent import futures
from common import Config as cfg
from collections import OrderedDict


class loadBalancer():
    def __init__(self):
        self.locs = cfg.chunkserverLocs
        self.csLoad = {}
        for cs in self.locs:
            self.csLoad[cs] = 0
        
    def getCs(self):
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

class MetaData(object):
    def __init__(self):
        self.locs = cfg.chunkserverLocs
        self.loadBalancer = loadBalancer()
        self.files = {}
        self.ch2fp = {}
        self.to_delete = set()

    def getLatestChunk(self, filePath):
        latestChunkHandle = list(self.files[filePath].chunks.keys())[-1]
        return latestChunkHandle

    def getChunkLocs(self, chunkHandle):
        filePath = self.ch2fp[chunkHandle]
        return self.files[filePath].chunks[chunkHandle].locs

    def create_new_file(self, filePath, chunkHandle):
        if filePath in self.files:
            return Status(-1, "ERROR: File exists already: {}".format(filePath))
        fl = File(filePath)
        self.files[filePath] = fl
        status = self.create_new_chunk(filePath, -1, chunkHandle)
        return status

    def create_new_chunk(self, filePath, prevChunkHandle, chunkHandle):
        if filePath not in self.files:
            return Status(-2, "ERROR: New chunk file doesn't exist: {}".format(filePath))

        latest_chunk = None
        if prevChunkHandle != -1:
            latest_chunk = self.getLatestChunk(filePath)

        # already created
        if prevChunkHandle != -1 and latest_chunk != prevChunkHandle:
            return Status(-3, "ERROR: New chunk already created: {} : {}".format(filePath, chunkHandle))

        chunk = Chunk()
        self.files[filePath].chunks[chunkHandle] = chunk
        locs = self.loadBalancer.getCs()

        for loc in locs:
            self.files[filePath].chunks[chunkHandle].locs.append(loc)

        self.ch2fp[chunkHandle] = filePath
        return Status(0, "New Chunk Created")

    def deleteFile(self, filePath):
        chunksToDel = []

        for handle, chunk in self.files[filePath].chunks.items():
            for loc in chunk.locs:
                self.loadBalancer.decreaseLoad(loc)
                os.remove(os.path.join(cfg.rootDir, os.path.join(loc, handle)))
            chunksToDel.append(chunk)

        for chunk in chunksToDel:
            del chunk
        del self.files[filePath]

class MasterServer(object):
    def __init__(self):
        self.file_list = ["/file1", "/file2", "/dir1/file3"]
        self.meta = MetaData()

    def get_chunkHandle(self):
        return str(uuid.uuid1())

    def get_available_chunkserver(self):
        return random.choice(self.chunkservers)

    def check_valid_file(self, filePath):
        if filePath not in self.meta.files:
            return Status(-1, "ERROR: file {} doesn't exist".format(filePath))
        else:
            return Status(0, "SUCCESS: file {} exists and not yet deleted".format(filePath))

    def list_files(self, filePath):
        file_list = []
        for fp in self.meta.files.keys():
            if fp.startswith(filePath):
                file_list.append(fp)
        return file_list

    def createFile(self, filePath):
        chunkHandle = self.get_chunkHandle()
        status = self.meta.create_new_file(filePath, chunkHandle)

        if status.v != 0:
            return None, None, status

        locs = self.meta.files[filePath].chunks[chunkHandle].locs
        return chunkHandle, locs, status

    def appendFile(self, filePath):
        status = self.check_valid_file(filePath)
        if status.v != 0:
            return None, None, status

        latestChunkHandle = self.meta.getLatestChunk(filePath)
        locs = self.meta.getChunkLocs(latestChunkHandle)
        status = Status(0, "Append handled")
        return latestChunkHandle, locs, status

    def createChunk(self, filePath, prevChunkHandle):
        chunkHandle = self.get_chunkHandle()
        status = self.meta.create_new_chunk(filePath, prevChunkHandle, chunkHandle)
        # TODO: check status
        locs = self.meta.files[filePath].chunks[chunkHandle].locs
        return chunkHandle, locs, status
    
    def getChunkSpace(self, filePath):
        try:
            latestChunkHandle = self.meta.getLatestChunk(filePath)
            path = os.path.join(cfg.rootDir, self.meta.files[filePath].chunks[latestChunkHandle].locs[0])
            chunkSpace = cfg.chunk_size - os.stat(os.path.join(path, latestChunkHandle)).st_size
        except Exception as e:
            return None, Status(-1, "ERROR: " + str(e))
        else:
            return str(chunkSpace), Status(0, "")

    def read_file(self, filePath, offset, numbytes):
        status = self.check_valid_file(filePath)
        if status.v != 0:
            return status

        chunk_size = cfg.chunk_size
        start_chunk = offset // chunk_size
        all_chunks = list(self.meta.files[filePath].chunks.keys())
        if start_chunk > len(all_chunks):
            return Status(-1, "ERROR: Offset is too large")

        start_offset = offset % chunk_size

        if numbytes == -1:
            end_offset = chunk_size
            end_chunk = len(all_chunks) - 1
        else:
            end_offset = offset + numbytes - 1
            end_chunk = end_offset // chunk_size
            end_offset = end_offset % chunk_size

        all_chunkHandles = all_chunks[start_chunk:end_chunk+1]
        ret = []
        for idx, chunkHandle in enumerate(all_chunkHandles):
            if idx == 0:
                stof = start_offset
            else:
                stof = 0
            if idx == len(all_chunkHandles) - 1:
                enof = end_offset
            else:
                enof = chunk_size - 1
            loc = self.meta.files[filePath].chunks[chunkHandle].locs[0]
            ret.append(chunkHandle + "*" + loc + "*" + str(stof) + "*" + str(enof - stof + 1))
        ret = "|".join(ret)
        return Status(0, ret)

    def deleteFile(self, filePath):
        status = self.check_valid_file(filePath)
        if status.v != 0:
            return status

        try:
            self.meta.deleteFile(filePath)
        except Exception as e:
            return Status(-1, "ERROR: " + str(e))
        else:
            return Status(0, "SUCCESS: file {} is deleted".format(filePath))

class MasterServicer(gfs_pb2_grpc.MasterServicer):
    def __init__(self, master):
        self.master = master

    def ListFiles(self, request, context):
        filePath = request.st
        print("Command List {}".format(filePath))
        fpls = self.master.list_files(filePath)
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
        # TODO: check status
        st = chunkHandle + "|" + "|".join(locs)
        return gfs_pb2.String(st=st)

    def ReadFile(self, request, context):
        filePath, offset, numbytes = request.st.split("|")
        print("Command ReadFile {} {} {}".format(filePath, offset, numbytes))
        status = self.master.read_file(filePath, int(offset), int(numbytes))
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
    server.add_insecure_port('[::]:'+cfg.masterLoc)
    server.start()
    print('Master serving at '+cfg.masterLoc)
    try:
        while True:
            time.sleep(2000)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    serve()
