import os
import sys
import time
import grpc
import gfs_pb2
import gfs_pb2_grpc
from concurrent import futures
from util import Status, Config
from multiprocessing import Pool, Process

class ChunkServer(object):
    def __init__(self, port, root):
        self.port = port
        self.root = root

    def create(self, chunkHandle):
        try:
            open(os.path.join(self.root, chunkHandle), 'w').close()
            return Status(0, "SUCCESS: chunk created")
        except Exception as e:
            return Status(-1, "ERROR :" + str(e))            

    def append(self, chunkHandle, data):
        try:
            with open(os.path.join(self.root, chunkHandle), "a") as f:
                f.write(data)
            return Status(0, "SUCCESS: data appended")
        except Exception as e:
            return Status(-1, "ERROR: " + str(e))            

    def read(self, chunkHandle, startOffset, numbytes):
        startOffset = int(startOffset)
        numbytes = int(numbytes)
        try:
            with open(os.path.join(self.root, chunkHandle), "r") as f:
                f.seek(startOffset)
                ret = f.read(numbytes)
            return Status(0, ret)
        except Exception as e:
            return  Status(-1, "ERROR: " + str(e))

class ChunkServerServicer(gfs_pb2_grpc.ChunkServerServicer):
    def __init__(self, chunkServer):
        self.chunkServer = chunkServer
        
    def Create(self, request, context):
        chunkHandle = request.st
        print("{} CreateChunk {}".format(self.chunkServer.port, chunkHandle))
        status = self.chunkServer.create(chunkHandle)
        return gfs_pb2.String(st=status.e)

    def Append(self, request, context):
        chunkHandle, data = request.st.split("|")
        print("{} Append {} {}".format(self.chunkServer.port, chunkHandle, data))
        status = self.chunkServer.append(chunkHandle, data)
        return gfs_pb2.String(st=status.e)

    def Read(self, request, context):
        chunkHandle, startOffset, numbytes = request.st.split("|")
        print("{} Read {} {}".format(chunkHandle, startOffset, numbytes))
        status = self.chunkServer.read(chunkHandle, startOffset, numbytes)
        return gfs_pb2.String(st=status.e)

def start(port):
    print("Starting Chunk server on {}".format(port))
    root = os.path.join(Config.rootDir, port)
    if not os.path.exists(root):
        os.makedirs(root)
    chunkServer = ChunkServer(port=port, root=root)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    gfs_pb2_grpc.add_ChunkServerServicer_to_server(ChunkServerServicer(chunkServer), server)
    server.add_insecure_port("[::]:{}".format(port))
    server.start()
    try:
        while True:
            time.sleep(2000)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":

    for port in Config.chunkserverLocs:
        p = Process(target=start, args=(port,))
        p.start()
    p.join()
