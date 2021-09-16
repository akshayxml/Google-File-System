import os
import sys
import time
import grpc
import gfs_pb2
import gfs_pb2_grpc
from common import Status
from concurrent import futures
from common import Config as cfg
from multiprocessing import Pool, Process

class ChunkServer(object):
    def __init__(self, port, root):
        self.port = port
        self.root = root
        if not os.path.isdir(root):
            os.mkdir(root)

    def create(self, chunkHandle):
        try:
            open(os.path.join(self.root, chunkHandle), 'w').close()
        except Exception as e:
            return Status(-1, "ERROR :" + str(e))
        else:
            return Status(0, "SUCCESS: chunk created")

    def append(self, chunkHandle, data):
        try:
            with open(os.path.join(self.root, chunkHandle), "a") as f:
                f.write(data)
        except Exception as e:
            return Status(-1, "ERROR: " + str(e))
        else:
            return Status(0, "SUCCESS: data appended")

    def read(self, chunkHandle, startOffset, numbytes):
        startOffset = int(startOffset)
        numbytes = int(numbytes)
        try:
            with open(os.path.join(self.root, chunkHandle), "r") as f:
                f.seek(startOffset)
                ret = f.read(numbytes)
        except Exception as e:
            return  Status(-1, "ERROR: " + str(e))
        else:
            return Status(0, ret)


class ChunkServerServicer(gfs_pb2_grpc.ChunkServerServicer):
    def __init__(self, ckser):
        self.ckser = ckser
        self.port = self.ckser.port

    def Create(self, request, context):
        chunkHandle = request.st
        print("{} CreateChunk {}".format(self.port, chunkHandle))
        status = self.ckser.create(chunkHandle)
        return gfs_pb2.String(st=status.e)

    def Append(self, request, context):
        chunkHandle, data = request.st.split("|")
        print("{} Append {} {}".format(self.port, chunkHandle, data))
        status = self.ckser.append(chunkHandle, data)
        return gfs_pb2.String(st=status.e)

    def Read(self, request, context):
        chunkHandle, startOffset, numbytes = request.st.split("|")
        print("{} Read {} {}".format(chunkHandle, startOffset, numbytes))
        status = self.ckser.read(chunkHandle, startOffset, numbytes)
        return gfs_pb2.String(st=status.e)

def start(port):
    print("Starting Chunk server on {}".format(port))
    if not os.path.exists(os.path.join(cfg.rootDir, port)):
        os.makedirs(os.path.join(cfg.rootDir, port))
    ckser = ChunkServer(port=port, root=os.path.join(cfg.rootDir, port))

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    gfs_pb2_grpc.add_ChunkServerServicer_to_server(ChunkServerServicer(ckser), server)
    server.add_insecure_port("[::]:{}".format(port))
    server.start()
    try:
        while True:
            time.sleep(200000)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":

    
    for loc in cfg.chunkserverLocs:
        p = Process(target=start, args=(loc,))
        p.start()
    p.join()
