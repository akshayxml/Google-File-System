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

    def create(self, chunk_handle):
        try:
            open(os.path.join(self.root, chunk_handle), 'w').close()
        except Exception as e:
            return Status(-1, "ERROR :" + str(e))
        else:
            return Status(0, "SUCCESS: chunk created")

    def append(self, chunk_handle, data):
        try:
            with open(os.path.join(self.root, chunk_handle), "a") as f:
                f.write(data)
        except Exception as e:
            return Status(-1, "ERROR: " + str(e))
        else:
            return Status(0, "SUCCESS: data appended")

    def read(self, chunk_handle, start_offset, numbytes):
        start_offset = int(start_offset)
        numbytes = int(numbytes)
        try:
            with open(os.path.join(self.root, chunk_handle), "r") as f:
                f.seek(start_offset)
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
        chunk_handle = request.st
        print("{} CreateChunk {}".format(self.port, chunk_handle))
        status = self.ckser.create(chunk_handle)
        return gfs_pb2.String(st=status.e)

    def Append(self, request, context):
        chunk_handle, data = request.st.split("|")
        print("{} Append {} {}".format(self.port, chunk_handle, data))
        status = self.ckser.append(chunk_handle, data)
        return gfs_pb2.String(st=status.e)

    def Read(self, request, context):
        chunk_handle, start_offset, numbytes = request.st.split("|")
        print("{} Read {} {}".format(chunk_handle, start_offset, numbytes))
        status = self.ckser.read(chunk_handle, start_offset, numbytes)
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
