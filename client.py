import os
import sys

import grpc
import gfs_pb2_grpc
import gfs_pb2

from common import Config as cfg
from common import isint

class Client():

    def __init__(self):
        masterChannel = grpc.insecure_channel("127.0.0.1:"+str(cfg.master_loc))
        self.masterStub = gfs_pb2_grpc.MasterStub(masterChannel)

    def createFile(self,filePath):
        request = gfs_pb2.String(st=filePath)
        masterResponse = self.masterStub.CreateFile(request).st
        print("Response from master: {}".format(masterResponse))

        if masterResponse.startswith("ERROR"):
            return -1

        data = masterResponse.split("|")
        chunkHandle = data[0]
        for loc in data[1:]:
            csAddress = "localhost:{}".format(loc)
            channel = grpc.insecure_channel(csAddress)
            stub = gfs_pb2_grpc.ChunkServerStub(channel)
            request = gfs_pb2.String(st=chunkHandle)
            response = stub.Create(request).st
            print("Response from chunkserver {} : {}".format(loc, response))

    def listFile(self,filePath):
        request = gfs_pb2.String(st=filePath)
        masterResponse = self.masterStub.ListFiles(request).st
        files = masterResponse.split("|")
        print(files)

    def appendFile(self,filePath, inputData):
        request = gfs_pb2.String(st=filePath)
        masterResponse = self.masterStub.AppendFile(request).st
        print("Response from master: {}".format(masterResponse))

        if masterResponse.startswith("ERROR"):
            return -1

        inputSize = len(inputData)
        data = masterResponse.split("|")
        chunkHandle = data[0]

        for loc in data[1:]:
            csAddress = "localhost:{}".format(loc)
            channel = grpc.insecure_channel(csAddress)
            stub = gfs_pb2_grpc.ChunkServerStub(channel)
            request = gfs_pb2.String(st=chunkHandle)
            response = stub.GetChunkSpace(request).st
            print("Response from chunkserver {} : {}".format(loc, response))

            if response.startswith("ERROR"):
                return -1

            spaceLeft = int(response)

            if spaceLeft >= inputSize:
                st = chunkHandle + "|" + inputData
                request = gfs_pb2.String(st=st)
                response = stub.Append(request).st
                print("Response from chunkserver {} : {}".format(loc, response))
            else:
                inp1, inp2 = inputData[:spaceLeft], inputData[spaceLeft:]
                st = chunkHandle + "|" + inp1
                request = gfs_pb2.String(st=st)
                response = stub.Append(request).st
                print("Response from chunkserver {} : {}".format(loc, response))

        if spaceLeft >= inputSize:
            return 0

        # if need to add more chunks then continue
        st = filePath + "|" + chunkHandle
        request = gfs_pb2.String(st=st)
        masterResponse = self.masterStub.CreateChunk(request).st
        print("Response from master: {}".format(masterResponse))

        data = masterResponse.split("|")
        chunkHandle = data[0]
        for loc in data[1:]:
            csAddress = "localhost:{}".format(loc)
            channel = grpc.insecure_channel(csAddress)
            stub = gfs_pb2_grpc.ChunkServerStub(channel)
            request = gfs_pb2.String(st=chunkHandle)
            response = stub.Create(request).st
            print("Response from chunkserver {} : {}".format(loc, response))

        self.appendFile(filePath, inp2)
        return 0

    def readFile(self,filePath, offset, numbytes):
        st = filePath + "|" + str(offset) + "|" + str(numbytes)
        request = gfs_pb2.String(st=st)
        masterResponse = self.masterStub.ReadFile(request).st
        print("Response from master: {}".format(masterResponse))

        if masterResponse.startswith("ERROR"):
            return -1

        file_content = ""
        data = masterResponse.split("|")
        for chunk_info in data:
            chunkHandle, loc, start_offset, numbytes = chunk_info.split("*")
            csAddress = "localhost:{}".format(loc)
            channel = grpc.insecure_channel(csAddress)
            stub = gfs_pb2_grpc.ChunkServerStub(channel)
            st = chunkHandle + "|" + start_offset + "|" + numbytes
            request = gfs_pb2.String(st=st)
            response = stub.Read(request).st
            print("Response from chunkserver {} : {}".format(loc, response))

            if response.startswith("ERROR"):
                return -1
            file_content += response

        print(file_content)

    def deleteFile(self,filePath):
        request = gfs_pb2.String(st=filePath)
        masterResponse = self.masterStub.DeleteFile(request).st
        print("Response from master: {}".format(masterResponse))

    def undeleteFile(self,filePath):
        request = gfs_pb2.String(st=filePath)
        masterResponse = self.masterStub.UndeleteFile(request).st
        print("Response from master: {}".format(masterResponse))


def run(cmd, client):
    cmd = cmd.split()

    if(len(cmd) == 1):
        if(cmd[0] == "exit"):
            sys.exit()
        else:
            print("[ERROR]: Invalid Command")
    else:
        filePath = cmd[1]
        if(cmd[0] == "create"):
            client.createFile(filePath)
        elif(cmd[0] == "list"):
            client.listFile(filePath)
        elif(cmd[0] == "append"):
            if(len(cmd) == 2):
                print("[ERROR]: No input data given to append")
            else:
                client.appendFile(filePath, cmd[2])
        elif(cmd[0] == "read"):
            if(len(cmd) <= 3 or not isint(cmd[2]) or not isint(cmd[3])):
                print("[ERROR]: Read command usage: read <filePath> <offset> <len>")
            else:
                client.readFile(filePath, int(cmd[2]), int(cmd[3]))
        elif(cmd[0] == "delete"):
            client.deleteFile(filePath)
        elif(cmd[0] == "undelete"):
            client.undeleteFile(filePath)
        else:
            print("Invalid Command")

if __name__ == "__main__":

    client = Client()
    print('Enter command')

    while(True):
        cmd = input()

        if(len(cmd)):
           run(cmd, client)
