import os
import sys
import grpc
import gfs_pb2
import gfs_pb2_grpc
from common import Config as cfg

class Client():
    def __init__(self):
        masterChannel = grpc.insecure_channel("127.0.0.1:"+str(cfg.masterLoc))
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
            csAddress = "127.0.0.1:{}".format(loc)
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

    def appendFile(self, filePath, inputData):
        request = gfs_pb2.String(st=filePath)
        masterResponse = self.masterStub.AppendFile(request).st
        print("Response from master: {}".format(masterResponse))

        if masterResponse.startswith("ERROR"):
            return -1

        inputSize = len(inputData)
        data = masterResponse.split("|")
        spaceLeft = int(data[0])
        chunkHandle = data[1]

        curInput = inputData
        if spaceLeft < inputSize:
            curInput, remInput = inputData[:spaceLeft], inputData[spaceLeft:]

        for loc in data[2:]:
            csAddress = "127.0.0.1:{}".format(loc)
            channel = grpc.insecure_channel(csAddress)
            stub = gfs_pb2_grpc.ChunkServerStub(channel)
            st = chunkHandle + "|" + curInput
            request = gfs_pb2.String(st=st)
            response = stub.Append(request).st
            print("Response from chunkserver {} : {}".format(loc, response))

        if spaceLeft >= inputSize:
            return 0

        st = filePath + "|" + chunkHandle
        request = gfs_pb2.String(st=st)
        masterResponse = self.masterStub.CreateChunk(request).st
        print("Response from master: {}".format(masterResponse))

        data = masterResponse.split("|")
        chunkHandle = data[0]
        for loc in data[1:]:
            csAddress = "127.0.0.1:{}".format(loc)
            channel = grpc.insecure_channel(csAddress)
            stub = gfs_pb2_grpc.ChunkServerStub(channel)
            request = gfs_pb2.String(st=chunkHandle)
            response = stub.Create(request).st
            print("Response from chunkserver {} : {}".format(loc, response))

        return self.appendFile(filePath, remInput)

    def readFile(self, filePath, offset, numbytes):
        st = filePath + "|" + str(offset) + "|" + str(numbytes)
        request = gfs_pb2.String(st=st)
        masterResponse = self.masterStub.ReadFile(request).st
        print("Response from master: {}".format(masterResponse))

        if masterResponse.startswith("ERROR"):
            return -1

        fileContent = ""
        data = masterResponse.split("|")
        for chunkInfo in data:
            chunkHandle, loc, offset, numbytes = chunkInfo.split("*")
            csAddress = "127.0.0.1:{}".format(loc)
            channel = grpc.insecure_channel(csAddress)
            stub = gfs_pb2_grpc.ChunkServerStub(channel)
            st = chunkHandle + "|" + offset + "|" + numbytes
            request = gfs_pb2.String(st=st)
            response = stub.Read(request).st
            print("Response from chunkserver {} : {}".format(loc, response))

            if response.startswith("ERROR"):
                return -1
            fileContent += response

        print(fileContent)

    def deleteFile(self,filePath):
        request = gfs_pb2.String(st=filePath)
        masterResponse = self.masterStub.DeleteFile(request).st
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
            offset = 0
            length = -1
            if(len(cmd) >= 3):
                offset = int(cmd[2])
            if(len(cmd) >= 4):
                length = int(cmd[3])
            client.readFile(filePath, offset, length)
        elif(cmd[0] == "delete"):
            while(True):
                print("Are you sure you want to delete this file - {}? (Y/N)".format(cmd[1]))
                confirm = input()
                if(confirm.lower() == 'n'):
                    return
                elif(confirm.lower() == 'y'):
                    client.deleteFile(filePath)
                    return
        else:
            print("[ERROR]: Invalid Command")

if __name__ == "__main__":

    client = Client()
    print('Enter command')

    while(True):
        cmd = input()

        if(len(cmd)):
           run(cmd, client)
