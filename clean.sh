rm -rf root
rm -rf __pycache__
rm gfs_pb2.py
rm gfs_pb2_grpc.py
python -m grpc_tools.protoc -I./ --python_out=. --grpc_python_out=. ./gfs.proto