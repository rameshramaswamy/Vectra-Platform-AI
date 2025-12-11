# scripts/compile_proto.py
import grpc_tools.protoc
import os

def compile():
    # Helper to compile proto files to python classes
    proto_dir = "./services/common/proto"
    out_dir = "./services/common/python"
    
    command = [
        "grpc_tools.protoc",
        f"-I{proto_dir}",
        f"--python_out={out_dir}",
        f"{proto_dir}/telemetry.proto"
    ]
    
    if grpc_tools.protoc.main(command) != 0:
        print("Error: Protobuf compilation failed.")
    else:
        print("Success: Proto classes generated.")

if __name__ == "__main__":
    compile()