.PHONY: protobuf

firmament/firmament_pb2.py: firmament/firmament.proto
	protoc --proto_path=firmament --python_out=firmament --pyi_out=firmament firmament/firmament.proto

protobuf: firmament/firmament_pb2.py
