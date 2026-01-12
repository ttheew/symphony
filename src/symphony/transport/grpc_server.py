import grpc

from symphony.conductor.service import ConductorService
from symphony.v1 import protocol_pb2_grpc


def create_grpc_server() -> tuple[grpc.aio.Server, ConductorService]:
    server = grpc.aio.server(
        options=[
            ("grpc.keepalive_time_ms", 20000),
            ("grpc.keepalive_timeout_ms", 5000),
        ]
    )

    protocol_pb2_grpc.add_ConductorServiceServicer_to_server(ConductorService(), server)
    return server
