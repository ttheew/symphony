import grpc

from symphony.config import TlsConfig
from symphony.transport.security import create_secure_channel


def create_channel(addr: str, tls: TlsConfig | None = None) -> grpc.aio.Channel:
    """
    Create a gRPC channel to the conductor.
    """
    return create_secure_channel(addr, tls)
