"""gRPC framework utilities (typed-error contract, status-details codec)."""

from almanak.framework.grpc.error_details import (
    StatusDetails,
    pack_status_details,
    set_grpc_error,
    unpack_status_details,
)

__all__ = [
    "StatusDetails",
    "pack_status_details",
    "set_grpc_error",
    "unpack_status_details",
]
