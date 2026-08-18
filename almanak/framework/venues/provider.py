"""Provider boundary for data measured from a verified exact venue."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Protocol

from .data import ExactVenueDataResult, ExactVenueFeatureRequest


@dataclass(frozen=True, slots=True)
class GatewayBlockIdentity:
    """Block identity returned by the generic gateway RPC seam."""

    number: int
    block_hash: str
    timestamp: int

    def __post_init__(self) -> None:
        if type(self.number) is not int or self.number <= 0:
            raise ValueError("GatewayBlockIdentity.number must be a positive integer")
        if type(self.timestamp) is not int or self.timestamp <= 0:
            raise ValueError("GatewayBlockIdentity.timestamp must be a positive integer")
        if type(self.block_hash) is not str or re.fullmatch(r"0x[0-9a-f]{64}", self.block_hash) is None:
            raise ValueError("GatewayBlockIdentity.block_hash must be canonical lowercase bytes32")


class ExactVenueDataGateway(Protocol):
    """Gateway-only read seam available to exact venue data providers."""

    def read(
        self,
        *,
        chain: str,
        target_address: str,
        payload: bytes,
        block_number: int,
    ) -> bytes: ...

    def block_identity(self, *, chain: str, block_number: int) -> GatewayBlockIdentity: ...


class BaseExactVenueDataProvider(ABC):
    """Connector-declared provider that cannot return an unbound observation."""

    @abstractmethod
    def observe(
        self,
        request: ExactVenueFeatureRequest,
        gateway: ExactVenueDataGateway,
    ) -> ExactVenueDataResult:
        """Measure the exact request or return a typed closed failure."""


__all__ = [
    "BaseExactVenueDataProvider",
    "ExactVenueDataGateway",
    "GatewayBlockIdentity",
]
