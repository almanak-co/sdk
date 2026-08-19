"""Provider boundary for data measured from a verified exact venue."""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from almanak.framework.data.interfaces import OHLCVCandle
from almanak.framework.data.timeframes import OHLCVTimeframe

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


@dataclass(frozen=True, slots=True)
class GatewayExactOhlcvResponse:
    """Gateway candle page with all identity echoes needed by an exact reader."""

    candles: tuple[OHLCVCandle, ...]
    chain: str
    pool_address: str
    timeframe: OHLCVTimeframe
    start_ts: int
    end_ts: int
    binding_hash: str
    feature_identity: str
    base_token_address: str
    quote_token_address: str
    source: str
    observed_at: datetime


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

    def exact_pool_ohlcv(
        self,
        *,
        chain: str,
        pool_address: str,
        base_token_address: str,
        quote_token_address: str,
        timeframe: OHLCVTimeframe,
        start_ts: int,
        end_ts: int,
        binding_hash: str,
        feature_identity: str,
    ) -> GatewayExactOhlcvResponse: ...


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
    "GatewayExactOhlcvResponse",
]
