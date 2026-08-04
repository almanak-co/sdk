"""Typed Chainlink feed metadata."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

# Preserve the long-standing wire label while centralizing its ownership. The
# package/factory is named ``chainlink``; PriceResult.source remains ``onchain``
# for downstream dashboards and accounting consumers.
CHAINLINK_SOURCE_NAME = "onchain"


class FeedKind(StrEnum):
    """Quote denomination of a Chainlink feed."""

    USD = "usd"
    ETH = "eth"


@dataclass(frozen=True)
class FeedSpec:
    """One Chainlink aggregator plus its decode and freshness policy."""

    chain: str
    chain_id: int
    pair: str
    address: str
    kind: FeedKind = FeedKind.USD
    decimals: int = 8
    heartbeat_seconds: int = 3600
    deviation_threshold_pct: Decimal = Decimal("1.0")

    def __post_init__(self) -> None:
        if self.chain != self.chain.strip().lower() or not self.chain:
            raise ValueError("FeedSpec.chain must be a canonical lowercase chain name")
        if "/" not in self.pair or self.pair != self.pair.upper():
            raise ValueError(f"FeedSpec.pair must be an uppercase BASE/QUOTE pair, got {self.pair!r}")
        if self.chain_id <= 0 or self.decimals < 0 or self.heartbeat_seconds <= 0:
            raise ValueError(f"Invalid Chainlink feed metadata for {self.chain}:{self.pair}")
