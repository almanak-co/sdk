"""Generic portfolio models for multi-provider wallet valuation.

These dataclasses define the provider-agnostic contract for portfolio data.
All portfolio providers (Zerion, Moralis, etc.) normalize their responses
into these shapes so framework and dashboard code never depend on
vendor-specific types.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass
class WalletPosition:
    """Normalized wallet position from any portfolio provider."""

    position_id: str
    protocol: str
    label: str
    position_type: str
    value_usd: str
    pool_address: str = ""
    token_symbols: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class WalletPortfolioSnapshot:
    """Normalized wallet portfolio payload from any provider."""

    provider: str
    wallet_address: str
    chain: str
    total_value_usd: str
    positions: list[WalletPosition] = field(default_factory=list)
    cache_hit: bool = False
    fetched_at: datetime = field(default_factory=lambda: datetime.now(UTC))
