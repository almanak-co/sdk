from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.cli.run import create_sync_price_oracle_func
from almanak.framework.data.interfaces import PriceResult


def _price_result(price: str) -> PriceResult:
    return PriceResult(
        price=Decimal(price),
        source="test",
        timestamp=datetime.now(UTC),
        confidence=1.0,
        stale=False,
    )


class _ChainAwareOracle:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str | None]] = []

    async def get_aggregated_price(
        self,
        token: str,
        quote: str = "USD",
        *,
        chain: str | None = None,
    ) -> PriceResult:
        self.calls.append((token, quote, chain))
        return _price_result("2500.50")


class _LegacyOracle:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    async def get_aggregated_price(self, token: str, quote: str = "USD") -> PriceResult:
        self.calls.append((token, quote))
        return _price_result("1.01")


class _BuggyChainAwareOracle:
    async def get_aggregated_price(
        self,
        token: str,
        quote: str = "USD",
        *,
        chain: str | None = None,
    ) -> PriceResult:
        raise TypeError(f"internal failure for {token}@{chain}")


def test_sync_wrapper_passes_chain_to_chain_aware_oracle() -> None:
    oracle = _ChainAwareOracle()

    sync_price = create_sync_price_oracle_func(oracle)

    assert sync_price("ETH", "USD", "base") == Decimal("2500.50")
    assert oracle.calls == [("ETH", "USD", "base")]


def test_sync_wrapper_falls_back_to_legacy_oracle_signature() -> None:
    oracle = _LegacyOracle()

    sync_price = create_sync_price_oracle_func(oracle)

    assert sync_price("USDC", "USD", "arbitrum") == Decimal("1.01")
    assert oracle.calls == [("USDC", "USD")]


def test_sync_wrapper_does_not_mask_internal_type_errors() -> None:
    sync_price = create_sync_price_oracle_func(_BuggyChainAwareOracle())

    with pytest.raises(TypeError, match="internal failure for ETH@arbitrum"):
        sync_price("ETH", "USD", "arbitrum")
