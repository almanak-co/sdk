"""Trust and identity controls for MarketSnapshot.perp_mark_price."""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from almanak.framework.data.funding import FundingRate
from almanak.framework.market import MarketSnapshot, MarketSnapshotBuilder, PriceUnavailableError


def _rate(*, mark: str | None, is_live_data: bool) -> FundingRate:
    return FundingRate(
        venue="hyperliquid",
        market="ETH-USD",
        rate_hourly=Decimal("0.00001"),
        rate_8h=Decimal("0.00008"),
        rate_annualized=Decimal("0.0876"),
        mark_price=Decimal(mark) if mark is not None else None,
        is_live_data=is_live_data,
        timestamp=datetime.now(UTC),
    )


def _snapshot(rate: FundingRate) -> tuple[MarketSnapshot, AsyncMock]:
    provider = AsyncMock()
    provider.get_funding_rate.return_value = rate
    strategy = SimpleNamespace(
        chain="hyperevm",
        wallet_address="0x" + "1" * 40,
        funding_rate_provider=provider,
    )
    return MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, runtime_surface="unit_test"), provider


def test_live_addressless_mark_requires_live_provenance() -> None:
    snapshot, _ = _snapshot(_rate(mark="3000", is_live_data=False))

    with pytest.raises(PriceUnavailableError, match="Trusted mark price unavailable"):
        snapshot.perp_mark_price("hyperliquid", "ETH/USD")


def test_live_addressless_mark_accepts_measured_venue_value() -> None:
    snapshot, provider = _snapshot(_rate(mark="3000", is_live_data=True))

    assert snapshot.perp_mark_price("hyperliquid", "ETH/USD") == Decimal("3000")
    provider.get_funding_rate.assert_awaited_once()


def test_addressless_mark_without_funding_provider_has_price_error() -> None:
    strategy = SimpleNamespace(
        chain="hyperevm",
        wallet_address="0x" + "1" * 40,
        funding_rate_provider=None,
    )
    snapshot = MarketSnapshotBuilder.for_strategy_runner(strategy=strategy, runtime_surface="unit_test")

    with pytest.raises(PriceUnavailableError, match="no funding rate provider configured"):
        snapshot.perp_mark_price("hyperliquid", "ETH/USD")


def test_backtest_mark_comes_from_tick_price_not_funding_fallback() -> None:
    snapshot, _ = _snapshot(_rate(mark="999999", is_live_data=False))
    snapshot._backtest_soft_empty_noted = set()
    snapshot.set_price("ETH", Decimal("2750"))

    assert snapshot.perp_mark_price("hyperliquid", "ETH/USD") == Decimal("2750")


def test_evm_mark_uses_index_address_without_funding_lookup() -> None:
    snapshot, provider = _snapshot(_rate(mark="999999", is_live_data=False))
    weth = "0x" + "1" * 40
    snapshot.set_price(weth, Decimal("2800"))

    assert snapshot.perp_mark_price("gmx_v2", "ETH/USD", index_token_address=weth, chain="hyperevm") == Decimal("2800")
    provider.get_funding_rate.assert_not_awaited()


def test_evm_mark_without_index_address_fails_closed() -> None:
    snapshot, provider = _snapshot(_rate(mark="3000", is_live_data=True))

    with pytest.raises(PriceUnavailableError, match="index_token_address is required"):
        snapshot.perp_mark_price("gmx_v2", "ETH/USD", chain="hyperevm")
    provider.get_funding_rate.assert_not_awaited()
