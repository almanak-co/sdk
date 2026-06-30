"""Keyed validation tier: trust-protocol Phases 3-4 (VIB-5081).

Phase 3 (data integrity): backtester price data must match an external
authoritative reference (CoinGecko), catching mock data, stale caches, and
provider failures. Phase 4 (reproducibility): identical configs with a fixed
seed must produce identical results.

These tests hit the live CoinGecko API and therefore belong to the keyed
(nightly) tier: they are marked ``validation`` and skip cleanly when
``COINGECKO_API_KEY`` is absent, so the keyless PR tier
(``-m "not validation"``) never touches the network.

Protocol source: docs/internal/reference/backtesting/Backtesting-TrustTest.md
(Tests 3.1 and 4.1).
"""

from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.engine import (
    DefaultFeeModel,
    DefaultSlippageModel,
    PnLBacktester,
)
from almanak.framework.backtesting.pnl.providers.coingecko import CoinGeckoDataProvider

pytestmark = [
    pytest.mark.validation,
    pytest.mark.skipif(
        not os.environ.get("COINGECKO_API_KEY"),
        reason="COINGECKO_API_KEY not set - keyed validation tier runs nightly with provider keys",
    ),
]

#: Trust protocol Phase 3.1 reference point: ETH on 2024-01-01 00:00 UTC.
_REFERENCE_DATE = datetime(2024, 1, 1, tzinfo=UTC)
_REFERENCE_ETH_PRICE = Decimal("2286")
_REFERENCE_TOLERANCE_PCT = Decimal("0.02")  # 2% per the protocol


class _HoldStrategy:
    """Strategy that never trades - reproducibility baseline."""

    deployment_id = "trust-keyed-reproducibility"

    def decide(self, market: object) -> None:
        return None


async def _fetch_reference_price() -> Decimal:
    provider = CoinGeckoDataProvider()
    try:
        price = await provider.get_price("WETH", _REFERENCE_DATE)
        return Decimal(str(price))
    finally:
        close = getattr(provider, "close", None)
        if close is not None:
            await close()


def test_phase3_price_data_matches_coingecko_reference() -> None:
    """Trust protocol Test 3.1: provider price within 2% of the known reference.

    Catches mock data, placeholder prices, and silent provider failures: the
    engine's historical prices must come from the real external source.
    """
    actual = asyncio.run(_fetch_reference_price())

    assert actual > Decimal("0"), "provider returned a non-positive reference price"
    deviation = abs(actual - _REFERENCE_ETH_PRICE) / _REFERENCE_ETH_PRICE
    assert deviation <= _REFERENCE_TOLERANCE_PCT, (
        f"ETH price on {_REFERENCE_DATE.date()} deviates {deviation:.2%} from the "
        f"CoinGecko reference ${_REFERENCE_ETH_PRICE} (got ${actual}); data integrity suspect"
    )


def _run_seeded_backtest() -> object:
    config = PnLBacktestConfig(
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        end_time=datetime(2024, 1, 3, tzinfo=UTC),
        interval_seconds=3600,
        initial_capital_usd=Decimal("10000"),
        tokens=["WETH", "USDC"],
        include_gas_costs=False,
        random_seed=42,
    )
    backtester = PnLBacktester(
        data_provider=CoinGeckoDataProvider(),
        fee_models={"default": DefaultFeeModel()},
        slippage_models={"default": DefaultSlippageModel()},
    )
    return asyncio.run(backtester.backtest(_HoldStrategy(), config))


def test_phase4_fixed_seed_runs_are_identical() -> None:
    """Trust protocol Test 4.1: same config + same seed == identical results.

    Catches non-deterministic code paths, time-based randomness, and cache
    inconsistencies. Compares the full equity curve, not just the headline
    metrics.
    """
    first = _run_seeded_backtest()
    second = _run_seeded_backtest()

    assert first.success and second.success
    assert first.config_hash == second.config_hash
    assert first.final_capital_usd == second.final_capital_usd
    assert first.metrics.total_trades == second.metrics.total_trades
    assert first.metrics.total_return_pct == second.metrics.total_return_pct
    assert len(first.equity_curve) == len(second.equity_curve)
    for a, b in zip(first.equity_curve, second.equity_curve, strict=True):
        assert a.timestamp == b.timestamp
        assert a.value_usd == b.value_usd
