"""A backtest snapshot never leaks live reference prices into historical ticks (ALM-3487).

``MarketSnapshot.reference_price`` is gateway-only: with no connected client it
fails closed. Inside a PnL backtest that bare transport error misdirects the
operator toward infra, and wiring a client would be worse — a reachable
sidecar answers a HISTORICAL tick with TODAY's price (look-ahead bias, the
same hazard fenced for lending rates in ALM-3008). The engine's snapshot
factory stamps a refusal detail so the lane refuses with a typed
``reference_price/backtest_no_historical_plane`` ledger entry BEFORE
consulting any client; the live lane (no backtest factory) is untouched.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import create_market_snapshot_from_state
from almanak.framework.market import MarketSnapshot

CHAIN = "ethereum"
WETH_ADDR = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
USDC_ADDR = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
TOKEN_ADDRESSES = {"WETH": (CHAIN, WETH_ADDR), "USDC": (CHAIN, USDC_ADDR)}
TS = datetime(2026, 7, 1, tzinfo=UTC)

REFUSAL_KEY = "backtest_no_historical_plane"


def _market_state() -> MarketState:
    state = MarketState(
        timestamp=TS,
        prices={(CHAIN, WETH_ADDR): Decimal("3000"), (CHAIN, USDC_ADDR): Decimal("1")},
        chain=CHAIN,
    )
    state.register_symbol_aliases(TOKEN_ADDRESSES)
    return state


def _backtest_snapshot() -> MarketSnapshot:
    """A snapshot exactly as the PnL engine builds it per historical tick."""
    return create_market_snapshot_from_state(
        market_state=_market_state(),
        chain=CHAIN,
        token_addresses=TOKEN_ADDRESSES,
    )


def _tripwire_client() -> MagicMock:
    """A connected client whose RPC explodes: any call is look-ahead bias."""
    client = MagicMock()
    client.is_connected = True
    client.market.GetReferencePrice.side_effect = AssertionError(
        "ALM-3487: GetReferencePrice issued inside a backtest snapshot"
    )
    return client


class TestBacktestRefusesLiveReferencePrice:
    def test_factory_stamps_refusal_detail(self):
        snapshot = _backtest_snapshot()

        assert snapshot._reference_price_refusal_detail is not None
        assert "no historical reference-price plane" in snapshot._reference_price_refusal_detail

    def test_refusal_precedes_any_wired_client(self):
        snapshot = _backtest_snapshot()
        snapshot._gateway_client = _tripwire_client()

        result = snapshot.reference_price("AAPL", quote="USD")

        assert result.price is None
        assert result.stale is True
        assert "no historical reference-price plane" in (result.reason or "")
        snapshot._gateway_client.market.GetReferencePrice.assert_not_called()
        assert ("reference_price", REFUSAL_KEY) in snapshot._critical_data_failures
        # The transport-misdirection key is NOT the story here.
        assert ("reference_price", f"AAPL@{CHAIN}") not in snapshot._critical_data_failures

    def test_unstamped_gatewayless_snapshot_keeps_transport_error(self):
        from almanak.framework.market.builders import MarketSnapshotBuilder

        snapshot = MarketSnapshotBuilder.seeded(chain="bsc", wallet_address="0x1", timestamp=TS)
        assert getattr(snapshot, "_reference_price_refusal_detail", None) is None

        result = snapshot.reference_price("AAPL", chain="bsc", quote="USD")

        assert result.price is None
        assert result.reason == "reference_price unavailable: no connected GatewayClient"
        assert ("reference_price", "AAPL@bsc") in snapshot._critical_data_failures
        assert ("reference_price", REFUSAL_KEY) not in snapshot._critical_data_failures

    def test_live_builder_snapshot_has_no_refusal_detail(self):
        from almanak.framework.market.builders import MarketSnapshotBuilder

        snapshot = MarketSnapshotBuilder.seeded(chain=CHAIN, wallet_address="0x" + "0" * 40)

        assert getattr(snapshot, "_reference_price_refusal_detail", None) is None


class TestRunLevelLedger:
    """The refusal lands in ``BacktestResult.decision_input_failures`` through the real engine loop."""

    def test_reference_gated_run_is_unsupported_not_tradable(self):
        from tests.validation.backtesting.trust_matrix import ScriptedStrategy, flat_series, run_backtest

        ticks: list[int] = []

        class ReferenceGatedProbe(ScriptedStrategy):
            def decide(self, market: Any) -> Any:
                ticks.append(1)
                ref = market.reference_price("AAPL", quote="USD")
                if not ref.is_tradeable(max_age_seconds=60, now=market.timestamp):
                    return None
                return super().decide(market)

        result = run_backtest(ReferenceGatedProbe([None, None]), flat_series(6), hours=3)

        assert ticks, "decide() never ran"
        assert result.success is False
        assert result.error is not None and result.error.startswith("BACKTEST_UNSUPPORTED_DATA:")
        assert f"reference_price:{REFUSAL_KEY}" in result.error
        keys = {f["key"] for f in (result.decision_input_failures or []) if f["source"] == "reference_price"}
        assert keys == {REFUSAL_KEY}, result.decision_input_failures
