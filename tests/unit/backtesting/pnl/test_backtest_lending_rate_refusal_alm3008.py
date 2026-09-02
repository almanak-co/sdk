"""A backtest snapshot never dials the live gateway for lending rates (ALM-3008).

``MarketSnapshot.lending_rate`` / ``best_lending_rate`` fall through, on a
cache miss with no injected monitor, to a lazily-built gateway ``RateMonitor``
that calls ``GetLendingRateCurrent``. Inside a PnL backtest that is look-ahead
bias: a reachable sidecar answers a HISTORICAL tick with TODAY's rate. The
engine's snapshot factory stamps a refusal detail so the lazy lane refuses
with a typed ``<source>/backtest_no_historical_plane`` ledger entry BEFORE
constructing anything; the live lane (no backtest factory) is untouched.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import create_market_snapshot_from_state
from almanak.framework.data.rates import monitor as monitor_module
from almanak.framework.data.rates.monitor import LendingRate
from almanak.framework.market import MarketSnapshot

CHAIN = "ethereum"
WETH_ADDR = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
USDC_ADDR = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
TOKEN_ADDRESSES = {"WETH": (CHAIN, WETH_ADDR), "USDC": (CHAIN, USDC_ADDR)}
TS = datetime(2026, 7, 1, tzinfo=UTC)

REFUSAL_KEY = "backtest_no_historical_plane"


def _rate(protocol: str, token: str, side: str, apy: str) -> LendingRate:
    return LendingRate(
        protocol=protocol,
        token=token,
        side=side,
        apy_ray=Decimal("0"),
        apy_percent=Decimal(apy),
        chain=CHAIN,
        timestamp=TS,
    )


SEEDED_RATE = _rate("aave_v3", "USDC", "supply", "3.00")


def _market_state() -> MarketState:
    state = MarketState(
        timestamp=TS,
        prices={(CHAIN, WETH_ADDR): Decimal("3000"), (CHAIN, USDC_ADDR): Decimal("1")},
        chain=CHAIN,
    )
    state.register_symbol_aliases(TOKEN_ADDRESSES)
    return state


def _backtest_snapshot() -> MarketSnapshot:
    """A snapshot exactly as the PnL engine builds it per tick (seeded aave_v3 supply only)."""
    return create_market_snapshot_from_state(
        market_state=_market_state(),
        chain=CHAIN,
        token_addresses=TOKEN_ADDRESSES,
        lending_rates=[SEEDED_RATE],
    )


def _live_snapshot() -> MarketSnapshot:
    """A live-lane snapshot: sanctioned builder, no backtest factory, no injected monitor."""
    from almanak.framework.market.builders import MarketSnapshotBuilder

    return MarketSnapshotBuilder.seeded(chain=CHAIN, wallet_address="0x" + "0" * 40)


class _GatewayProbe:
    """Counts every path that would reach the gateway; installed via ``monkeypatch``.

    ``RateMonitor`` construction is counted (and, in tripwire mode, refused
    outright) and every gateway client resolution / ``GetLendingRateCurrent``
    call is an assertion failure — so a passing refusal test proves ZERO
    gateway traffic, not merely "the call raised".
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch, *, served: LendingRate | None = None) -> None:
        self.constructed: list[dict[str, Any]] = []
        self.rpc_calls = 0
        probe = self

        class FakeRateMonitor:
            protocols = ["aave_v3", "compound_v3"]

            def __init__(self, *, chain: str, gateway_client: Any = None, _internal: bool = False) -> None:
                probe.constructed.append({"chain": chain, "gateway_client": gateway_client, "_internal": _internal})
                if served is None:
                    raise AssertionError("ALM-3008: RateMonitor constructed inside a backtest snapshot")

            async def _fetch_lending_rate_via_gateway(
                self, protocol: str, token: str, side: str, market_id: str | None = None
            ) -> LendingRate:
                probe.rpc_calls += 1
                assert served is not None
                return served

        def _no_gateway(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("ALM-3008: gateway client resolved inside a backtest snapshot")

        async def _no_rpc(*args: Any, **kwargs: Any) -> Any:
            raise AssertionError("ALM-3008: GetLendingRateCurrent issued inside a backtest snapshot")

        monkeypatch.setattr(monitor_module, "RateMonitor", FakeRateMonitor)
        monkeypatch.setattr(monitor_module, "_monitor_get_connected_gateway_client", _no_gateway)
        monkeypatch.setattr(monitor_module, "_monitor_call_lending_rate_current", _no_rpc)


class TestBacktestRefusesLiveGateway:
    def test_lending_rate_cache_miss_refuses_without_touching_gateway(self, monkeypatch):
        probe = _GatewayProbe(monkeypatch)
        snapshot = _backtest_snapshot()

        with pytest.raises(ValueError, match="lending_rate refused for compound_v3/USDC/supply") as excinfo:
            snapshot.lending_rate("compound_v3", "USDC", "supply")

        assert "no historical lending-rate plane" in str(excinfo.value)
        assert probe.constructed == [], "RateMonitor must never be built inside a backtest"
        assert probe.rpc_calls == 0
        assert ("lending_rate", REFUSAL_KEY) in snapshot._critical_data_failures
        # The live-misconfiguration key is NOT the story here.
        assert ("lending_rate", "unconfigured") not in snapshot._critical_data_failures

    def test_seeded_protocol_unseeded_side_refuses(self, monkeypatch):
        # Only aave_v3/USDC/supply is seeded; the borrow side is a cache miss
        # that must refuse rather than dial for a live borrow rate.
        _GatewayProbe(monkeypatch)
        snapshot = _backtest_snapshot()

        with pytest.raises(ValueError, match="aave_v3/USDC/borrow"):
            snapshot.lending_rate("aave_v3", "USDC", "borrow")

        assert ("lending_rate", REFUSAL_KEY) in snapshot._critical_data_failures

    def test_best_lending_rate_refuses_without_touching_gateway(self, monkeypatch):
        probe = _GatewayProbe(monkeypatch)
        snapshot = _backtest_snapshot()

        with pytest.raises(ValueError, match="best_lending_rate refused for best supply rate on USDC"):
            snapshot.best_lending_rate("USDC", "supply")

        assert probe.constructed == []
        assert probe.rpc_calls == 0
        assert ("best_lending_rate", REFUSAL_KEY) in snapshot._critical_data_failures
        assert ("best_lending_rate", "unconfigured") not in snapshot._critical_data_failures

    def test_seeded_read_still_serves(self, monkeypatch):
        probe = _GatewayProbe(monkeypatch)
        snapshot = _backtest_snapshot()

        served = snapshot.lending_rate("aave_v3", "USDC", "supply")

        assert served is SEEDED_RATE
        assert served.apy_percent == Decimal("3.00")
        assert probe.constructed == []
        assert not snapshot._critical_data_failures

    def test_injected_monitor_is_not_consulted_in_a_backtest(self, monkeypatch):
        # The refusal covers every monitor, not only the lazy gateway lane: an
        # injected gateway-backed monitor would answer a historical tick with a
        # current rate just the same.
        _GatewayProbe(monkeypatch)
        snapshot = _backtest_snapshot()
        injected = _rate("compound_v3", "USDC", "supply", "4.50")
        calls: list[tuple[str, ...]] = []

        class InjectedMonitor:
            async def get_lending_rate(self, protocol: str, token: str, side: Any, market_id: Any = None) -> Any:
                calls.append((protocol, token, str(side)))
                return injected

            async def get_best_lending_rate(self, token: str, side: Any, protocols: Any = None) -> Any:
                calls.append(("best", token, str(side)))
                return injected

        snapshot._rate_monitor = InjectedMonitor()

        with pytest.raises(ValueError, match="lending_rate refused for"):
            snapshot.lending_rate("compound_v3", "USDC", "supply")
        with pytest.raises(ValueError, match="best_lending_rate refused for"):
            snapshot.best_lending_rate("USDC", "supply")

        assert calls == []
        assert ("lending_rate", REFUSAL_KEY) in snapshot._critical_data_failures
        assert ("best_lending_rate", REFUSAL_KEY) in snapshot._critical_data_failures

    def test_injected_monitor_is_honoured_on_a_live_snapshot(self, monkeypatch):
        _GatewayProbe(monkeypatch)
        snapshot = _live_snapshot()
        injected = _rate("compound_v3", "USDC", "supply", "4.50")

        class InjectedMonitor:
            async def get_lending_rate(self, protocol: str, token: str, side: Any, market_id: Any = None) -> Any:
                return injected

        snapshot._rate_monitor = InjectedMonitor()

        assert snapshot.lending_rate("compound_v3", "USDC", "supply") is injected
        assert not snapshot._critical_data_failures


class TestRunLevelLedger:
    """The refusal lands in ``BacktestResult.decision_input_failures`` through the real engine loop."""

    def test_unseeded_reads_are_reported_not_fetched(self, monkeypatch):
        from tests.validation.backtesting.trust_matrix import ScriptedStrategy, flat_series, run_backtest

        probe = _GatewayProbe(monkeypatch)
        ticks: list[int] = []
        errors: list[str] = []

        class UnseededProbe(ScriptedStrategy):
            def decide(self, market):
                ticks.append(1)
                # The engine seeds every connector-table protocol for the
                # run's OWN tokens on the UNSCOPED key. Both reads below miss
                # that plane exactly the way a live strategy would: a token
                # outside the run, and a market-scoped isolated-market read
                # (scoped keys carry market_id) — the look-ahead trap.
                for read in (
                    lambda: market.lending_rate("aave_v3", "WBTC", "supply"),
                    lambda: market.lending_rate("morpho_blue", "USDC", "borrow", market_id="0x" + "ab" * 32),
                    lambda: market.best_lending_rate("USDC", "supply"),
                ):
                    try:
                        read()
                    except ValueError as exc:  # strategy-side HOLD on refused input
                        errors.append(str(exc))
                return super().decide(market)

        result = run_backtest(UnseededProbe([None, None]), flat_series(6), hours=3)

        # Every tick refused a required input and no intent was emitted: the
        # engine's hollow-run detection must name the refusal
        # rather than report passive mark-to-market as strategy performance.
        assert result.success is False
        assert result.error is not None and result.error.startswith("BACKTEST_UNSUPPORTED_DATA:")
        assert f"lending_rate:{REFUSAL_KEY}" in result.error
        assert f"best_lending_rate:{REFUSAL_KEY}" in result.error
        assert ticks, "decide() never ran"
        assert len(errors) == 3 * len(ticks), errors
        assert all("no historical lending-rate plane" in message for message in errors)
        assert probe.constructed == []
        assert probe.rpc_calls == 0
        by_source = {
            source: {f["key"] for f in (result.decision_input_failures or []) if f["source"] == source}
            for source in ("lending_rate", "best_lending_rate")
        }
        assert by_source == {"lending_rate": {REFUSAL_KEY}, "best_lending_rate": {REFUSAL_KEY}}, (
            result.decision_input_failures
        )


class TestLiveLaneUnchanged:
    """Outside a backtest the lazy gateway monitor is built and used exactly as before."""

    def test_lending_rate_builds_gateway_monitor(self, monkeypatch):
        live_rate = _rate("compound_v3", "USDC", "supply", "5.25")
        probe = _GatewayProbe(monkeypatch, served=live_rate)
        snapshot = _live_snapshot()
        assert getattr(snapshot, "_lending_rate_refusal_detail", None) is None

        served = snapshot.lending_rate("compound_v3", "USDC", "supply")

        assert served is live_rate
        assert probe.constructed == [{"chain": CHAIN, "gateway_client": None, "_internal": True}]
        assert probe.rpc_calls == 1
        assert not snapshot._critical_data_failures
        # Cached after the first gateway read: no second construction.
        assert snapshot.lending_rate("compound_v3", "USDC", "supply") is live_rate
        assert len(probe.constructed) == 1

    def test_best_lending_rate_builds_gateway_monitor(self, monkeypatch):
        live_rate = _rate("aave_v3", "USDC", "supply", "2.10")
        probe = _GatewayProbe(monkeypatch, served=live_rate)
        snapshot = _live_snapshot()

        result = snapshot.best_lending_rate("USDC", "supply")

        assert result.best_rate is live_rate
        assert len(probe.constructed) == 1
        assert probe.rpc_calls == 2  # one fan-out per FakeRateMonitor.protocols entry
        assert not snapshot._critical_data_failures
