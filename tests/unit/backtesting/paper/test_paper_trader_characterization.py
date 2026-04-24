"""Characterization tests for PaperTrader.run (Phase 9.4).

These tests pin the current behaviour of ``PaperTrader.run`` so a refactor can
be verified to leave it byte-for-byte identical. Every branch of the main
entry point is exercised with a deterministic in-memory harness that stubs the
expensive async helpers (fork init, orchestrator, metrics, cleanup, equity
recording) and asserts:

* state-reset on entry (trades/errors/equity/tick_count/backtest_id/...)
* effective-duration resolution from args vs config vs default
* session start/end event emission with exact payloads
* main loop phase ordering (advance -> tick -> reconciler -> sleep -> refresh)
* tick-limit vs duration-limit exit paths
* CancelledError + generic Exception handling including error-handler wiring
* final-value fallback chain (rich -> last equity point -> simple)
* final BacktestResult shape: initial_capital_usd, final_capital_usd,
  trade_records mapping, equity_curve passthrough, compliance_violations,
  config dict (including error_summary), institutional_compliance,
  fallback_usage dict
* TradeRecord byte-for-byte mapping from PaperTrade

The tests do NOT exercise the real fork / orchestrator / price oracle. Those
are covered elsewhere. The goal here is control-flow + output-shape pinning.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    EquityPoint,
    IntentType,
)
from almanak.framework.backtesting.paper.config import ForkLifecycle, PaperTraderConfig
from almanak.framework.backtesting.paper.engine import (
    PaperTradeEventType,
    PaperTrader,
)
from almanak.framework.backtesting.paper.models import PaperTrade

# ---------------------------------------------------------------------------
# Shared deterministic mocks
# ---------------------------------------------------------------------------


@dataclass
class _MockPortfolioTracker:
    initial_balances: dict[str, Decimal] = field(default_factory=dict)
    current_balances: dict[str, Decimal] = field(default_factory=dict)

    def start_session(self, **kwargs: Any) -> None:
        pass

    def record_trade(self, trade: Any) -> None:
        pass


@dataclass
class _MockForkManager:
    rpc_url: str = "http://127.0.0.1:0"
    is_running: bool = False
    current_block: int | None = None
    stop_calls: int = 0

    async def start(self) -> None:
        self.is_running = True
        self.current_block = 12345

    async def stop(self) -> None:
        self.is_running = False
        self.stop_calls += 1

    async def reset_to_latest(self) -> None:
        pass

    def get_rpc_url(self) -> str:
        return self.rpc_url


class _MockStrategy:
    strategy_id = "char_strategy"

    async def decide(self, snapshot: Any) -> None:
        return None


def _make_config(**overrides: Any) -> PaperTraderConfig:
    """Build a PaperTraderConfig with deterministic defaults for char tests."""
    kwargs: dict[str, Any] = {
        "chain": "arbitrum",
        "rpc_url": "https://arb.example/rpc",
        "strategy_id": "char_strategy",
        "tick_interval_seconds": 0.001,  # keep sleeps near-instant
        "price_source": "coingecko",
        "strict_price_mode": False,
    }
    kwargs.update(overrides)
    return PaperTraderConfig(**kwargs)


def _make_trader(
    *,
    config: PaperTraderConfig | None = None,
    event_callback: Any = None,
) -> PaperTrader:
    """Construct a PaperTrader with price/indicator init patched out."""
    from unittest.mock import patch

    cfg = config or _make_config()
    with patch(
        "almanak.framework.backtesting.paper.engine.CoinGeckoPriceSource"
    ), patch(
        "almanak.framework.backtesting.paper.engine.ChainlinkDataProvider"
    ), patch(
        "almanak.framework.backtesting.paper.engine.DEXTWAPDataProvider"
    ):
        trader = PaperTrader(
            fork_manager=_MockForkManager(),
            portfolio_tracker=_MockPortfolioTracker(),
            config=cfg,
            event_callback=event_callback,
        )
    # Null out providers so no network is touched.
    trader._price_aggregator = MagicMock()
    trader._chainlink_provider = None
    trader._twap_provider = None
    trader._rsi_calculator = None
    return trader


def _install_fast_run_harness(
    trader: PaperTrader,
    *,
    tick_behaviour: Any = None,
    initial_equity_value: Decimal = Decimal("10000"),
    final_rich: tuple[Decimal, Decimal, Decimal] | None = None,
    final_simple: Decimal = Decimal("10500"),
    metrics: BacktestMetrics | None = None,
) -> dict[str, list]:
    """Stub out heavy helpers so run() is fully deterministic.

    Returns a ``spy`` dict capturing the call order of the phase helpers so
    tests can assert per-bar ordering is preserved.
    """
    spy: dict[str, list] = {"order": [], "tick_calls": 0, "reconciler_calls": 0}

    async def _noop(*args: Any, **kwargs: Any) -> None:
        return None

    async def _initialize_fork() -> None:
        spy["order"].append("init_fork")

    async def _initialize_orchestrator() -> None:
        spy["order"].append("init_orchestrator")
        trader._orchestrator = MagicMock()
        trader._orchestrator.signer = MagicMock()
        trader._orchestrator.signer.address = "0x0000000000000000000000000000000000000001"

    def _init_portfolio_valuer() -> None:
        spy["order"].append("init_portfolio_valuer")

    async def _seed_initial_market_snapshot() -> None:
        spy["order"].append("seed_initial_market_snapshot")

    async def _record_equity_point() -> None:
        spy["order"].append("record_equity")
        trader._equity_curve.append(
            EquityPoint(
                timestamp=datetime.now(UTC),
                value_usd=initial_equity_value,
                eth_price_usd=Decimal("3000"),
                valuation_source="simple",
            )
        )

    async def _advance_persistent_fork() -> None:
        spy["order"].append("advance_persistent")

    async def _execute_tick(strategy: Any) -> None:
        spy["order"].append("execute_tick")
        spy["tick_calls"] += 1
        if callable(tick_behaviour):
            await tick_behaviour(trader, strategy, spy)

    async def _run_position_reconciler() -> None:
        spy["order"].append("reconciler")
        spy["reconciler_calls"] += 1

    async def _should_refresh_fork() -> bool:
        return False

    async def _refresh_fork() -> None:
        spy["order"].append("refresh_fork")

    async def _cleanup() -> None:
        spy["order"].append("cleanup")

    async def _get_portfolio_prices() -> dict[str, Decimal]:
        spy["order"].append("get_portfolio_prices")
        return {"ETH": Decimal("3000")}

    def _value_portfolio_rich() -> tuple[Decimal, Decimal, Decimal] | None:
        return final_rich

    def _calculate_portfolio_value() -> Decimal:
        return final_simple

    def _calculate_initial_capital() -> Decimal:
        return Decimal("10000")

    def _calculate_metrics() -> BacktestMetrics:
        return metrics if metrics is not None else BacktestMetrics(
            net_pnl_usd=Decimal("500"),
        )

    trader._initialize_fork = _initialize_fork  # type: ignore[method-assign]
    trader._initialize_orchestrator = _initialize_orchestrator  # type: ignore[method-assign]
    trader._init_portfolio_valuer = _init_portfolio_valuer  # type: ignore[method-assign]
    trader._seed_initial_market_snapshot = _seed_initial_market_snapshot  # type: ignore[method-assign]
    trader._record_equity_point = _record_equity_point  # type: ignore[method-assign]
    trader._advance_persistent_fork = _advance_persistent_fork  # type: ignore[method-assign]
    trader._execute_tick = _execute_tick  # type: ignore[method-assign]
    trader._run_position_reconciler = _run_position_reconciler  # type: ignore[method-assign]
    trader._should_refresh_fork = _should_refresh_fork  # type: ignore[method-assign]
    trader._refresh_fork = _refresh_fork  # type: ignore[method-assign]
    trader._cleanup = _cleanup  # type: ignore[method-assign]
    trader._get_portfolio_prices = _get_portfolio_prices  # type: ignore[method-assign]
    trader._value_portfolio_rich = _value_portfolio_rich  # type: ignore[method-assign]
    trader._calculate_portfolio_value = _calculate_portfolio_value  # type: ignore[method-assign]
    trader._calculate_initial_capital = _calculate_initial_capital  # type: ignore[method-assign]
    trader._calculate_metrics = _calculate_metrics  # type: ignore[method-assign]
    return spy


# ---------------------------------------------------------------------------
# Entry guard / state reset
# ---------------------------------------------------------------------------


class TestRunGuardAndStateReset:
    @pytest.mark.asyncio
    async def test_raises_if_already_running(self) -> None:
        trader = _make_trader()
        trader._running = True
        with pytest.raises(RuntimeError, match="already running"):
            await trader.run(_MockStrategy(), max_ticks=0)

    @pytest.mark.asyncio
    async def test_resets_per_run_state(self) -> None:
        trader = _make_trader()
        _install_fast_run_harness(trader)
        # Poison prior state.
        trader._trades = [MagicMock()]
        trader._errors = [MagicMock()]
        trader._equity_curve = [MagicMock()]
        trader._tick_count = 99
        trader._reconciler_discrepancies = [MagicMock()]
        trader._last_execution_result = MagicMock()
        trader._ticks_with_fork = 5
        trader._ticks_with_indicators = 5
        trader._ticks_with_action = 5
        trader._last_successful_decision_at = datetime.now(UTC)
        trader._last_trade_at = datetime.now(UTC)

        await trader.run(_MockStrategy(), duration_seconds=0.0, max_ticks=0)

        assert trader._trades == []
        assert trader._errors == []
        # Equity curve gets one initial point via the harness.
        assert len(trader._equity_curve) == 1
        assert trader._tick_count == 0
        assert trader._reconciler_discrepancies == []
        assert trader._last_execution_result is None
        assert trader._ticks_with_fork == 0
        assert trader._ticks_with_indicators == 0
        assert trader._ticks_with_action == 0
        assert trader._last_successful_decision_at is None
        assert trader._last_trade_at is None
        assert trader._backtest_id is not None
        assert trader._error_handler is not None
        # _running must be False after return.
        assert trader._running is False
        assert trader._current_strategy is None


# ---------------------------------------------------------------------------
# Duration resolution
# ---------------------------------------------------------------------------


class TestEffectiveDuration:
    @pytest.mark.asyncio
    async def test_arg_wins_over_config(self) -> None:
        # config.max_duration_seconds == max_ticks * tick_interval_seconds
        # -> 10 * 10 = 100.
        cfg = _make_config(tick_interval_seconds=10, max_ticks=10)
        trader = _make_trader(config=cfg)
        events: list[tuple[str, dict]] = []

        def _cb(kind: str, data: dict) -> None:
            events.append((kind, data))

        trader.event_callback = _cb  # type: ignore[assignment]
        _install_fast_run_harness(trader)
        await trader.run(_MockStrategy(), duration_seconds=1.5, max_ticks=0)
        start = [e for e in events if e[0] == PaperTradeEventType.SESSION_STARTED][0][1]
        assert start["duration_seconds"] == 1.5

    @pytest.mark.asyncio
    async def test_config_used_when_arg_none(self) -> None:
        # config.max_duration_seconds == max_ticks * tick_interval_seconds == 7 * 6 = 42.
        cfg = _make_config(tick_interval_seconds=6, max_ticks=7)
        trader = _make_trader(config=cfg)
        events: list[tuple[str, dict]] = []
        trader.event_callback = lambda k, d: events.append((k, d))  # type: ignore[assignment]
        _install_fast_run_harness(trader)
        await trader.run(_MockStrategy(), max_ticks=0)
        start = [e for e in events if e[0] == PaperTradeEventType.SESSION_STARTED][0][1]
        assert start["duration_seconds"] == 42

    @pytest.mark.asyncio
    async def test_defaults_to_one_hour_when_both_missing(self) -> None:
        # config.max_duration_seconds is None (max_ticks=None) -> default 3600.
        cfg = _make_config(max_ticks=None)
        trader = _make_trader(config=cfg)
        events: list[tuple[str, dict]] = []
        trader.event_callback = lambda k, d: events.append((k, d))  # type: ignore[assignment]
        _install_fast_run_harness(trader)
        await trader.run(_MockStrategy(), max_ticks=0)
        start = [e for e in events if e[0] == PaperTradeEventType.SESSION_STARTED][0][1]
        assert start["duration_seconds"] == 3600.0


# ---------------------------------------------------------------------------
# Setup-phase ordering
# ---------------------------------------------------------------------------


class TestSetupPhase:
    @pytest.mark.asyncio
    async def test_setup_order_is_fixed(self) -> None:
        trader = _make_trader()
        spy = _install_fast_run_harness(trader)
        await trader.run(_MockStrategy(), duration_seconds=0.0, max_ticks=0)
        prefix = spy["order"][:5]
        assert prefix == [
            "init_fork",
            "init_orchestrator",
            "init_portfolio_valuer",
            "seed_initial_market_snapshot",
            "record_equity",
        ]


# ---------------------------------------------------------------------------
# Main loop iteration ordering & exit paths
# ---------------------------------------------------------------------------


class TestMainLoopOrdering:
    @pytest.mark.asyncio
    async def test_per_bar_order_rolling(self) -> None:
        # Rolling reset: no advance_persistent, no reconciler (default cfg).
        trader = _make_trader()
        spy = _install_fast_run_harness(trader)
        await trader.run(_MockStrategy(), duration_seconds=60.0, max_ticks=3)
        # Filter to loop-only events (skip setup prefix).
        loop_events = [e for e in spy["order"] if e not in {
            "init_fork",
            "init_orchestrator",
            "init_portfolio_valuer",
            "seed_initial_market_snapshot",
            "cleanup",
            "get_portfolio_prices",
        }]
        # First is the initial record_equity; then 3 tick cycles.
        assert loop_events[0] == "record_equity"
        # Exactly 3 execute_tick calls.
        assert loop_events.count("execute_tick") == 3
        # No advance_persistent / reconciler in rolling mode.
        assert "advance_persistent" not in loop_events
        assert "reconciler" not in loop_events

    @pytest.mark.asyncio
    async def test_per_bar_order_persistent(self) -> None:
        cfg = _make_config(
            fork_lifecycle=ForkLifecycle.PERSISTENT,
            position_reconciler_enabled=True,
        )
        trader = _make_trader(config=cfg)
        spy = _install_fast_run_harness(trader)
        await trader.run(_MockStrategy(), duration_seconds=60.0, max_ticks=3)

        # Persistent mode: tick0 has no advance (tick_count==0 check),
        # ticks 1 and 2 have advance_persistent BEFORE execute_tick.
        order = spy["order"]
        tick_positions = [i for i, e in enumerate(order) if e == "execute_tick"]
        advance_positions = [i for i, e in enumerate(order) if e == "advance_persistent"]
        reconciler_positions = [i for i, e in enumerate(order) if e == "reconciler"]

        assert len(tick_positions) == 3
        # First tick NOT preceded by advance (tick_count==0 gate).
        assert advance_positions[0] > tick_positions[0]  # advance only after tick 0
        # Every execute_tick followed by reconciler.
        assert len(reconciler_positions) == 3
        for tpos, rpos in zip(tick_positions, reconciler_positions, strict=True):
            assert rpos > tpos

    @pytest.mark.asyncio
    async def test_max_ticks_exit(self) -> None:
        trader = _make_trader()
        spy = _install_fast_run_harness(trader)
        result = await trader.run(_MockStrategy(), duration_seconds=3600.0, max_ticks=2)
        assert spy["tick_calls"] == 2
        assert trader._tick_count == 2
        assert result.error is None

    @pytest.mark.asyncio
    async def test_duration_exit(self) -> None:
        # duration_seconds=0 forces immediate exit before any tick.
        trader = _make_trader()
        spy = _install_fast_run_harness(trader)
        await trader.run(_MockStrategy(), duration_seconds=0.0, max_ticks=None)
        assert spy["tick_calls"] == 0

    @pytest.mark.asyncio
    async def test_tick_count_increments(self) -> None:
        trader = _make_trader()
        _install_fast_run_harness(trader)
        await trader.run(_MockStrategy(), duration_seconds=3600.0, max_ticks=4)
        assert trader._tick_count == 4


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestErrorPaths:
    @pytest.mark.asyncio
    async def test_cancelled_error_sets_cancel_message(self) -> None:
        trader = _make_trader()

        async def _boom_tick(self_: Any, strategy: Any, spy: dict) -> None:
            raise asyncio.CancelledError()

        _install_fast_run_harness(trader, tick_behaviour=_boom_tick)
        result = await trader.run(_MockStrategy(), duration_seconds=3600.0, max_ticks=5)
        assert result.error == "Session cancelled"

    @pytest.mark.asyncio
    async def test_generic_exception_captured_as_string(self) -> None:
        trader = _make_trader()

        async def _boom_tick(self_: Any, strategy: Any, spy: dict) -> None:
            raise ValueError("kaboom")

        _install_fast_run_harness(trader, tick_behaviour=_boom_tick)
        result = await trader.run(_MockStrategy(), duration_seconds=3600.0, max_ticks=5)
        assert result.error == "kaboom"

    @pytest.mark.asyncio
    async def test_cleanup_runs_even_on_error(self) -> None:
        trader = _make_trader()

        async def _boom_tick(self_: Any, strategy: Any, spy: dict) -> None:
            raise ValueError("kaboom")

        spy = _install_fast_run_harness(trader, tick_behaviour=_boom_tick)
        await trader.run(_MockStrategy(), duration_seconds=3600.0, max_ticks=5)
        assert "cleanup" in spy["order"]
        # Running flag cleared.
        assert trader._running is False
        assert trader._current_strategy is None


# ---------------------------------------------------------------------------
# Final value selection (rich > last equity > simple)
# ---------------------------------------------------------------------------


class TestFinalValueFallbackChain:
    @pytest.mark.asyncio
    async def test_rich_valuation_wins(self) -> None:
        trader = _make_trader()
        _install_fast_run_harness(
            trader,
            final_rich=(Decimal("12345.67"), Decimal("10000"), Decimal("2345.67")),
            final_simple=Decimal("9000"),
        )
        result = await trader.run(_MockStrategy(), duration_seconds=0.0, max_ticks=0)
        assert result.final_capital_usd == Decimal("12345.67")

    @pytest.mark.asyncio
    async def test_last_equity_point_when_rich_unavailable(self) -> None:
        trader = _make_trader()
        spy = _install_fast_run_harness(
            trader,
            final_rich=None,
            initial_equity_value=Decimal("10123.45"),
            final_simple=Decimal("9000"),
        )
        result = await trader.run(_MockStrategy(), duration_seconds=0.0, max_ticks=0)
        # _equity_curve has the initial record_equity point only.
        assert result.final_capital_usd == Decimal("10123.45")
        assert "cleanup" in spy["order"]

    @pytest.mark.asyncio
    async def test_simple_when_rich_and_equity_missing(self) -> None:
        trader = _make_trader()

        async def _record_no_op() -> None:
            # Suppress the initial equity point.
            return None

        _install_fast_run_harness(
            trader,
            final_rich=None,
            final_simple=Decimal("7777.77"),
        )
        # Override so equity curve stays empty.
        trader._record_equity_point = _record_no_op  # type: ignore[method-assign]
        result = await trader.run(_MockStrategy(), duration_seconds=0.0, max_ticks=0)
        assert result.final_capital_usd == Decimal("7777.77")


# ---------------------------------------------------------------------------
# Event emission
# ---------------------------------------------------------------------------


class TestEventEmission:
    @pytest.mark.asyncio
    async def test_session_started_and_ended_fired_with_payload(self) -> None:
        events: list[tuple[str, dict]] = []
        trader = _make_trader(event_callback=lambda k, d: events.append((k, d)))
        _install_fast_run_harness(trader)
        await trader.run(_MockStrategy(), duration_seconds=0.0, max_ticks=0)
        kinds = [e[0] for e in events]
        assert PaperTradeEventType.SESSION_STARTED in kinds
        assert PaperTradeEventType.SESSION_ENDED in kinds
        end_payload = [e for e in events if e[0] == PaperTradeEventType.SESSION_ENDED][0][1]
        assert end_payload["strategy_id"] == "char_strategy"
        assert end_payload["tick_count"] == 0
        assert end_payload["trade_count"] == 0
        assert end_payload["error"] is None


# ---------------------------------------------------------------------------
# TradeRecord mapping & result assembly
# ---------------------------------------------------------------------------


def _make_paper_trade(
    *,
    intent_type: str = "SWAP",
    gas_cost_usd: Decimal = Decimal("1.25"),
    tokens_in: dict[str, Decimal] | None = None,
    tokens_out: dict[str, Decimal] | None = None,
    token_prices_usd: dict[str, Decimal] | None = None,
    metadata: dict[str, Any] | None = None,
    tx_hash: str = "0xabc",
    protocol: str = "uniswap_v3",
) -> PaperTrade:
    return PaperTrade(
        timestamp=datetime.now(UTC),
        block_number=12345,
        intent={"type": intent_type},
        tx_hash=tx_hash,
        gas_used=100000,
        gas_cost_usd=gas_cost_usd,
        tokens_in=tokens_in or {"USDC": Decimal("100")},
        tokens_out=tokens_out or {"WETH": Decimal("0.03")},
        protocol=protocol,
        intent_type=intent_type,
        metadata=metadata or {"amount_usd": "100.0"},
        token_prices_usd=token_prices_usd or {},
    )


class TestResultAssembly:
    @pytest.mark.asyncio
    async def test_basic_result_shape(self) -> None:
        trader = _make_trader()
        _install_fast_run_harness(trader)
        result = await trader.run(_MockStrategy(), duration_seconds=0.0, max_ticks=0)

        assert result.engine == BacktestEngine.PAPER
        assert result.strategy_id == "char_strategy"
        assert result.chain == "arbitrum"
        assert result.initial_capital_usd == Decimal("10000")
        assert result.run_started_at is not None
        assert result.run_ended_at is not None
        assert result.run_duration_seconds >= 0.0
        assert result.backtest_id == trader._backtest_id
        assert isinstance(result.config, dict)
        # institutional_compliance flips when fallback counters positive.
        assert result.institutional_compliance is True
        assert result.compliance_violations == []
        # fallback_usage copied.
        assert "hardcoded_price" in result.fallback_usage

    @pytest.mark.asyncio
    async def test_trade_records_mapped_from_paper_trades(self) -> None:
        trader = _make_trader()

        async def _add_trade(self_: Any, strategy: Any, spy: dict) -> None:
            self_._trades.append(
                _make_paper_trade(
                    intent_type="SWAP",
                    gas_cost_usd=Decimal("2.5"),
                    tokens_in={"WETH": Decimal("0.03")},
                    tokens_out={"USDC": Decimal("100")},
                    # Prices chosen so net_token_flow_usd == (0.03 * 3500 - 100 * 1) == 5.0
                    token_prices_usd={"WETH": Decimal("3500"), "USDC": Decimal("1")},
                    metadata={"amount_usd": "100.0"},
                    tx_hash="0xfeed",
                    protocol="uniswap_v3",
                )
            )

        _install_fast_run_harness(trader, tick_behaviour=_add_trade)
        result = await trader.run(_MockStrategy(), duration_seconds=3600.0, max_ticks=1)
        assert len(result.trades) == 1
        rec = result.trades[0]
        assert rec.intent_type == IntentType.SWAP
        assert rec.executed_price == Decimal("0")
        assert rec.fee_usd == Decimal("0")
        assert rec.slippage_usd == Decimal("0")
        assert rec.gas_cost_usd == Decimal("2.5")
        assert rec.pnl_usd == Decimal("5.0")  # pre-gas: 0.03*3500 - 100*1
        assert rec.success is True
        assert rec.amount_usd == Decimal("100.0")
        assert rec.protocol == "uniswap_v3"
        assert rec.tokens == ["WETH", "USDC"]
        assert rec.tx_hash == "0xfeed"
        assert rec.metadata == {"amount_usd": "100.0"}

    @pytest.mark.asyncio
    async def test_unknown_intent_type_maps_to_unknown(self) -> None:
        trader = _make_trader()

        async def _add_trade(self_: Any, strategy: Any, spy: dict) -> None:
            self_._trades.append(_make_paper_trade(intent_type=""))

        _install_fast_run_harness(trader, tick_behaviour=_add_trade)
        result = await trader.run(_MockStrategy(), duration_seconds=3600.0, max_ticks=1)
        assert result.trades[0].intent_type == IntentType.UNKNOWN

    @pytest.mark.asyncio
    async def test_equity_curve_passed_through(self) -> None:
        trader = _make_trader()
        _install_fast_run_harness(trader)
        result = await trader.run(_MockStrategy(), duration_seconds=0.0, max_ticks=0)
        # initial equity point from harness.
        assert len(result.equity_curve) == 1
        assert result.equity_curve[0].value_usd == Decimal("10000")

    @pytest.mark.asyncio
    async def test_compliance_violations_populated_from_fallback_usage(self) -> None:
        trader = _make_trader()
        _install_fast_run_harness(trader)
        trader._fallback_usage["hardcoded_price"] = 3
        trader._fallback_usage["default_gas_price"] = 1
        trader._fallback_usage["default_usd_amount"] = 2
        trader._fallback_usage["zero_output_placeholder"] = 5
        result = await trader.run(_MockStrategy(), duration_seconds=0.0, max_ticks=0)
        assert result.institutional_compliance is False
        assert len(result.compliance_violations) == 4
        assert any("Hardcoded price fallback used 3 time(s)" in v for v in result.compliance_violations)
        assert any("Default gas price fallback used 1 time(s)" in v for v in result.compliance_violations)
        assert any("Default USD amount fallback used 2 time(s)" in v for v in result.compliance_violations)
        assert any("Zero output placeholder used 5 time(s)" in v for v in result.compliance_violations)

    @pytest.mark.asyncio
    async def test_config_dict_includes_error_summary_when_present(self) -> None:
        trader = _make_trader()

        # run() calls reset_run_state() which recreates trader._error_handler,
        # so the summary must be seeded AFTER reset. Do it inside a tick
        # callback so the live handler is populated mid-run.
        async def _seed_in_tick(self_: Any, strategy: Any, spy: dict) -> None:
            try:
                raise RuntimeError("in-tick error")
            except RuntimeError as exc:
                assert self_._error_handler is not None
                self_._error_handler.handle_error(exc, context="tick")

        _install_fast_run_harness(trader, tick_behaviour=_seed_in_tick)
        result = await trader.run(_MockStrategy(), duration_seconds=3600.0, max_ticks=1)

        # Assert the optional key is present AND non-empty.
        assert "error_summary" in result.config
        assert result.config["error_summary"]  # non-empty dict

    @pytest.mark.asyncio
    async def test_trade_count_in_session_ended_event(self) -> None:
        events: list[tuple[str, dict]] = []
        trader = _make_trader(event_callback=lambda k, d: events.append((k, d)))

        async def _add_trade(self_: Any, strategy: Any, spy: dict) -> None:
            self_._trades.append(_make_paper_trade())

        _install_fast_run_harness(trader, tick_behaviour=_add_trade)
        await trader.run(_MockStrategy(), duration_seconds=3600.0, max_ticks=3)
        end = [e for e in events if e[0] == PaperTradeEventType.SESSION_ENDED][0][1]
        assert end["tick_count"] == 3
        assert end["trade_count"] == 3
