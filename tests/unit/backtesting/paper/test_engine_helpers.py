"""Unit tests for ``almanak/framework/backtesting/paper/_engine_helpers.py``.

These tests drive the helpers in isolation (no PaperTrader construction) so
each phase-level helper can be verified independently of the main entry
point. The characterization suite in
``test_paper_trader_characterization.py`` tests the assembled behaviour.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import pytest

from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    EquityPoint,
    IntentType,
    TradeRecord,
)
from almanak.framework.backtesting.paper import _engine_helpers
from almanak.framework.backtesting.paper.config import (
    ForkLifecycle,
    PaperTraderConfig,
)
from almanak.framework.backtesting.paper.models import PaperTrade

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


class _FakeErrorHandler:
    def __init__(self, should_stop: bool = True) -> None:
        self.calls: list[tuple[Exception, str]] = []
        self.should_stop = should_stop
        self._summary: dict[str, Any] = {}

    def handle_error(self, exc: Exception, context: str) -> SimpleNamespace:
        self.calls.append((exc, context))
        return SimpleNamespace(should_stop=self.should_stop)

    def get_error_summary(self) -> dict[str, Any]:
        return self._summary


def _make_fake_trader(
    *,
    running: bool = False,
    config: PaperTraderConfig | None = None,
    equity_curve: list[EquityPoint] | None = None,
    rich_value: tuple[Decimal, Decimal, Decimal] | None = None,
    simple_value: Decimal = Decimal("1000"),
    error_handler: _FakeErrorHandler | None = None,
    tick_count: int = 0,
    fork_lifecycle: ForkLifecycle | None = None,
    position_reconciler_enabled: bool = False,
    should_refresh: bool = False,
) -> SimpleNamespace:
    """Build a lightweight duck-typed trader for helper tests."""
    if config is None:
        # Minimal config: tick_interval trivially small so asyncio.sleep is instant.
        if fork_lifecycle is None:
            fork_lifecycle = ForkLifecycle.ROLLING_RESET
        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb.example/rpc",
            strategy_id="t",
            tick_interval_seconds=0.001,
            price_source="coingecko",
            fork_lifecycle=fork_lifecycle,
            position_reconciler_enabled=position_reconciler_enabled,
        )

    calls: dict[str, int] = {
        "initialize_fork": 0,
        "initialize_orchestrator": 0,
        "init_portfolio_valuer": 0,
        "seed_initial_market_snapshot": 0,
        "record_equity_point": 0,
        "advance_persistent_fork": 0,
        "execute_tick": 0,
        "run_position_reconciler": 0,
        "should_refresh_fork": 0,
        "refresh_fork": 0,
        "get_portfolio_prices": 0,
        "calculate_portfolio_value": 0,
        "value_portfolio_rich": 0,
    }

    trader = SimpleNamespace(
        _backtest_id="test-backtest-id",
        _running=running,
        _current_strategy=None,
        _session_start=None,
        _trades=[],
        _errors=[],
        _equity_curve=equity_curve if equity_curve is not None else [],
        _tick_count=tick_count,
        _reconciler_discrepancies=[],
        _last_execution_result=None,
        _ticks_with_fork=0,
        _ticks_with_indicators=0,
        _ticks_with_action=0,
        _last_successful_decision_at=None,
        _last_trade_at=None,
        _error_handler=error_handler,
        _fallback_usage={"hardcoded_price": 0, "default_gas_price": 0, "default_usd_amount": 0},
        config=config,
        calls=calls,
    )

    async def _initialize_fork() -> None:
        calls["initialize_fork"] += 1

    async def _initialize_orchestrator() -> None:
        calls["initialize_orchestrator"] += 1

    def _init_portfolio_valuer() -> None:
        calls["init_portfolio_valuer"] += 1

    async def _seed_initial_market_snapshot() -> None:
        calls["seed_initial_market_snapshot"] += 1

    async def _record_equity_point() -> None:
        calls["record_equity_point"] += 1

    async def _advance_persistent_fork() -> None:
        calls["advance_persistent_fork"] += 1

    async def _execute_tick(strategy: Any) -> None:
        calls["execute_tick"] += 1

    async def _run_position_reconciler() -> None:
        calls["run_position_reconciler"] += 1

    async def _should_refresh_fork() -> bool:
        calls["should_refresh_fork"] += 1
        return should_refresh

    async def _refresh_fork() -> None:
        calls["refresh_fork"] += 1

    async def _get_portfolio_prices() -> dict[str, Decimal]:
        calls["get_portfolio_prices"] += 1
        return {"ETH": Decimal("3000")}

    def _calculate_portfolio_value() -> Decimal:
        calls["calculate_portfolio_value"] += 1
        return simple_value

    def _value_portfolio_rich() -> tuple[Decimal, Decimal, Decimal] | None:
        calls["value_portfolio_rich"] += 1
        return rich_value

    trader._initialize_fork = _initialize_fork
    trader._initialize_orchestrator = _initialize_orchestrator
    trader._init_portfolio_valuer = _init_portfolio_valuer
    trader._seed_initial_market_snapshot = _seed_initial_market_snapshot
    trader._record_equity_point = _record_equity_point
    trader._advance_persistent_fork = _advance_persistent_fork
    trader._execute_tick = _execute_tick
    trader._run_position_reconciler = _run_position_reconciler
    trader._should_refresh_fork = _should_refresh_fork
    trader._refresh_fork = _refresh_fork
    trader._get_portfolio_prices = _get_portfolio_prices
    trader._calculate_portfolio_value = _calculate_portfolio_value
    trader._value_portfolio_rich = _value_portfolio_rich
    return trader


class _Strategy:
    strategy_id = "helper_strategy"


# ---------------------------------------------------------------------------
# reset_run_state
# ---------------------------------------------------------------------------


class TestResetRunState:
    def test_clears_all_tracked_state(self) -> None:
        trader = _make_fake_trader(running=False)
        # Poison state.
        trader._trades = [1, 2, 3]
        trader._errors = [9]
        trader._equity_curve = [EquityPoint(datetime.now(UTC), Decimal("1"))]
        trader._tick_count = 77
        trader._reconciler_discrepancies = [7]
        trader._last_execution_result = object()
        trader._ticks_with_fork = 9
        trader._ticks_with_indicators = 9
        trader._ticks_with_action = 9
        trader._last_successful_decision_at = datetime.now(UTC)
        trader._last_trade_at = datetime.now(UTC)

        before = datetime.now(UTC)
        started = _engine_helpers.reset_run_state(trader, _Strategy())
        after = datetime.now(UTC)

        assert before <= started <= after
        assert trader._running is True
        assert isinstance(trader._current_strategy, _Strategy)
        assert trader._trades == []
        assert trader._errors == []
        assert trader._equity_curve == []
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
        assert trader._session_start == started

    def test_unique_backtest_ids(self) -> None:
        t = _make_fake_trader()
        _engine_helpers.reset_run_state(t, _Strategy())
        first_id = t._backtest_id
        _engine_helpers.reset_run_state(t, _Strategy())
        second_id = t._backtest_id
        assert first_id != second_id


# ---------------------------------------------------------------------------
# resolve_effective_duration
# ---------------------------------------------------------------------------


class TestResolveEffectiveDuration:
    def test_arg_wins(self) -> None:
        cfg = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb.example/rpc",
            strategy_id="t",
            tick_interval_seconds=1,
            max_ticks=60,  # config.max_duration_seconds == 60
        )
        assert _engine_helpers.resolve_effective_duration(cfg, 2.5) == 2.5

    def test_config_used_when_arg_none(self) -> None:
        cfg = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb.example/rpc",
            strategy_id="t",
            tick_interval_seconds=10,
            max_ticks=5,  # 50 seconds
        )
        assert _engine_helpers.resolve_effective_duration(cfg, None) == 50.0

    def test_default_when_both_none(self) -> None:
        cfg = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb.example/rpc",
            strategy_id="t",
            tick_interval_seconds=1,
            max_ticks=None,  # None -> config.max_duration_seconds is None
        )
        assert _engine_helpers.resolve_effective_duration(cfg, None) == 3600.0

    def test_zero_arg_preserved(self) -> None:
        cfg = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb.example/rpc",
            strategy_id="t",
            tick_interval_seconds=1,
            max_ticks=None,
        )
        # 0.0 is not None -> must be returned exactly.
        assert _engine_helpers.resolve_effective_duration(cfg, 0.0) == 0.0


# ---------------------------------------------------------------------------
# setup_session
# ---------------------------------------------------------------------------


class TestSetupSession:
    @pytest.mark.asyncio
    async def test_calls_setup_helpers_in_order(self) -> None:
        trader = _make_fake_trader()
        order: list[str] = []

        async def _init_fork() -> None:
            order.append("fork")

        async def _init_orch() -> None:
            order.append("orch")

        def _init_valuer() -> None:
            order.append("valuer")

        async def _seed() -> None:
            order.append("seed")

        async def _record() -> None:
            order.append("record")

        trader._initialize_fork = _init_fork
        trader._initialize_orchestrator = _init_orch
        trader._init_portfolio_valuer = _init_valuer
        trader._seed_initial_market_snapshot = _seed
        trader._record_equity_point = _record

        await _engine_helpers.setup_session(trader)

        assert order == ["fork", "orch", "valuer", "seed", "record"]


# ---------------------------------------------------------------------------
# run_main_loop
# ---------------------------------------------------------------------------


class TestRunMainLoop:
    @pytest.mark.asyncio
    async def test_exits_immediately_when_duration_zero(self) -> None:
        trader = _make_fake_trader(running=True)
        await _engine_helpers.run_main_loop(
            trader, _Strategy(), 0.0, None, datetime.now(UTC)
        )
        assert trader.calls["execute_tick"] == 0

    @pytest.mark.asyncio
    async def test_honours_max_ticks(self) -> None:
        trader = _make_fake_trader(running=True)
        await _engine_helpers.run_main_loop(
            trader, _Strategy(), 3600.0, 3, datetime.now(UTC)
        )
        assert trader.calls["execute_tick"] == 3
        assert trader._tick_count == 3

    @pytest.mark.asyncio
    async def test_exits_when_running_flag_cleared_externally(self) -> None:
        trader = _make_fake_trader(running=True)

        async def _tick_that_clears(strategy: Any) -> None:
            trader.calls["execute_tick"] += 1
            trader._running = False

        trader._execute_tick = _tick_that_clears
        await _engine_helpers.run_main_loop(
            trader, _Strategy(), 3600.0, 10, datetime.now(UTC)
        )
        assert trader.calls["execute_tick"] == 1

    @pytest.mark.asyncio
    async def test_persistent_skips_advance_on_first_tick(self) -> None:
        trader = _make_fake_trader(
            running=True,
            fork_lifecycle=ForkLifecycle.PERSISTENT,
            position_reconciler_enabled=True,
        )
        await _engine_helpers.run_main_loop(
            trader, _Strategy(), 3600.0, 3, datetime.now(UTC)
        )
        # 3 ticks: advance happens only before ticks 2 and 3 -> 2 calls.
        assert trader.calls["advance_persistent_fork"] == 2
        assert trader.calls["execute_tick"] == 3
        assert trader.calls["run_position_reconciler"] == 3

    @pytest.mark.asyncio
    async def test_rolling_skips_reconciler_and_advance(self) -> None:
        trader = _make_fake_trader(
            running=True,
            fork_lifecycle=ForkLifecycle.ROLLING_RESET,
            position_reconciler_enabled=True,
        )
        await _engine_helpers.run_main_loop(
            trader, _Strategy(), 3600.0, 2, datetime.now(UTC)
        )
        assert trader.calls["advance_persistent_fork"] == 0
        assert trader.calls["run_position_reconciler"] == 0
        assert trader.calls["execute_tick"] == 2

    @pytest.mark.asyncio
    async def test_refresh_fork_called_when_flagged(self) -> None:
        trader = _make_fake_trader(running=True, should_refresh=True)
        await _engine_helpers.run_main_loop(
            trader, _Strategy(), 3600.0, 2, datetime.now(UTC)
        )
        assert trader.calls["refresh_fork"] == 2

    @pytest.mark.asyncio
    async def test_duration_exit_beats_tick_limit(self) -> None:
        # run_started_at far enough in the past that end_time is already past.
        trader = _make_fake_trader(running=True)
        await _engine_helpers.run_main_loop(
            trader,
            _Strategy(),
            1.0,  # 1s effective duration
            5,
            datetime.now(UTC) - timedelta(seconds=10),  # end_time -9s => past
        )
        assert trader.calls["execute_tick"] == 0

    @pytest.mark.asyncio
    async def test_indefinite_duration_does_not_overflow(self) -> None:
        """Regression for #1839: effective_duration=inf must not raise.

        ``PaperTrader.start()`` passes ``float('inf')`` for indefinite paper-
        trade sessions. ``timedelta(seconds=float('inf'))`` raises
        ``OverflowError``; the helper must detect the sentinel and skip the
        time-limit gate entirely, relying on ``_running`` / ``max_ticks`` /
        externally-cleared flag to terminate the loop.
        """
        trader = _make_fake_trader(running=True)

        async def _tick_that_clears(strategy: Any) -> None:
            trader.calls["execute_tick"] += 1
            trader._running = False

        trader._execute_tick = _tick_that_clears

        await _engine_helpers.run_main_loop(
            trader,
            _Strategy(),
            float("inf"),
            None,  # no tick limit either — must still terminate via _running
            datetime.now(UTC),
        )
        # Exactly one tick executed before _running flipped false.
        assert trader.calls["execute_tick"] == 1
        assert trader._tick_count == 1


# ---------------------------------------------------------------------------
# classify_run_exception
# ---------------------------------------------------------------------------


class TestClassifyRunException:
    def test_cancelled_error(self) -> None:
        trader = _make_fake_trader(error_handler=_FakeErrorHandler())
        err = _engine_helpers.classify_run_exception(trader, asyncio.CancelledError())
        assert err == "Session cancelled"
        # handler NOT invoked for CancelledError.
        assert trader._error_handler.calls == []  # type: ignore[union-attr]

    def test_generic_exception_invokes_handler_stop(self) -> None:
        handler = _FakeErrorHandler(should_stop=True)
        trader = _make_fake_trader(error_handler=handler)
        exc = RuntimeError("boom")
        err = _engine_helpers.classify_run_exception(trader, exc)
        assert err == "boom"
        assert handler.calls[0][0] is exc
        assert handler.calls[0][1] == "paper_trading_session"

    def test_generic_exception_invokes_handler_continue(self) -> None:
        handler = _FakeErrorHandler(should_stop=False)
        trader = _make_fake_trader(error_handler=handler)
        err = _engine_helpers.classify_run_exception(trader, ValueError("soft"))
        assert err == "soft"
        assert len(handler.calls) == 1

    def test_generic_exception_without_handler_uses_logger_exception(self) -> None:
        trader = _make_fake_trader(error_handler=None)
        err = _engine_helpers.classify_run_exception(trader, RuntimeError("no-handler"))
        assert err == "no-handler"


# ---------------------------------------------------------------------------
# capture_final_portfolio_value
# ---------------------------------------------------------------------------


class TestCaptureFinalPortfolioValue:
    @pytest.mark.asyncio
    async def test_rich_valuation_wins(self) -> None:
        trader = _make_fake_trader(
            rich_value=(Decimal("999.99"), Decimal("500"), Decimal("499.99")),
            equity_curve=[
                EquityPoint(
                    datetime.now(UTC),
                    Decimal("50"),
                    valuation_source="simple",
                )
            ],
            simple_value=Decimal("1"),
        )
        val = await _engine_helpers.capture_final_portfolio_value(trader)
        assert val.value_usd == Decimal("999.99")
        assert val.source == "portfolio_valuer"
        # Price refresh called once.
        assert trader.calls["get_portfolio_prices"] == 1

    @pytest.mark.asyncio
    async def test_last_equity_point_used_when_rich_none(self) -> None:
        trader = _make_fake_trader(
            rich_value=None,
            equity_curve=[
                EquityPoint(
                    datetime.now(UTC),
                    Decimal("42.42"),
                    valuation_source="custom_label",
                )
            ],
            simple_value=Decimal("1"),
        )
        val = await _engine_helpers.capture_final_portfolio_value(trader)
        assert val.value_usd == Decimal("42.42")
        assert val.source == "custom_label"

    @pytest.mark.asyncio
    async def test_simple_when_empty_curve(self) -> None:
        trader = _make_fake_trader(
            rich_value=None,
            equity_curve=[],
            simple_value=Decimal("7"),
        )
        val = await _engine_helpers.capture_final_portfolio_value(trader)
        assert val.value_usd == Decimal("7")
        assert val.source == "simple"

    @pytest.mark.asyncio
    async def test_price_refresh_error_swallowed(self) -> None:
        trader = _make_fake_trader(rich_value=None, simple_value=Decimal("5"))

        async def _fail() -> dict[str, Decimal]:
            raise RuntimeError("no network")

        trader._get_portfolio_prices = _fail  # type: ignore[assignment]
        # Must not propagate.
        val = await _engine_helpers.capture_final_portfolio_value(trader)
        assert val.value_usd == Decimal("5")


# ---------------------------------------------------------------------------
# build_trade_records
# ---------------------------------------------------------------------------


_UNSET: Any = object()


def _mktrade(
    *,
    intent_type: str = "SWAP",
    gas_cost_usd: Decimal = Decimal("1"),
    tokens_in: dict[str, Decimal] | None = None,
    tokens_out: dict[str, Decimal] | None = None,
    token_prices_usd: dict[str, Decimal] | None = None,
    metadata: Any = _UNSET,
    protocol: str = "uniswap_v3",
    tx_hash: str = "0xabc",
) -> PaperTrade:
    if metadata is _UNSET:
        metadata = {"amount_usd": "100.0"}
    return PaperTrade(
        timestamp=datetime.now(UTC),
        block_number=1,
        intent={"type": intent_type},
        tx_hash=tx_hash,
        gas_used=21000,
        gas_cost_usd=gas_cost_usd,
        tokens_in=tokens_in or {"USDC": Decimal("100")},
        tokens_out=tokens_out or {"WETH": Decimal("0.03")},
        protocol=protocol,
        intent_type=intent_type,
        metadata=metadata,
        token_prices_usd=token_prices_usd or {},
    )


class TestBuildTradeRecords:
    def test_empty_list(self) -> None:
        assert _engine_helpers.build_trade_records([]) == []

    def test_basic_mapping(self) -> None:
        trade = _mktrade(
            intent_type="SWAP",
            gas_cost_usd=Decimal("2.5"),
            tokens_in={"WETH": Decimal("0.03")},
            tokens_out={"USDC": Decimal("100")},
            token_prices_usd={"WETH": Decimal("3500"), "USDC": Decimal("1")},
            metadata={"amount_usd": "100.0"},
            tx_hash="0xfeed",
            protocol="uniswap_v3",
        )
        records = _engine_helpers.build_trade_records([trade])
        assert len(records) == 1
        rec = records[0]
        assert isinstance(rec, TradeRecord)
        assert rec.intent_type == IntentType.SWAP
        assert rec.executed_price == Decimal("0")
        assert rec.fee_usd == Decimal("0")
        assert rec.slippage_usd == Decimal("0")
        assert rec.gas_cost_usd == Decimal("2.5")
        # 0.03*3500 - 100*1 = 5.0
        assert rec.pnl_usd == Decimal("5.0")
        assert rec.success is True
        assert rec.amount_usd == Decimal("100.0")
        assert rec.tokens == ["WETH", "USDC"]
        assert rec.tx_hash == "0xfeed"
        assert rec.metadata == {"amount_usd": "100.0"}
        assert rec.protocol == "uniswap_v3"

    def test_unknown_intent_when_empty(self) -> None:
        trade = _mktrade(intent_type="")
        recs = _engine_helpers.build_trade_records([trade])
        assert recs[0].intent_type == IntentType.UNKNOWN

    def test_amount_usd_default_zero(self) -> None:
        trade = _mktrade(metadata={})
        recs = _engine_helpers.build_trade_records([trade])
        assert recs[0].amount_usd == Decimal("0")

    def test_all_intent_types_roundtrip(self) -> None:
        for kind in ["SWAP", "LP_OPEN", "BORROW", "SUPPLY", "WITHDRAW", "HOLD"]:
            trade = _mktrade(intent_type=kind)
            recs = _engine_helpers.build_trade_records([trade])
            assert recs[0].intent_type == IntentType(kind)


# ---------------------------------------------------------------------------
# collect_compliance_violations
# ---------------------------------------------------------------------------


class TestCollectComplianceViolations:
    def test_empty_counters_compliant(self) -> None:
        violations, compliant = _engine_helpers.collect_compliance_violations({})
        assert violations == []
        assert compliant is True

    def test_zero_counters_compliant(self) -> None:
        violations, compliant = _engine_helpers.collect_compliance_violations(
            {
                "hardcoded_price": 0,
                "default_gas_price": 0,
                "default_usd_amount": 0,
                "zero_output_placeholder": 0,
            }
        )
        assert violations == []
        assert compliant is True

    def test_hardcoded_price_violation(self) -> None:
        violations, compliant = _engine_helpers.collect_compliance_violations(
            {"hardcoded_price": 3}
        )
        assert compliant is False
        assert any("Hardcoded price fallback used 3 time(s)" in v for v in violations)
        assert any("strict_price_mode=True" in v for v in violations)

    def test_default_gas_price_violation(self) -> None:
        violations, _ = _engine_helpers.collect_compliance_violations(
            {"default_gas_price": 1}
        )
        assert violations == ["Default gas price fallback used 1 time(s)."]

    def test_default_usd_amount_violation(self) -> None:
        violations, _ = _engine_helpers.collect_compliance_violations(
            {"default_usd_amount": 2}
        )
        assert violations == ["Default USD amount fallback used 2 time(s)."]

    def test_zero_output_placeholder_violation(self) -> None:
        violations, _ = _engine_helpers.collect_compliance_violations(
            {"zero_output_placeholder": 5}
        )
        assert len(violations) == 1
        assert "Zero output placeholder used 5 time(s)" in violations[0]
        assert "PnL calculations may be inaccurate" in violations[0]

    def test_all_violations_combined_order_stable(self) -> None:
        violations, compliant = _engine_helpers.collect_compliance_violations(
            {
                "hardcoded_price": 1,
                "default_gas_price": 2,
                "default_usd_amount": 3,
                "zero_output_placeholder": 4,
            }
        )
        assert compliant is False
        assert len(violations) == 4
        # Order must match the engine's historical order so operator dashboards
        # that grep by position do not break.
        assert "Hardcoded price" in violations[0]
        assert "Default gas price" in violations[1]
        assert "Default USD amount" in violations[2]
        assert "Zero output placeholder" in violations[3]


# ---------------------------------------------------------------------------
# assemble_backtest_result
# ---------------------------------------------------------------------------


class TestAssembleBacktestResult:
    def test_basic_passthrough(self) -> None:
        trader = _make_fake_trader()
        trader._backtest_id = "bt-1"
        started = datetime.now(UTC)
        ended = started + timedelta(seconds=30)
        metrics = BacktestMetrics(net_pnl_usd=Decimal("500"))

        result = _engine_helpers.assemble_backtest_result(
            trader=trader,
            strategy_id="s1",
            run_started_at=started,
            run_ended_at=ended,
            metrics=metrics,
            trade_records=[],
            equity_curve=[EquityPoint(started, Decimal("10000"))],
            final_value=Decimal("10500"),
            error=None,
            initial_capital=Decimal("10000"),
            config_dict={"chain": "arbitrum"},
            fallback_usage={"hardcoded_price": 0},
            compliance_violations=[],
            institutional_compliance=True,
        )

        assert result.engine == BacktestEngine.PAPER
        assert result.strategy_id == "s1"
        assert result.initial_capital_usd == Decimal("10000")
        assert result.final_capital_usd == Decimal("10500")
        assert result.run_duration_seconds == 30.0
        assert result.error is None
        assert result.backtest_id == "bt-1"
        assert result.institutional_compliance is True
        assert result.chain == "arbitrum"

    def test_error_string_propagated(self) -> None:
        trader = _make_fake_trader()
        trader._backtest_id = "bt-err"
        started = datetime.now(UTC)
        ended = started
        result = _engine_helpers.assemble_backtest_result(
            trader=trader,
            strategy_id="s",
            run_started_at=started,
            run_ended_at=ended,
            metrics=BacktestMetrics(),
            trade_records=[],
            equity_curve=[],
            final_value=Decimal("0"),
            error="Session cancelled",
            initial_capital=Decimal("0"),
            config_dict={},
            fallback_usage={},
            compliance_violations=[],
            institutional_compliance=True,
        )
        assert result.error == "Session cancelled"

    def test_non_compliant_flag(self) -> None:
        trader = _make_fake_trader()
        started = datetime.now(UTC)
        result = _engine_helpers.assemble_backtest_result(
            trader=trader,
            strategy_id="s",
            run_started_at=started,
            run_ended_at=started,
            metrics=BacktestMetrics(),
            trade_records=[],
            equity_curve=[],
            final_value=Decimal("1"),
            error=None,
            initial_capital=Decimal("1"),
            config_dict={},
            fallback_usage={"hardcoded_price": 1},
            compliance_violations=["Hardcoded price fallback used 1 time(s)."],
            institutional_compliance=False,
        )
        assert result.institutional_compliance is False
        assert len(result.compliance_violations) == 1


# ---------------------------------------------------------------------------
# FinalValuation dataclass sanity
# ---------------------------------------------------------------------------


class TestFinalValuation:
    def test_frozen_and_slotted(self) -> None:
        fv = _engine_helpers.FinalValuation(Decimal("1"), "simple")
        # Frozen dataclass raises FrozenInstanceError on mutation.
        import dataclasses

        with pytest.raises(dataclasses.FrozenInstanceError):
            fv.value_usd = Decimal("2")  # type: ignore[misc]
