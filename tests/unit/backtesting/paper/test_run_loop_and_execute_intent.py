"""Unit tests for paper-trader run-loop, tick, and intent execution paths.

These tests pin the orchestration shape of the two top-level paper-trader
entry points without spinning up a real Anvil fork or RPC orchestrator.
Heavy collaborators (fork, orchestrator, valuer, snapshot, balance probes,
event emission helpers) are stubbed via the same harness used by the
characterization suite.

Coverage targets:

* ``run_loop``: state init, setup-helper invocation, loop iteration with
  ``_running``/``max_ticks`` exit, exception handling via
  ``handle_run_loop_exception``, ``finally`` teardown (cleanup + cached
  valuation + summary assembly), session-event emission.
* ``_execute_tick``: fork-recovery failure emits a balanced start/end event pair.
* ``_execute_intent``: missing-orchestrator early return, compile-failure
  branch (errors list + return None), success branch (delegates to
  ``_build_successful_trade``), non-success-result branch
  (``_record_intent_failure``), exception branch
  (``_handle_intent_exception``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.paper.config import PaperTraderConfig
from almanak.framework.backtesting.paper.engine import (
    PaperTradeEventType,
    PaperTrader,
)
from almanak.framework.backtesting.paper.models import (
    PaperTrade,
    PaperTradeError,
    PaperTradeErrorType,
    PaperTradingSummary,
)
from almanak.framework.execution.orchestrator import ExecutionPhase

# ---------------------------------------------------------------------------
# Shared mocks (mirror test_paper_trader_characterization.py shape)
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
    current_block: int | None = 12345
    stop_calls: int = 0

    async def start(self) -> None:
        self.is_running = True

    async def stop(self) -> None:
        self.is_running = False
        self.stop_calls += 1

    def get_rpc_url(self) -> str:
        return self.rpc_url


class _MockStrategy:
    deployment_id = "loop_strategy"

    async def decide(self, snapshot: Any) -> None:
        return None


def _make_config(**overrides: Any) -> PaperTraderConfig:
    kwargs: dict[str, Any] = {
        "chain": "arbitrum",
        "rpc_url": "https://arb.example/rpc",
        "deployment_id": "loop_strategy",
        "tick_interval_seconds": 0.001,
        "price_source": "coingecko",
    }
    kwargs.update(overrides)
    return PaperTraderConfig(**kwargs)


def _make_trader(config: PaperTraderConfig | None = None) -> PaperTrader:
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
        )
    trader._price_aggregator = MagicMock()
    trader._chainlink_provider = None
    trader._twap_provider = None
    trader._rsi_calculator = None
    return trader


def _install_run_loop_harness(
    trader: PaperTrader,
    *,
    tick_count_target: int | None = None,
    raise_in_loop: BaseException | None = None,
    final_simple: Decimal = Decimal("10500"),
    initial_capital: Decimal = Decimal("10000"),
) -> dict[str, Any]:
    """Stub every heavy collaborator so ``run_loop`` is fully deterministic."""
    spy: dict[str, Any] = {"order": [], "tick_calls": 0}

    async def _initialize_fork() -> None:
        spy["order"].append("init_fork")

    async def _initialize_orchestrator() -> None:
        spy["order"].append("init_orchestrator")
        trader._orchestrator = MagicMock()
        trader._orchestrator.signer = MagicMock()
        trader._orchestrator.signer.address = "0x" + "1" * 40

    def _init_portfolio_valuer() -> None:
        spy["order"].append("init_valuer")

    async def _seed_initial_market_snapshot() -> None:
        spy["order"].append("seed_snapshot")

    async def _record_equity_point() -> None:
        spy["order"].append("record_equity")

    async def _tick() -> Any:
        spy["tick_calls"] += 1
        trader._tick_count += 1
        if raise_in_loop is not None and spy["tick_calls"] == 1:
            raise raise_in_loop
        if tick_count_target is not None and spy["tick_calls"] >= tick_count_target:
            trader._running = False
        return None

    async def _cleanup() -> None:
        spy["order"].append("cleanup")

    async def _get_portfolio_prices() -> dict[str, Decimal]:
        return {"ETH": Decimal("3000")}

    def _value_portfolio_rich() -> tuple[Decimal, Decimal, Decimal] | None:
        return None  # force fall through to simple value

    def _calculate_portfolio_value() -> Decimal:
        return final_simple

    def _calculate_initial_capital() -> Decimal:
        return initial_capital

    trader._initialize_fork = _initialize_fork  # type: ignore[method-assign]
    trader._initialize_orchestrator = _initialize_orchestrator  # type: ignore[method-assign]
    trader._init_portfolio_valuer = _init_portfolio_valuer  # type: ignore[method-assign]
    trader._seed_initial_market_snapshot = _seed_initial_market_snapshot  # type: ignore[method-assign]
    trader._record_equity_point = _record_equity_point  # type: ignore[method-assign]
    trader.tick = _tick  # type: ignore[method-assign]
    trader._cleanup = _cleanup  # type: ignore[method-assign]
    trader._get_portfolio_prices = _get_portfolio_prices  # type: ignore[method-assign]
    trader._value_portfolio_rich = _value_portfolio_rich  # type: ignore[method-assign]
    trader._calculate_portfolio_value = _calculate_portfolio_value  # type: ignore[method-assign]
    trader._calculate_initial_capital = _calculate_initial_capital  # type: ignore[method-assign]
    return spy


# ---------------------------------------------------------------------------
# run_loop
# ---------------------------------------------------------------------------


class TestRunLoopGuard:
    @pytest.mark.asyncio
    async def test_raises_if_already_running(self) -> None:
        trader = _make_trader()
        trader._running = True
        with pytest.raises(RuntimeError, match="already running"):
            await trader.run_loop(_MockStrategy(), max_ticks=0)


class TestRunLoopHappyPath:
    @pytest.mark.asyncio
    async def test_returns_summary_with_expected_shape(self) -> None:
        events: list[tuple[str, dict[str, Any]]] = []

        def _cb(kind: str, data: dict[str, Any]) -> None:
            events.append((kind, data))

        trader = _make_trader()
        trader.event_callback = _cb  # type: ignore[assignment]
        spy = _install_run_loop_harness(trader, tick_count_target=2)

        summary = await trader.run_loop(_MockStrategy(), max_ticks=5)

        assert isinstance(summary, PaperTradingSummary)
        assert summary.deployment_id == "loop_strategy"
        assert summary.chain == "arbitrum"
        # Two ticks executed; no successful trades / errors.
        assert spy["tick_calls"] == 2
        assert summary.successful_trades == 0
        assert summary.failed_trades == 0
        assert summary.total_trades == 0
        assert summary.pnl_usd == Decimal("500")  # final 10500 - initial 10000
        assert summary.valuation_source == "simple"
        # Setup ran exactly once and cleanup ran in the finally block.
        assert spy["order"][:5] == [
            "init_fork",
            "init_orchestrator",
            "init_valuer",
            "seed_snapshot",
            "record_equity",
        ]
        assert spy["order"][-1] == "cleanup"
        # Session events emitted on the way in and out.
        kinds = [k for k, _ in events]
        assert PaperTradeEventType.SESSION_STARTED in kinds
        assert PaperTradeEventType.SESSION_ENDED in kinds

    @pytest.mark.asyncio
    async def test_max_ticks_falls_back_to_config_when_arg_none(self) -> None:
        cfg = _make_config(max_ticks=3)
        trader = _make_trader(config=cfg)
        spy = _install_run_loop_harness(trader)

        summary = await trader.run_loop(_MockStrategy(), max_ticks=None)

        # config.max_ticks=3 governs the loop budget.
        assert spy["tick_calls"] == 3
        assert summary.total_trades == 0


class TestRunLoopErrorPath:
    @pytest.mark.asyncio
    async def test_loop_exception_recorded_and_summary_still_returned(self) -> None:
        trader = _make_trader()
        spy = _install_run_loop_harness(
            trader,
            raise_in_loop=RuntimeError("boom"),
        )

        summary = await trader.run_loop(_MockStrategy(), max_ticks=10)

        # ``handle_run_loop_exception`` records exactly one PaperTradeError.
        assert len(trader._errors) == 1
        err = trader._errors[0]
        assert isinstance(err, PaperTradeError)
        assert err.error_type == PaperTradeErrorType.INTERNAL_ERROR
        assert "boom" in err.error_message
        # Summary still assembles and reflects the error.
        assert summary.failed_trades == 1
        assert summary.successful_trades == 0
        assert summary.error_summary[PaperTradeErrorType.INTERNAL_ERROR.value] == 1
        # Cleanup still ran (finally).
        assert "cleanup" in spy["order"]


class TestExecuteTickLifecycle:
    @pytest.mark.asyncio
    async def test_fork_recovery_failure_still_emits_tick_end(self) -> None:
        events: list[tuple[str, dict[str, Any]]] = []

        def _cb(kind: str, data: dict[str, Any]) -> None:
            events.append((kind, data))

        trader = _make_trader()
        trader.event_callback = _cb  # type: ignore[assignment]

        async def _reset_to_latest() -> bool:
            return False

        trader.fork_manager.reset_to_latest = _reset_to_latest  # type: ignore[attr-defined]

        trade = await trader._execute_tick(_MockStrategy())

        assert trade is None
        assert [event[0] for event in events] == [
            PaperTradeEventType.TICK_STARTED,
            PaperTradeEventType.TICK_ENDED,
        ]
        assert events[-1][1]["tick_number"] == trader._tick_count
        assert "duration_seconds" in events[-1][1]

    @pytest.mark.asyncio
    async def test_fatal_tick_error_emits_error_before_reraising(self) -> None:
        events: list[tuple[str, dict[str, Any]]] = []

        trader = _make_trader()

        def _record_event(kind: str, data: dict[str, Any]) -> None:
            events.append((kind, data))

        trader.event_callback = _record_event  # type: ignore[assignment]
        trader._error_handler = MagicMock(handle_error=MagicMock(return_value=SimpleNamespace(should_stop=True)))

        async def _raise_fatal() -> bool:
            raise RuntimeError("fatal tick")

        trader._ensure_tick_fork_ready = _raise_fatal  # type: ignore[method-assign]

        with pytest.raises(RuntimeError, match="fatal tick"):
            await trader._execute_tick(_MockStrategy())

        assert [event[0] for event in events] == [
            PaperTradeEventType.TICK_STARTED,
            PaperTradeEventType.ERROR,
            PaperTradeEventType.TICK_ENDED,
        ]
        assert events[1][1] == {"error": "fatal tick", "tick_number": trader._tick_count}
        assert trader._running is False
        assert len(trader._errors) == 1


# ---------------------------------------------------------------------------
# _execute_intent
# ---------------------------------------------------------------------------


def _install_execute_intent_harness(
    trader: PaperTrader,
    *,
    compile_returns: Any = "default-bundle",
    compile_raises: BaseException | None = None,
    execute_result: Any | None = None,
    execute_raises: BaseException | None = None,
    successful_trade: PaperTrade | None = None,
) -> dict[str, Any]:
    """Stub the exact collaborators ``_execute_intent`` calls."""
    spy: dict[str, Any] = {"events": [], "balances_calls": 0, "build_calls": 0}

    trader._orchestrator = MagicMock()
    trader._orchestrator.signer = MagicMock()
    trader._orchestrator.signer.address = "0x" + "1" * 40

    if execute_raises is not None:
        async def _execute(*args: Any, **kwargs: Any) -> Any:
            raise execute_raises

        trader._orchestrator.execute = _execute
    else:

        async def _execute_ok(*args: Any, **kwargs: Any) -> Any:
            return execute_result

        trader._orchestrator.execute = _execute_ok

    def _compile(intent: Any) -> Any:
        if compile_raises is not None:
            raise compile_raises
        return compile_returns

    def _serialize(intent: Any) -> dict[str, Any]:
        return {"intent_type": "SWAP"}

    def _get_intent_type(intent: Any) -> IntentType:
        return IntentType.SWAP

    async def _snapshot_balances(wallet: str, intent: Any = None) -> dict[str, int]:
        spy["balances_calls"] += 1
        return {}

    async def _build_successful_trade(**kwargs: Any) -> PaperTrade:
        spy["build_calls"] += 1
        if successful_trade is not None:
            return successful_trade
        return _make_paper_trade()

    def _emit_event(kind: str, data: dict[str, Any]) -> None:
        spy["events"].append((kind, data))

    trader._compile_intent = _compile  # type: ignore[method-assign]
    trader._serialize_intent = _serialize  # type: ignore[method-assign]
    trader._get_intent_type = _get_intent_type  # type: ignore[method-assign]
    trader._snapshot_balances = _snapshot_balances  # type: ignore[method-assign]
    trader._build_successful_trade = _build_successful_trade  # type: ignore[method-assign]
    trader._emit_event = _emit_event  # type: ignore[method-assign]
    return spy


def _make_paper_trade() -> PaperTrade:
    return PaperTrade(
        timestamp=datetime.now(UTC),
        block_number=1,
        intent={"intent_type": "SWAP"},
        tx_hash="0xfeed",
        gas_used=21000,
        gas_cost_usd=Decimal("1"),
        tokens_in={"WETH": Decimal("0.03")},
        tokens_out={"USDC": Decimal("100")},
    )


def _make_intent() -> Any:
    return SimpleNamespace(intent_type="SWAP", from_token="USDC", to_token="WETH", amount=100)


class TestExecuteIntentNoOrchestrator:
    @pytest.mark.asyncio
    async def test_returns_none_when_orchestrator_missing(self) -> None:
        trader = _make_trader()
        trader._orchestrator = None
        snapshot = MagicMock()
        result = await trader._execute_intent(_make_intent(), _MockStrategy(), snapshot)
        assert result is None


class TestExecuteIntentCompileFailure:
    @pytest.mark.asyncio
    async def test_compile_returns_none_records_error(self) -> None:
        trader = _make_trader()
        spy = _install_execute_intent_harness(trader, compile_returns=None)
        snapshot = MagicMock()

        out = await trader._execute_intent(_make_intent(), _MockStrategy(), snapshot)

        assert out is None
        assert len(trader._errors) == 1
        err = trader._errors[0]
        assert err.error_type == PaperTradeErrorType.INTENT_INVALID
        assert "Failed to compile" in err.error_message
        # Build path NOT entered.
        assert spy["build_calls"] == 0
        # Single TRADE_EXECUTING event was emitted before the early return.
        kinds = [k for k, _ in spy["events"]]
        assert kinds == [PaperTradeEventType.TRADE_EXECUTING]


class TestExecuteIntentSuccess:
    @pytest.mark.asyncio
    async def test_success_path_returns_built_trade(self) -> None:
        trader = _make_trader()
        execution_result = SimpleNamespace(
            success=True,
            phase=SimpleNamespace(value="execute"),
            error=None,
            error_phase=None,
            transaction_results=[],
            total_gas_used=21000,
            total_gas_cost_wei=0,
            correlation_id="cid",
            position_id=None,
            swap_amounts=None,
            extracted_data={},
            extraction_warnings=[],
        )
        my_trade = _make_paper_trade()
        spy = _install_execute_intent_harness(
            trader,
            compile_returns=SimpleNamespace(metadata=None),
            execute_result=execution_result,
            successful_trade=my_trade,
        )
        # Patch enrich_result since the success branch calls it.
        with patch(
            "almanak.framework.backtesting.paper.engine.enrich_result",
            side_effect=lambda result, *a, **kw: result,
        ):
            out = await trader._execute_intent(_make_intent(), _MockStrategy(), MagicMock())

        assert out is my_trade
        assert spy["build_calls"] == 1
        # _last_execution_result populated.
        assert trader._last_execution_result is execution_result


class TestExecuteIntentNonSuccess:
    @pytest.mark.asyncio
    async def test_non_success_records_failure_and_returns_none(self) -> None:
        trader = _make_trader()
        execution_result = SimpleNamespace(
            success=False,
            phase=ExecutionPhase.SUBMISSION,
            error="reverted",
            error_phase=ExecutionPhase.SIMULATION,
            transaction_results=[],
            total_gas_used=0,
            total_gas_cost_wei=0,
            correlation_id="cid",
        )
        spy = _install_execute_intent_harness(
            trader,
            compile_returns=SimpleNamespace(metadata=None),
            execute_result=execution_result,
        )
        out = await trader._execute_intent(_make_intent(), _MockStrategy(), MagicMock())

        assert out is None
        assert len(trader._errors) == 1
        err = trader._errors[0]
        assert err.error_type == PaperTradeErrorType.SIMULATION_FAILED
        assert err.error_message == "reverted"
        # _build_successful_trade NOT called.
        assert spy["build_calls"] == 0
        # TRADE_FAILED event emitted.
        kinds = [k for k, _ in spy["events"]]
        assert PaperTradeEventType.TRADE_FAILED in kinds


class TestExecuteIntentException:
    @pytest.mark.asyncio
    async def test_exception_records_error_and_returns_none(self) -> None:
        trader = _make_trader()
        spy = _install_execute_intent_harness(
            trader,
            compile_returns=SimpleNamespace(metadata=None),
            execute_raises=RuntimeError("rpc dead"),
        )
        out = await trader._execute_intent(_make_intent(), _MockStrategy(), MagicMock())

        assert out is None
        assert len(trader._errors) == 1
        err = trader._errors[0]
        assert err.error_type == PaperTradeErrorType.INTERNAL_ERROR
        assert "rpc dead" in err.error_message
        assert err.metadata["exception_type"] == "RuntimeError"
        assert spy["build_calls"] == 0
        kinds = [k for k, _ in spy["events"]]
        assert PaperTradeEventType.TRADE_FAILED in kinds


class TestCheckOracleDivergence:
    @pytest.mark.asyncio
    async def test_no_cached_prices_returns_early(self) -> None:
        trader = _make_trader()
        trader._cached_prices = {}
        trader.fork_manager = _MockForkManager()

        with patch(
            "almanak.framework.backtesting.paper.engine._engine_helpers.resolve_chainlink_divergence_chain"
        ) as resolver, patch(
            "almanak.framework.backtesting.paper.engine.ChainlinkDataProvider"
        ) as provider_cls:
            await trader._check_oracle_divergence()

        resolver.assert_not_called()
        provider_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_fork_manager_returns_early(self) -> None:
        trader = _make_trader()
        trader._cached_prices = {"ETH": Decimal("3000")}
        trader.fork_manager = None

        with patch(
            "almanak.framework.backtesting.paper.engine._engine_helpers.resolve_chainlink_divergence_chain"
        ) as resolver, patch(
            "almanak.framework.backtesting.paper.engine.ChainlinkDataProvider"
        ) as provider_cls:
            await trader._check_oracle_divergence()

        resolver.assert_not_called()
        provider_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_chainlink_resolver_returns_none_returns_early(self) -> None:
        trader = _make_trader()
        trader._cached_prices = {"ETH": Decimal("3000")}
        trader.fork_manager = _MockForkManager(rpc_url="http://127.0.0.1:8545")

        with patch(
            "almanak.framework.backtesting.paper.engine._engine_helpers.resolve_chainlink_divergence_chain",
            return_value=None,
        ) as resolver, patch(
            "almanak.framework.backtesting.paper.engine.ChainlinkDataProvider"
        ) as provider_cls:
            await trader._check_oracle_divergence()

        resolver.assert_called_once()
        provider_cls.assert_not_called()

    @pytest.mark.asyncio
    async def test_provider_construction_failure_returns_early(self) -> None:
        trader = _make_trader()
        trader._cached_prices = {"ETH": Decimal("3000")}
        trader.fork_manager = _MockForkManager(rpc_url="http://127.0.0.1:8545")

        with patch(
            "almanak.framework.backtesting.paper.engine._engine_helpers.resolve_chainlink_divergence_chain",
            return_value="arbitrum",
        ), patch(
            "almanak.framework.backtesting.paper.engine.ChainlinkDataProvider",
            side_effect=RuntimeError("provider boom"),
        ), patch(
            "almanak.framework.backtesting.paper.engine._engine_helpers.compute_max_oracle_divergence",
            new=AsyncMock(),
        ) as compute:
            await trader._check_oracle_divergence()

        compute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_divergence_below_threshold_returns_quietly(self) -> None:
        trader = _make_trader(_make_config(oracle_divergence_threshold=0.1))
        trader._cached_prices = {"ETH": Decimal("3000")}
        trader.fork_manager = _MockForkManager(rpc_url="http://127.0.0.1:8545")

        provider = MagicMock()
        provider.close = AsyncMock(return_value=None)

        with patch(
            "almanak.framework.backtesting.paper.engine._engine_helpers.resolve_chainlink_divergence_chain",
            return_value="arbitrum",
        ), patch(
            "almanak.framework.backtesting.paper.engine.ChainlinkDataProvider",
            return_value=provider,
        ), patch(
            "almanak.framework.backtesting.paper.engine._engine_helpers.compute_max_oracle_divergence",
            new=AsyncMock(return_value=(0.02, "ETH")),
        ):
            await trader._check_oracle_divergence()

        provider.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_divergence_above_threshold_raises(self) -> None:
        trader = _make_trader(_make_config(oracle_divergence_threshold=0.05))
        trader._cached_prices = {"ETH": Decimal("3000")}
        trader.fork_manager = _MockForkManager(rpc_url="http://127.0.0.1:8545")

        provider = MagicMock()
        provider.close = AsyncMock(return_value=None)

        with patch(
            "almanak.framework.backtesting.paper.engine._engine_helpers.resolve_chainlink_divergence_chain",
            return_value="arbitrum",
        ), patch(
            "almanak.framework.backtesting.paper.engine.ChainlinkDataProvider",
            return_value=provider,
        ), patch(
            "almanak.framework.backtesting.paper.engine._engine_helpers.compute_max_oracle_divergence",
            new=AsyncMock(return_value=(0.5, "ETH")),
        ), patch(
            "almanak.framework.backtesting.paper.engine._engine_helpers.build_divergence_error_message",
            return_value="divergence too high",
        ):
            with pytest.raises(RuntimeError, match="divergence too high"):
                await trader._check_oracle_divergence()
