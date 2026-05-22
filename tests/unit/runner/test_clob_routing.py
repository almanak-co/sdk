"""Tests for CLOB routing in StrategyRunner._execute_single_chain.

CLOB bundles (Polymarket prediction market orders) must route to
ClobActionHandler.execute() instead of ExecutionOrchestrator.execute().
Non-CLOB bundles must continue routing to the orchestrator as before.
"""

from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.execution.clob_handler import ClobExecutionResult, ClobOrderStatus
from almanak.framework.execution.orchestrator import (
    ExecutionPhase,
    ExecutionResult,
)
from almanak.framework.runner.strategy_runner import (
    IterationStatus,
    StrategyRunner,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@contextmanager
def apply_patches(patches):
    """Context manager to start/stop a list of unittest.mock patches."""
    for p in patches:
        p.start()
    try:
        yield
    finally:
        for p in reversed(patches):
            p.stop()


def _make_state_manager() -> MagicMock:
    """Build a state_manager mock with awaitable persistence methods.

    VIB-3157: ``_write_ledger_entry`` awaits ``save_ledger_entry`` /
    ``save_position_event`` and, in live mode, re-raises unexpected failures
    as ``AccountingPersistenceError``. Plain ``MagicMock`` attributes are
    not awaitables, so the fail-closed path blows up on test shims unless
    these coroutines are explicitly provided.
    """
    sm = MagicMock()
    sm.save_ledger_entry = AsyncMock(return_value=None)
    sm.save_position_event = AsyncMock(return_value=None)
    sm.save_portfolio_snapshot = AsyncMock(return_value=1)
    sm.save_portfolio_metrics = AsyncMock(return_value=True)
    return sm


def _make_runner(**overrides) -> StrategyRunner:
    defaults = dict(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=_make_state_manager(),
        alert_manager=None,
    )
    defaults.update(overrides)
    return StrategyRunner(**defaults)


class FakeGatewayExecutionOrchestrator:
    """Minimal gateway orchestrator shim for gateway-routed CLOB tests."""

    def __init__(self) -> None:
        self._client = SimpleNamespace(is_connected=True)
        self.execute = AsyncMock()
        self.tx_risk_config = None


def _make_prediction_intent():
    """Return a mock PredictionBuyIntent-like object for Polygon."""
    intent = MagicMock()
    intent.intent_type = SimpleNamespace(value="PREDICTION_BUY")
    intent.chain = "polygon"
    intent.intent_id = "pred-intent-001"
    intent.protocol = "polymarket"
    intent.from_token = "USDC"
    intent.to_token = None
    intent.amount = 50
    intent.amount_usd = None
    intent.max_slippage = None
    return intent


def _make_swap_intent():
    """Return a mock SwapIntent-like object for Arbitrum."""
    intent = MagicMock()
    intent.intent_type = SimpleNamespace(value="SWAP")
    intent.chain = "arbitrum"
    intent.intent_id = "swap-intent-001"
    intent.from_token = "USDC"
    intent.to_token = "ETH"
    intent.amount = 100
    intent.amount_usd = None
    intent.max_slippage = None
    intent.protocol = None
    return intent


def _make_strategy(intent):
    """Build a minimal mock strategy that returns *intent* from decide()."""
    strategy = MagicMock()
    strategy.deployment_id = "test_strat"
    strategy.chain = intent.chain
    strategy.wallet_address = "0xWALLET"
    strategy.decide.return_value = intent
    strategy.create_market_snapshot.return_value = MagicMock(
        get_price_oracle_dict=MagicMock(return_value={"ETH": 3000}),
    )
    return strategy


def _make_clob_bundle():
    """ActionBundle that looks like a Polymarket CLOB order (off-chain)."""
    bundle = MagicMock()
    bundle.transactions = []
    bundle.metadata = {
        "protocol": "polymarket",
        "order_request": {
            "token_id": "0x123",
            "side": "BUY",
            "price": "0.50",
            "size": "50",
            "time_in_force": "GTC",
            "expiration": 0,
        },
        "intent_id": "pred-intent-001",
    }
    return bundle


def _make_order_request_bundle():
    """ActionBundle for the gateway-backed order_request path."""
    bundle = MagicMock()
    bundle.transactions = []
    bundle.metadata = {
        "protocol": "polymarket",
        "intent_id": "pred-intent-001",
        "order_request": {
            "token_id": "0x123",
            "side": "BUY",
            "price": "0.50",
            "size": "100",
            "time_in_force": "IOC",
            "fee_rate_bps": "1000",
        },
    }
    return bundle


def _make_onchain_bundle():
    """ActionBundle for a normal on-chain swap."""
    bundle = MagicMock()
    bundle.transactions = [MagicMock()]
    bundle.metadata = {"protocol": "uniswap_v3"}
    return bundle


class FakeStateMachine:
    """One-step state machine: emits an action bundle then completes."""

    def __init__(self, bundle, **kwargs):
        self._bundle = bundle
        self._done = False
        self._retry = 0
        self._success = False

    @property
    def is_complete(self):
        return self._done

    @property
    def success(self):
        return self._success

    @property
    def error(self):
        return None

    @property
    def retry_count(self):
        return self._retry

    def step(self):
        if not self._done:
            return SimpleNamespace(
                needs_execution=True,
                action_bundle=self._bundle,
                retry_delay=None,
                error=None,
                is_complete=False,
            )
        return SimpleNamespace(
            needs_execution=False,
            action_bundle=None,
            retry_delay=None,
            error=None,
            is_complete=True,
        )

    def set_receipt(self, receipt):
        if receipt.success:
            self._success = True
        self._done = True


def _common_patches(bundle, default_protocol="polymarket"):
    """Return a list of context managers for common patches."""
    mock_compiler = MagicMock()
    mock_compiler.default_protocol = default_protocol
    mock_compiler.clear_allowance_cache = MagicMock()

    sm = FakeStateMachine(bundle=bundle)

    return [
        patch(
            "almanak.framework.runner.strategy_runner.IntentStateMachine",
            lambda **kw: sm,
        ),
        patch(
            "almanak.framework.runner.strategy_runner.IntentCompiler",
            return_value=mock_compiler,
        ),
        patch(
            "almanak.framework.runner.strategy_runner._format_intent_for_log",
            return_value="mock_log",
        ),
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clob_bundle_routes_to_clob_handler() -> None:
    """CLOB bundles should route to ClobActionHandler, not ExecutionOrchestrator."""
    intent = _make_prediction_intent()
    strategy = _make_strategy(intent)
    clob_bundle = _make_clob_bundle()

    orch = FakeGatewayExecutionOrchestrator()

    runner = _make_runner(execution_orchestrator=orch)

    mock_clob_handler = MagicMock()
    mock_clob_handler.can_handle.return_value = True
    # Include requested_size + filled_size so to_prediction_fill() returns
    # a populated PredictionFill (rather than None) and we can assert that
    # the runner propagates it onto execution_result.prediction_fill.
    from decimal import Decimal as _D
    mock_clob_handler.execute = AsyncMock(
        return_value=ClobExecutionResult(
            success=True,
            order_id="order-abc-123",
            status=ClobOrderStatus.MATCHED,
            filled_size=_D("100"),
            avg_fill_price=_D("0.50"),
            requested_size=_D("100"),
        )
    )

    patches = _common_patches(clob_bundle)
    patches.extend([
        patch(
            "almanak.framework.execution.gateway_orchestrator.GatewayExecutionOrchestrator",
            FakeGatewayExecutionOrchestrator,
        ),
        patch(
            "almanak.framework.connectors.polymarket.gateway_client.GatewayPolymarketClient",
            return_value=object(),
        ),
        patch(
            "almanak.framework.execution.clob_handler.ClobActionHandler",
            return_value=mock_clob_handler,
        ),
    ])

    with apply_patches(patches):
        result = await runner._execute_single_chain(
            strategy=strategy,
            intent=intent,
            start_time=datetime.now(UTC),
        )

    # CLOB handler was called, not the orchestrator
    mock_clob_handler.execute.assert_awaited_once_with(clob_bundle)
    orch.execute.assert_not_awaited()

    assert result.status == IterationStatus.SUCCESS
    assert result.execution_result is not None
    assert result.execution_result.extracted_data["order_id"] == "order-abc-123"
    assert result.execution_result.extracted_data["clob_status"] == ClobOrderStatus.MATCHED.value
    # CodeRabbit #1611: verify PredictionFill propagates end-to-end so
    # strategies can read ``result.prediction_fill.filled_shares`` without
    # reaching into ClobExecutionResult.
    assert result.execution_result.prediction_fill is not None
    assert result.execution_result.prediction_fill.filled_shares == _D("100")
    assert result.execution_result.prediction_fill.requested_shares == _D("100")
    assert result.execution_result.prediction_fill.avg_fill_price == _D("0.50")
    assert result.execution_result.prediction_fill.is_fully_filled is True


@pytest.mark.asyncio
async def test_order_request_bundle_routes_to_clob_handler() -> None:
    """Gateway-backed order_request bundles should route to ClobActionHandler."""
    intent = _make_prediction_intent()
    strategy = _make_strategy(intent)
    clob_bundle = _make_order_request_bundle()

    orch = FakeGatewayExecutionOrchestrator()

    runner = _make_runner(execution_orchestrator=orch)

    mock_clob_handler = MagicMock()
    mock_clob_handler.can_handle.return_value = True
    from decimal import Decimal as _D

    mock_clob_handler.execute = AsyncMock(
        return_value=ClobExecutionResult(
            success=False,
            order_id="order-ioc-unmatched",
            status=ClobOrderStatus.FAILED,
            filled_size=_D("0"),
            avg_fill_price=None,
            requested_size=_D("100"),
            error="CLOB order rejected (status=failed)",
        )
    )

    patches = _common_patches(clob_bundle)
    patches.extend([
        patch(
            "almanak.framework.execution.gateway_orchestrator.GatewayExecutionOrchestrator",
            FakeGatewayExecutionOrchestrator,
        ),
        patch(
            "almanak.framework.connectors.polymarket.gateway_client.GatewayPolymarketClient",
            return_value=object(),
        ),
        patch(
            "almanak.framework.execution.clob_handler.ClobActionHandler",
            return_value=mock_clob_handler,
        ),
    ])

    with apply_patches(patches):
        result = await runner._execute_single_chain(
            strategy=strategy,
            intent=intent,
            start_time=datetime.now(UTC),
        )

    mock_clob_handler.execute.assert_awaited_once_with(clob_bundle)
    orch.execute.assert_not_awaited()
    assert result.status == IterationStatus.EXECUTION_FAILED
    assert result.execution_result is not None
    assert result.execution_result.extracted_data["clob_status"] == ClobOrderStatus.FAILED.value


@pytest.mark.asyncio
async def test_non_clob_bundle_routes_to_orchestrator() -> None:
    """Non-CLOB bundles (non-Polygon chain) go to the on-chain orchestrator."""
    intent = _make_swap_intent()  # chain = "arbitrum"
    strategy = _make_strategy(intent)
    onchain_bundle = _make_onchain_bundle()

    exec_result = ExecutionResult(
        success=True,
        phase=ExecutionPhase.COMPLETE,
        completed_at=datetime.now(UTC),
    )
    orch = MagicMock()
    orch.execute = AsyncMock(return_value=exec_result)
    orch.tx_risk_config = None

    runner = _make_runner(execution_orchestrator=orch)

    patches = _common_patches(onchain_bundle, default_protocol="uniswap_v3")

    with apply_patches(patches):
        result = await runner._execute_single_chain(
            strategy=strategy,
            intent=intent,
            start_time=datetime.now(UTC),
        )

    orch.execute.assert_awaited_once()
    assert result.status == IterationStatus.SUCCESS


@pytest.mark.asyncio
async def test_clob_failure_propagates() -> None:
    """Failed CLOB execution should produce a failed ExecutionResult with error and status."""
    intent = _make_prediction_intent()
    strategy = _make_strategy(intent)
    clob_bundle = _make_clob_bundle()

    orch = FakeGatewayExecutionOrchestrator()

    runner = _make_runner(execution_orchestrator=orch)

    mock_clob_handler = MagicMock()
    mock_clob_handler.can_handle.return_value = True
    mock_clob_handler.execute = AsyncMock(
        return_value=ClobExecutionResult(
            success=False,
            status=ClobOrderStatus.FAILED,
            error="Insufficient balance",
        )
    )

    patches = _common_patches(clob_bundle)
    patches.extend([
        patch(
            "almanak.framework.execution.gateway_orchestrator.GatewayExecutionOrchestrator",
            FakeGatewayExecutionOrchestrator,
        ),
        patch(
            "almanak.framework.connectors.polymarket.gateway_client.GatewayPolymarketClient",
            return_value=object(),
        ),
        patch(
            "almanak.framework.execution.clob_handler.ClobActionHandler",
            return_value=mock_clob_handler,
        ),
    ])

    with apply_patches(patches):
        result = await runner._execute_single_chain(
            strategy=strategy,
            intent=intent,
            start_time=datetime.now(UTC),
        )

    assert result.execution_result is not None
    assert result.execution_result.success is False
    assert result.execution_result.error == "Insufficient balance"
    # clob_status is always present, even when order_id is None
    assert result.execution_result.extracted_data["clob_status"] == ClobOrderStatus.FAILED.value
    # CodeRabbit #1611: when the handler has no ``requested_size`` to thread
    # through (e.g. exception before parsing, or a SELL "all" bundle),
    # ``to_prediction_fill()`` returns None and the runner leaves
    # ``execution_result.prediction_fill`` as None. Strategies should then
    # fall back to post-execution balance reads.
    assert result.execution_result.prediction_fill is None


@pytest.mark.asyncio
async def test_no_clob_handler_when_non_polygon_chain() -> None:
    """When chain is not Polygon, clob_handler stays None - all bundles go to orchestrator."""
    intent = _make_swap_intent()  # chain = "arbitrum"
    strategy = _make_strategy(intent)
    onchain_bundle = _make_onchain_bundle()

    exec_result = ExecutionResult(
        success=True,
        phase=ExecutionPhase.COMPLETE,
        completed_at=datetime.now(UTC),
    )
    orch = MagicMock()
    orch.execute = AsyncMock(return_value=exec_result)
    orch.tx_risk_config = None

    runner = _make_runner(execution_orchestrator=orch)

    patches = _common_patches(onchain_bundle, default_protocol="uniswap_v3")

    with (
        apply_patches(patches),
        patch(
            "almanak.framework.connectors.polymarket.gateway_client.GatewayPolymarketClient",
        ) as mock_gateway_client,
    ):
        result = await runner._execute_single_chain(
            strategy=strategy,
            intent=intent,
            start_time=datetime.now(UTC),
        )

    # For non-polygon chain, gateway Polymarket client is never constructed.
    mock_gateway_client.assert_not_called()
    orch.execute.assert_awaited_once()
    assert result.status == IterationStatus.SUCCESS
