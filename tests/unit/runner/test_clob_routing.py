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


def _make_runner(**overrides) -> StrategyRunner:
    defaults = dict(
        price_oracle=MagicMock(),
        balance_provider=MagicMock(),
        execution_orchestrator=MagicMock(),
        state_manager=MagicMock(),
        alert_manager=None,
    )
    defaults.update(overrides)
    return StrategyRunner(**defaults)


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
    strategy.strategy_id = "test_strat"
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
        "order_payload": {"token_id": "0x123", "side": "BUY", "size": "50"},
        "intent_id": "pred-intent-001",
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

    orch = MagicMock()
    orch.execute = AsyncMock()
    orch.tx_risk_config = None

    runner = _make_runner(execution_orchestrator=orch)

    mock_clob_handler = MagicMock()
    mock_clob_handler.can_handle.return_value = True
    mock_clob_handler.execute = AsyncMock(
        return_value=ClobExecutionResult(
            success=True,
            order_id="order-abc-123",
            status=ClobOrderStatus.LIVE,
        )
    )

    mock_polymarket_config = MagicMock()
    mock_polymarket_config.wallet_address = "0xPOLYWALLET1234567890"

    patches = _common_patches(clob_bundle)
    patches.extend([
        patch(
            "almanak.framework.connectors.polymarket.PolymarketConfig.from_env",
            return_value=mock_polymarket_config,
        ),
        patch(
            "almanak.framework.execution.clob_handler.ClobActionHandler",
            return_value=mock_clob_handler,
        ),
        patch(
            "almanak.framework.connectors.polymarket.clob_client.ClobClient",
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
    assert result.execution_result.extracted_data["clob_status"] == ClobOrderStatus.LIVE.value


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

    orch = MagicMock()
    orch.execute = AsyncMock()
    orch.tx_risk_config = None

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

    mock_polymarket_config = MagicMock()
    mock_polymarket_config.wallet_address = "0xPOLYWALLET1234567890"

    patches = _common_patches(clob_bundle)
    patches.extend([
        patch(
            "almanak.framework.connectors.polymarket.PolymarketConfig.from_env",
            return_value=mock_polymarket_config,
        ),
        patch(
            "almanak.framework.execution.clob_handler.ClobActionHandler",
            return_value=mock_clob_handler,
        ),
        patch(
            "almanak.framework.connectors.polymarket.clob_client.ClobClient",
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
            "almanak.framework.connectors.polymarket.PolymarketConfig.from_env",
        ) as mock_from_env,
    ):
        result = await runner._execute_single_chain(
            strategy=strategy,
            intent=intent,
            start_time=datetime.now(UTC),
        )

    # For non-polygon chain, PolymarketConfig.from_env is never called
    mock_from_env.assert_not_called()
    orch.execute.assert_awaited_once()
    assert result.status == IterationStatus.SUCCESS
