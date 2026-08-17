"""A missing GMX index price must not drive teardown slippage escalation."""

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from almanak.connectors.gmx_v2 import market_catalog
from almanak.connectors.gmx_v2.sdk import GMXV2SDK
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import PerpCloseIntent
from almanak.framework.teardown.config import TeardownConfig
from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownPositionSummary,
    TeardownState,
    TeardownStatus,
)
from almanak.framework.teardown.slippage_manager import EscalatingSlippageManager, ExecutionAttempt
from almanak.framework.teardown.teardown_manager import TeardownManager
from tests.unit.connectors.gmx_v2.market_fixtures import market_record, prime_catalog


@pytest.fixture(autouse=True)
def _clean_gmx_market_catalog():
    market_catalog.clear()
    yield
    market_catalog.clear()


class _Intent:
    intent_type = "PERP_CLOSE"
    max_slippage = Decimal("0.01")


async def _run_ladder(*, retryable: bool) -> tuple[object, list[Decimal], list[object]]:
    config = TeardownConfig.default()
    config.retry_delay_seconds = 0
    manager = EscalatingSlippageManager(config=config)
    levels: list[Decimal] = []
    approvals: list[object] = []

    async def attempt(_intent, slippage):
        levels.append(slippage)
        return ExecutionAttempt(
            success=False,
            slippage_used=slippage,
            actual_slippage=Decimal("0"),
            error="GMX V2 PERP_CLOSE: no usable USD price for the ETH index token",
            retryable=retryable,
        )

    async def approve(request):
        approvals.append(request)
        return type("_Approval", (), {"approved": True, "action": "approve"})()

    result = await manager.execute_with_escalation(
        intent=_Intent(),
        position_value=Decimal("6"),
        execute_func=attempt,
        on_approval_needed=approve,
        teardown_id="td-vib-6254",
        deployment_id="deployment:test",
        intent_slippage=Decimal("0.01"),
    )
    return result, levels, approvals


@pytest.mark.asyncio
async def test_price_refusal_stops_before_escalation_or_human_approval() -> None:
    result, levels, approvals = await _run_ladder(retryable=False)

    assert result.status == "failed_non_retryable"
    assert set(levels) == {Decimal("0.01")}
    assert approvals == []


@pytest.mark.asyncio
async def test_retryable_negative_control_reproduces_escalation_and_approval() -> None:
    """Prove the regression test observes the exact flag that caused the incident."""
    _result, levels, approvals = await _run_ladder(retryable=True)

    assert len(set(levels)) > 1
    assert approvals


@pytest.mark.asyncio
async def test_real_gmx_compiler_price_refusal_reaches_ladder_as_non_retryable() -> None:
    """Pin compiler -> manager -> ladder wiring, not two disconnected unit claims."""
    verified_market = market_record("arbitrum", "ETH/USD")
    prime_catalog(verified_market, chain="arbitrum")

    class _MustNotExecute:
        async def execute(self, *_args, **_kwargs):
            raise AssertionError("a price-refused compilation must never reach execution")

    compiler = IntentCompiler(
        chain="arbitrum",
        wallet_address="0x" + "1" * 40,
        price_oracle={"USDC": Decimal("1")},
        rpc_url="http://127.0.0.1:1",
    )
    config = TeardownConfig.default()
    config.retry_delay_seconds = 0
    manager = TeardownManager(
        orchestrator=_MustNotExecute(),
        compiler=compiler,
        config=config,
    )
    intent = PerpCloseIntent(
        market=verified_market.market_token,
        collateral_token="USDC",
        is_long=True,
        size_usd=Decimal("1000"),
        protocol="gmx_v2",
    )
    now = datetime.now(UTC)
    state = TeardownState(
        teardown_id="td-vib-6254-integration",
        deployment_id="deployment:test",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=1,
        completed_intents=0,
        current_intent_index=0,
        started_at=now,
        updated_at=now,
        cancel_window_until=now,
        pending_intents_json="[]",
        config_json="{}",
    )
    approvals: list[object] = []
    compile_calls = 0
    real_compile = compiler.compile

    def tracked_compile(close_intent):
        nonlocal compile_calls
        compile_calls += 1
        return real_compile(close_intent)

    compiler.compile = tracked_compile  # type: ignore[method-assign]

    async def approve(request):
        approvals.append(request)
        return SimpleNamespace(approved=True, action="approve")

    with patch.object(GMXV2SDK, "get_execution_fee", return_value=100_000_000_000_000):
        result = await manager._execute_intents(
            teardown_id=state.teardown_id,
            strategy=SimpleNamespace(
                chain="arbitrum",
                deployment_id="deployment:test",
                wallet_address="0x" + "1" * 40,
            ),
            intents=[intent],
            positions=TeardownPositionSummary(
                deployment_id="deployment:test",
                timestamp=now,
                positions=[],
            ),
            mode=TeardownMode.SOFT,
            teardown_state=state,
            on_approval_needed=approve,
            price_oracle={"USDC": Decimal("1")},
        )

    assert result.success is False
    assert result.intents_failed == 1
    assert compile_calls == 1
    assert approvals == []
