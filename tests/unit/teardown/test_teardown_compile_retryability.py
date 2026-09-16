from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.teardown.models import TeardownMode, TeardownPositionSummary, TeardownState, TeardownStatus
from almanak.framework.teardown.teardown_manager import TeardownManager


@pytest.mark.asyncio
async def test_transient_compilation_failure_reaches_slippage_manager_as_retryable() -> None:
    intent = MagicMock()
    intent.intent_type = "LP_CLOSE"
    intent.chain = "avalanche"
    intent.to_dict.return_value = {"type": "lp_close"}
    del intent.max_slippage

    strategy = MagicMock()
    strategy.deployment_id = "tjv2_strat"
    strategy.chain = "avalanche"

    compiler = MagicMock()
    compiler.price_oracle = None
    compiler._using_placeholders = True
    compiler.compile.return_value = MagicMock(
        status=MagicMock(value="FAILED"),
        error="Rate limited, retry after 51.72s",
        is_transient=True,
        retry_after_seconds=51.72,
    )

    captured_attempts = []

    class _CapturingSlippageManager:
        async def execute_with_escalation(self, **kwargs):
            attempt = await kwargs["execute_func"](kwargs["intent"], Decimal("0.02"))
            captured_attempts.append(attempt)
            return MagicMock(success=False, status="failed_non_retryable")

    manager = TeardownManager(compiler=compiler, orchestrator=MagicMock())
    manager.slippage_manager = _CapturingSlippageManager()

    positions = TeardownPositionSummary(
        deployment_id="tjv2_strat",
        timestamp=datetime.now(UTC),
        positions=[],
        total_value_usd=Decimal("100"),
    )
    state = TeardownState(
        teardown_id="td_1",
        deployment_id="tjv2_strat",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=1,
        completed_intents=0,
        current_intent_index=0,
        started_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        pending_intents_json="[]",
        cancel_window_until=datetime.now(UTC),
        config_json="{}",
    )

    await manager._execute_intents(
        teardown_id="td_1",
        strategy=strategy,
        intents=[intent],
        positions=positions,
        mode=TeardownMode.SOFT,
        teardown_state=state,
    )

    assert len(captured_attempts) == 1
    assert captured_attempts[0].retryable is True
    assert captured_attempts[0].disposition == "retry_same_level"
    assert captured_attempts[0].retry_after_seconds == 51.72


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message,expected_calls,expected_status",
    [
        ("RPC timeout", 3, "failed_rpc_unreachable"),
        ("unknown token metadata", 1, "failed_non_retryable"),
        ("Too little received", 1, "failed_non_retryable"),
    ],
)
async def test_balance_resolution_failure_never_widens_tolerance(message, expected_calls, expected_status):
    from almanak.framework.teardown.slippage_manager import EscalatingSlippageManager

    manager = TeardownManager(compiler=MagicMock())
    manager._attach_lp_outstanding = AsyncMock(side_effect=lambda strategy, intent: (intent, None))
    market = MagicMock()
    market.balance.side_effect = RuntimeError(message)
    strategy = SimpleNamespace(deployment_id="balance-retry", chain="arbitrum")
    intent = {"intent_type": "SWAP", "chain": "arbitrum", "from_token": "WETH", "to_token": "USDC", "amount": "all"}
    ladder = EscalatingSlippageManager()
    ladder.config.retry_delay_seconds = 0

    async def execute(current_intent, slippage):
        return await manager._prepare_execution_attempt(strategy, current_intent, slippage, None, market)

    result = await ladder.execute_with_escalation(
        intent=intent,
        position_value=Decimal("100"),
        execute_func=execute,
        intent_slippage=Decimal("0.005"),
    )
    assert result.status == expected_status
    assert market.balance.call_count == expected_calls
    assert [attempt.slippage_used for attempt in result.attempts] == [Decimal("0.005")] * expected_calls
    manager.compiler.compile.assert_not_called()
