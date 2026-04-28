from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

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
    strategy.strategy_id = "tjv2_strat"
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
        strategy_id="tjv2_strat",
        timestamp=datetime.now(UTC),
        positions=[],
        total_value_usd=Decimal("100"),
    )
    state = TeardownState(
        teardown_id="td_1",
        strategy_id="tjv2_strat",
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
    assert captured_attempts[0].retry_after_seconds == 51.72
