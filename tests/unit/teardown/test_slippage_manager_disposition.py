"""Disposition handling in EscalatingSlippageManager (VIB-4532 / VIB-4664 / VIB-4258).

These tests drive ``execute_with_escalation`` with an ``execute_func`` that
returns attempts carrying an explicit ``disposition`` and assert the manager
reacts correctly: deterministic reverts short-circuit, transport blips retry the
same level then abort, and genuine slippage still walks the ladder to approval.
"""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.teardown.config import TeardownConfig
from almanak.framework.teardown.slippage_manager import EscalatingSlippageManager, ExecutionAttempt


def _manager() -> EscalatingSlippageManager:
    # retry_delay_seconds=0 keeps the same-level/ladder retries instant.
    return EscalatingSlippageManager(config=TeardownConfig(retry_delay_seconds=0))


def _fail(slippage: Decimal, disposition: str, *, retryable: bool = True) -> ExecutionAttempt:
    return ExecutionAttempt(
        success=False,
        slippage_used=slippage,
        error="boom",
        retryable=retryable,
        disposition=disposition,
    )


class TestNonRetryable:
    @pytest.mark.asyncio
    async def test_short_circuits_in_one_attempt_no_approval(self) -> None:
        """VIB-4664: a deterministic revert terminates in 1 attempt, no approval gate."""
        manager = _manager()
        calls = 0

        async def execute_func(_intent: object, slippage: Decimal) -> ExecutionAttempt:
            nonlocal calls
            calls += 1
            return _fail(slippage, "non_retryable")

        approval = AsyncMock()
        result = await manager.execute_with_escalation(
            intent=MagicMock(),
            position_value=Decimal("100"),
            execute_func=execute_func,
            on_approval_needed=approval,
        )

        assert result.status == "failed_non_retryable"
        assert calls == 1
        approval.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_legacy_retryable_false_still_short_circuits(self) -> None:
        """retryable=False (permanent compile failures) keeps its old meaning."""
        manager = _manager()

        async def execute_func(_intent: object, slippage: Decimal) -> ExecutionAttempt:
            return _fail(slippage, "escalate", retryable=False)

        result = await manager.execute_with_escalation(
            intent=MagicMock(),
            position_value=Decimal("100"),
            execute_func=execute_func,
            on_approval_needed=None,
        )
        assert result.status == "failed_non_retryable"


class TestTransportRetrySameLevel:
    @pytest.mark.asyncio
    async def test_retries_same_level_then_aborts_without_escalation(self) -> None:
        """VIB-4258: transport errors stay at level_1 and abort, never escalating."""
        manager = _manager()
        slippages: list[Decimal] = []

        async def execute_func(_intent: object, slippage: Decimal) -> ExecutionAttempt:
            slippages.append(slippage)
            return _fail(slippage, "retry_same_level")

        approval = AsyncMock()
        result = await manager.execute_with_escalation(
            intent=MagicMock(),
            position_value=Decimal("100"),
            execute_func=execute_func,
            on_approval_needed=approval,
        )

        assert result.status == "failed_rpc_unreachable"
        # All attempts at the SAME (first) level — the ladder never advanced.
        assert set(slippages) == {Decimal("0.02")}
        # Capped at LEVEL_1 retries (3), not the full 5-attempt ladder.
        assert len(slippages) == 3
        approval.assert_not_awaited()


class TestSlippageStillEscalates:
    @pytest.mark.asyncio
    async def test_escalate_walks_ladder_to_approval(self) -> None:
        """Regression guard: genuine slippage still walks 2%/3% then pauses at 5%."""
        manager = _manager()
        slippages: list[Decimal] = []

        async def execute_func(_intent: object, slippage: Decimal) -> ExecutionAttempt:
            slippages.append(slippage)
            return _fail(slippage, "escalate")

        result = await manager.execute_with_escalation(
            intent=MagicMock(),
            position_value=Decimal("100"),
            execute_func=execute_func,
            on_approval_needed=None,  # no callback -> pauses at the approval gate
        )

        assert result.status == "paused_awaiting_approval"
        assert slippages[:3] == [Decimal("0.02")] * 3
        assert slippages[3:5] == [Decimal("0.03")] * 2
