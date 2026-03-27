"""Tests for intent_slippage floor in EscalatingSlippageManager.

Validates that when a strategy's teardown intent specifies a max_slippage
higher than the default auto-approve levels (2%, 3%), the escalation manager
injects an auto-approve level at that slippage. This fixes the Pendle YT
teardown failure (VIB-1912) where the AMM needs 15% slippage but the
escalation manager only auto-approves up to 3%.
"""

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.teardown.config import TeardownConfig
from almanak.framework.teardown.slippage_manager import (
    EscalatingSlippageManager,
    ExecutionAttempt,
)


def _make_manager(absolute_max_slippage: Decimal | None = None) -> EscalatingSlippageManager:
    """Create a manager with default config, optionally overriding absolute max."""
    if absolute_max_slippage is not None:
        config = TeardownConfig(absolute_max_slippage=absolute_max_slippage)
        return EscalatingSlippageManager(config=config)
    return EscalatingSlippageManager()


def _success_attempt(slippage: Decimal) -> ExecutionAttempt:
    return ExecutionAttempt(success=True, slippage_used=slippage, actual_slippage=slippage)


def _fail_attempt(slippage: Decimal) -> ExecutionAttempt:
    return ExecutionAttempt(success=False, slippage_used=slippage, error="INSUFFICIENT_TOKEN_OUT")


class TestIntentSlippageFloor:
    """Test that intent_slippage creates auto-approve level above default ladder."""

    @pytest.mark.asyncio
    async def test_no_intent_slippage_uses_default_ladder(self) -> None:
        """Without intent_slippage, uses default 2%/3% auto-approve ladder."""
        manager = _make_manager()
        call_count = 0
        slippages_used = []

        async def execute_func(intent, slippage):
            nonlocal call_count
            call_count += 1
            slippages_used.append(slippage)
            return _fail_attempt(slippage)

        result = await manager.execute_with_escalation(
            intent=MagicMock(),
            position_value=Decimal("100"),
            execute_func=execute_func,
            on_approval_needed=None,  # No approval callback -> will pause
        )

        # Should try 2% (3 retries) + 3% (2 retries) = 5 attempts, then pause at 5%
        assert result.status == "paused_awaiting_approval"
        assert call_count == 5
        assert slippages_used[:3] == [Decimal("0.02")] * 3
        assert slippages_used[3:5] == [Decimal("0.03")] * 2

    @pytest.mark.asyncio
    async def test_intent_slippage_injects_auto_approve_level(self) -> None:
        """Intent slippage of 15% injects an auto-approve level between 3% and 5%."""
        manager = _make_manager(absolute_max_slippage=Decimal("0.20"))
        slippages_used = []

        async def execute_func(intent, slippage):
            slippages_used.append(slippage)
            # Succeed only at 15%
            if slippage >= Decimal("0.15"):
                return _success_attempt(slippage)
            return _fail_attempt(slippage)

        result = await manager.execute_with_escalation(
            intent=MagicMock(),
            position_value=Decimal("100"),
            execute_func=execute_func,
            intent_slippage=Decimal("0.15"),
        )

        assert result.success
        # Should have tried 2% (3x), 3% (2x), 5% (1x), 8% (1x), then 15% (succeeds)
        # All levels up to 15% are auto-approved and sorted monotonically
        assert Decimal("0.15") in slippages_used
        assert result.final_slippage == Decimal("0.15")

    @pytest.mark.asyncio
    async def test_intent_slippage_below_auto_approve_no_injection(self) -> None:
        """Intent slippage <= max auto-approve (3%) does NOT inject extra level."""
        manager = _make_manager()
        slippages_used = []

        async def execute_func(intent, slippage):
            slippages_used.append(slippage)
            return _fail_attempt(slippage)

        result = await manager.execute_with_escalation(
            intent=MagicMock(),
            position_value=Decimal("100"),
            execute_func=execute_func,
            on_approval_needed=None,
            intent_slippage=Decimal("0.02"),  # Same as level 1
        )

        # Should behave exactly as default: 2% (3x), 3% (2x), then pause at 5%
        assert result.status == "paused_awaiting_approval"
        assert len(slippages_used) == 5
        # No 2% injected level
        unique_slippages = sorted(set(slippages_used))
        assert unique_slippages == [Decimal("0.02"), Decimal("0.03")]

    @pytest.mark.asyncio
    async def test_intent_slippage_none_ignored(self) -> None:
        """None intent_slippage is ignored (backward compatible)."""
        manager = _make_manager()
        call_count = 0

        async def execute_func(intent, slippage):
            nonlocal call_count
            call_count += 1
            return _fail_attempt(slippage)

        result = await manager.execute_with_escalation(
            intent=MagicMock(),
            position_value=Decimal("100"),
            execute_func=execute_func,
            on_approval_needed=None,
            intent_slippage=None,
        )

        assert result.status == "paused_awaiting_approval"
        assert call_count == 5  # Default ladder

    @pytest.mark.asyncio
    async def test_intent_slippage_zero_ignored(self) -> None:
        """Zero intent_slippage is ignored."""
        manager = _make_manager()
        call_count = 0

        async def execute_func(intent, slippage):
            nonlocal call_count
            call_count += 1
            return _fail_attempt(slippage)

        result = await manager.execute_with_escalation(
            intent=MagicMock(),
            position_value=Decimal("100"),
            execute_func=execute_func,
            on_approval_needed=None,
            intent_slippage=Decimal("0"),
        )

        assert result.status == "paused_awaiting_approval"
        assert call_count == 5

    @pytest.mark.asyncio
    async def test_intent_slippage_clamped_to_absolute_max(self) -> None:
        """Intent slippage exceeding absolute_max_slippage (10%) is clamped."""
        manager = _make_manager()
        slippages_used = []

        async def execute_func(intent, slippage):
            slippages_used.append(slippage)
            # Succeed at 10% (the clamped value)
            if slippage >= Decimal("0.10"):
                return _success_attempt(slippage)
            return _fail_attempt(slippage)

        result = await manager.execute_with_escalation(
            intent=MagicMock(),
            position_value=Decimal("100"),
            execute_func=execute_func,
            intent_slippage=Decimal("0.50"),  # 50% -- way too high
        )

        # Should clamp to 10% (absolute_max_slippage default) and succeed there
        assert result.success
        assert result.final_slippage == Decimal("0.10")
        # 50% should never appear in the ladder
        assert Decimal("0.50") not in slippages_used
        assert Decimal("0.10") in slippages_used

    @pytest.mark.asyncio
    async def test_injected_level_has_retries(self) -> None:
        """Injected auto-approve level at intent slippage has 1 retry."""
        manager = _make_manager(absolute_max_slippage=Decimal("0.20"))
        slippages_at_15 = []

        async def execute_func(intent, slippage):
            if slippage == Decimal("0.15"):
                slippages_at_15.append(slippage)
            return _fail_attempt(slippage)

        result = await manager.execute_with_escalation(
            intent=MagicMock(),
            position_value=Decimal("100"),
            execute_func=execute_func,
            on_approval_needed=None,
            intent_slippage=Decimal("0.15"),
        )

        # After default ladder + auto-approved intermediate levels, should try 15% with 1 retry
        assert len(slippages_at_15) == 1
        # All levels up to 15% are auto-approved, so all are exhausted
        assert result.status == "failed_manual_intervention_required"

    @pytest.mark.asyncio
    async def test_escalation_is_monotonic(self) -> None:
        """All slippage attempts are in non-decreasing order (no de-escalation)."""
        manager = _make_manager(absolute_max_slippage=Decimal("0.20"))
        slippages_used = []

        async def execute_func(intent, slippage):
            slippages_used.append(slippage)
            return _fail_attempt(slippage)

        await manager.execute_with_escalation(
            intent=MagicMock(),
            position_value=Decimal("100"),
            execute_func=execute_func,
            on_approval_needed=None,
            intent_slippage=Decimal("0.15"),
        )

        # Verify monotonic: each slippage >= previous
        for i in range(1, len(slippages_used)):
            assert slippages_used[i] >= slippages_used[i - 1], (
                f"Non-monotonic at index {i}: {slippages_used[i - 1]} -> {slippages_used[i]}"
            )
        # Verify all expected levels are present
        unique = sorted(set(slippages_used))
        assert unique == [Decimal("0.02"), Decimal("0.03"), Decimal("0.05"), Decimal("0.08"), Decimal("0.15")]

    @pytest.mark.asyncio
    async def test_intermediate_levels_auto_approved(self) -> None:
        """Levels between default auto-approve (3%) and intent (15%) are auto-approved."""
        manager = _make_manager(absolute_max_slippage=Decimal("0.20"))
        slippages_used = []

        async def execute_func(intent, slippage):
            slippages_used.append(slippage)
            # Succeed at 5% — which normally requires manual approval
            if slippage >= Decimal("0.05"):
                return _success_attempt(slippage)
            return _fail_attempt(slippage)

        result = await manager.execute_with_escalation(
            intent=MagicMock(),
            position_value=Decimal("100"),
            execute_func=execute_func,
            on_approval_needed=None,  # No callback — would pause if not auto-approved
            intent_slippage=Decimal("0.15"),
        )

        # 5% should succeed without approval because intent_slippage=15% overrides it
        assert result.success
        assert result.final_slippage == Decimal("0.05")

    @pytest.mark.asyncio
    async def test_pendle_yt_scenario_succeeds(self) -> None:
        """Simulates the Pendle YT teardown: AMM needs ~15% slippage.

        Uses a config with 20% absolute_max to allow the 15% intent_slippage
        (real Pendle strategies would configure this in their TeardownConfig).
        """
        manager = _make_manager(absolute_max_slippage=Decimal("0.20"))

        async def pendle_amm_execute(intent, slippage):
            # Pendle YT AMM needs at least 10% slippage to fill
            if slippage >= Decimal("0.10"):
                return _success_attempt(slippage)
            return ExecutionAttempt(
                success=False,
                slippage_used=slippage,
                error="INSUFFICIENT_TOKEN_OUT",
            )

        # Without intent_slippage: would pause at 5% (needs approval)
        result_no_floor = await manager.execute_with_escalation(
            intent=MagicMock(),
            position_value=Decimal("10"),
            execute_func=pendle_amm_execute,
            on_approval_needed=None,
        )
        assert not result_no_floor.success
        assert result_no_floor.status == "paused_awaiting_approval"

        # With intent_slippage=0.15: succeeds at the injected 15% level
        result_with_floor = await manager.execute_with_escalation(
            intent=MagicMock(),
            position_value=Decimal("10"),
            execute_func=pendle_amm_execute,
            on_approval_needed=None,
            intent_slippage=Decimal("0.15"),
        )
        assert result_with_floor.success
        assert result_with_floor.final_slippage == Decimal("0.15")
