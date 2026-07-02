"""VIB-5573 WI-2: transient-revert DEFERRED-requeue retry in ``_execute_intents``.

A closing intent that reverts with a vetted TRANSIENT signature (MetaMorpho
withdraw-queue ``Panic 0x11`` on a ``VAULT_REDEEM``) must be retried on the TIME
axis — re-queued to the tail and re-fired after a backoff — NOT counted failed on
first sight, and NOT retried inline (which would delay later risk-reducing
closes on the sequential execution lane). These tests drive ``_execute_intents``
with a scripted ``execute_with_escalation`` (same harness as
``test_execute_intents_receipt_block``).
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.teardown import teardown_manager as tm
from almanak.framework.teardown.models import (
    TeardownMode,
    TeardownPositionSummary,
    TeardownState,
    TeardownStatus,
)
from almanak.framework.teardown.teardown_manager import TeardownManager

_PANIC_11 = "execution reverted: Panic(17): Arithmetic overflow/underflow"


def _fail(error: str, status: str = "failed") -> SimpleNamespace:
    # Faithful to the real ExecutionResult shape: the revert text lives on the
    # last ExecutionAttempt's ``error`` (and the summarized ``message``), NOT on
    # an outer ``.error`` attribute (ExecutionResult has none). Building it this
    # way is what catches a regression to reading ``exec_result.error``.
    return SimpleNamespace(
        success=False,
        final_slippage=Decimal("0"),
        total_gas_used=0,
        transaction_results=[],
        status=status,
        message=error,
        attempts=[SimpleNamespace(error=error, success=False)],
        approval_request=None,
    )


def _ok() -> SimpleNamespace:
    receipt = SimpleNamespace(block_number=100)
    tx = SimpleNamespace(success=True, receipt=receipt, tx_hash="0xok")
    return SimpleNamespace(
        success=True,
        final_slippage=Decimal("0"),
        total_gas_used=21000,
        transaction_results=[tx],
        status="success",
        error=None,
        approval_request=None,
    )


def _state(n: int) -> TeardownState:
    now = datetime.now(UTC)
    return TeardownState(
        teardown_id="teardown-test",
        deployment_id="deployment:abc123",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=n,
        completed_intents=0,
        current_intent_index=0,
        started_at=now,
        updated_at=now,
    )


def _positions() -> TeardownPositionSummary:
    return TeardownPositionSummary(
        deployment_id="deployment:abc123", timestamp=datetime.now(UTC), positions=[]
    )


def _strategy() -> MagicMock:
    s = MagicMock()
    s.deployment_id = "deployment:abc123"
    s.name = "Test"
    s.chain = "base"
    del s._framework_record_intent_execution
    del s.on_intent_executed
    del s.save_state
    del s.flush_pending_saves
    return s


def _vault_intent():
    return SimpleNamespace(max_slippage=None, intent_type="VAULT_REDEEM", protocol="metamorpho")


async def _run(exec_results: list, intents: list) -> SimpleNamespace:
    mgr = TeardownManager()
    mgr.state_manager = None
    mgr.slippage_manager.execute_with_escalation = AsyncMock(side_effect=exec_results)
    return await mgr._execute_intents(
        teardown_id="teardown-test",
        strategy=_strategy(),
        intents=intents,
        positions=_positions(),
        mode=TeardownMode.SOFT,
        teardown_state=_state(len(intents)),
    )


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    monkeypatch.setattr(tm, "_TRANSIENT_BACKOFF_S", 0.0)


@pytest.mark.asyncio
async def test_deferred_retry_resume_floor_never_skips_pending_intent():
    # CodeRabbit (Major): with a deferred retry, completion is NON-contiguous —
    # intent 0 defers, intent 1 succeeds. The persisted resume cutoff
    # (completed_intents / current_intent_index) must NOT advance past the still-
    # pending intent 0, or resume() would skip it and strand the position. Record
    # every save and assert the cutoff stays at 0 until intent 0 actually completes.
    saves: list[tuple[int, int]] = []

    class _RecordingStateMgr:
        async def save_teardown_state(self, state) -> None:
            saves.append((state.completed_intents, state.current_intent_index))

    mgr = TeardownManager()
    mgr.state_manager = _RecordingStateMgr()
    # intent0: transient-fail then succeed; intent1: succeed.
    mgr.slippage_manager.execute_with_escalation = AsyncMock(
        side_effect=[_fail(_PANIC_11), _ok(), _ok()]
    )
    intents = [
        SimpleNamespace(max_slippage=None, intent_type="VAULT_REDEEM", protocol="metamorpho"),
        SimpleNamespace(max_slippage=None, intent_type="LP_CLOSE", protocol="sibling_lp"),
    ]
    result = await mgr._execute_intents(
        teardown_id="teardown-test",
        strategy=_strategy(),
        intents=intents,
        positions=_positions(),
        mode=TeardownMode.SOFT,
        teardown_state=_state(2),
    )
    assert result.intents_succeeded == 2
    assert result.intents_failed == 0
    assert saves, "expected progress saves"
    assert saves[-1] == (2, 2), f"final cutoff should be (2,2), got {saves[-1]}"
    # THE invariant: the resume cutoff must NEVER be 1 while intent 0 is pending —
    # a (1, …) save is exactly what would make resume() skip the deferred intent 0
    # and strand it. With the floor fix the cutoff only ever holds at 0 (intent 0
    # pending) then jumps to 2 (both done). The pre-fix code produced (1, …).
    assert all(comp != 1 and ci != 1 for comp, ci in saves), saves


@pytest.mark.asyncio
async def test_transient_revert_deferred_then_succeeds_on_retry():
    # First fire: transient Panic(17) → deferred. Retry: success. Net: 0 failed.
    result = await _run([_fail(_PANIC_11), _ok()], [_vault_intent()])
    assert result.intents_succeeded == 1
    assert result.intents_failed == 0
    assert result.success is True


@pytest.mark.asyncio
async def test_transient_revert_that_never_clears_ends_failed_after_bounded_attempts():
    # Every attempt reverts transient. Bounded by _TRANSIENT_MAX_ATTEMPTS: the
    # intent is deferred attempts 0,1,2 (=3 deferrals) then on attempt==3 is NOT
    # deferred → counted failed. So MAX_ATTEMPTS+1 executions, then FAILED.
    n = tm._TRANSIENT_MAX_ATTEMPTS + 1
    result = await _run([_fail(_PANIC_11)] * n, [_vault_intent()])
    assert result.intents_failed == 1
    assert result.intents_succeeded == 0
    assert result.success is False


@pytest.mark.asyncio
async def test_non_transient_revert_is_not_deferred():
    # An ordinary revert (not on the allowlist) fails immediately — exactly ONE
    # execution, no time-axis retry (never mask a permanent bug).
    mgr_results = [_fail("execution reverted: insufficient balance")]
    result = await _run(mgr_results, [_vault_intent()])
    assert result.intents_failed == 1
    assert result.intents_succeeded == 0


@pytest.mark.asyncio
async def test_panic_on_wrong_protocol_is_not_deferred():
    # Panic(17) but protocol is NOT a vault → classifier returns UNKNOWN → no
    # deferral (Codex over-broad-retry guard).
    intent = SimpleNamespace(max_slippage=None, intent_type="LP_CLOSE", protocol="uniswap_v3")
    result = await _run([_fail(_PANIC_11)], [intent])
    assert result.intents_failed == 1


@pytest.mark.asyncio
async def test_deferred_transient_does_not_delay_a_sibling_close():
    # A transient vault redeem (intent 0) must NOT block a later risk-reducing
    # close (intent 1). Processing order proves it: intent 0 fires (transient,
    # deferred to tail), intent 1 fires and succeeds, THEN intent 0's retry
    # fires and succeeds. So the sibling's success happens BEFORE the retry.
    order: list[str] = []

    def _tag(result, tag):
        order.append(tag)
        return result

    seq = [
        _fail(_PANIC_11),  # intent0 first attempt → transient → deferred
        _ok(),             # intent1 → success (must run before intent0 retry)
        _ok(),             # intent0 retry → success
    ]
    calls = {"i": 0}

    async def _exec(*args, **kwargs):
        idx = calls["i"]
        calls["i"] += 1
        # Record which logical intent fired by reading the passed intent.
        intent = kwargs.get("intent") or args[0]
        order.append(getattr(intent, "protocol", "?"))
        return seq[idx]

    mgr = TeardownManager()
    mgr.state_manager = None
    mgr.slippage_manager.execute_with_escalation = _exec
    intents = [
        SimpleNamespace(max_slippage=None, intent_type="VAULT_REDEEM", protocol="metamorpho"),
        SimpleNamespace(max_slippage=None, intent_type="LP_CLOSE", protocol="sibling_lp"),
    ]
    result = await mgr._execute_intents(
        teardown_id="teardown-test",
        strategy=_strategy(),
        intents=intents,
        positions=_positions(),
        mode=TeardownMode.SOFT,
        teardown_state=_state(2),
    )
    assert result.intents_failed == 0
    assert result.intents_succeeded == 2
    # The sibling LP close ran BEFORE the deferred vault retry (the retry is the
    # LAST execution), proving the transient never delayed a risk-reducing close.
    assert order == ["metamorpho", "sibling_lp", "metamorpho"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
