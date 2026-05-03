"""End-to-end coverage tests for the VIB-3773 teardown accounting wiring
(Phase 3, tests T9–T13).

These tests exercise ``TeardownManager._execute_intents`` *with* a populated
``TeardownRunnerHelpers`` bag — i.e. the production wiring path. They use a
fake runner that records every call, and verify:

* T9 — after a successful ``orchestrator.execute`` the manager calls
  ``runner.commit_teardown_intent`` with the right cycle id, intent,
  result, context and bundle metadata.
* T10 — happy-path 2-intent teardown produces 2 commit invocations + 2
  snapshot invocations (pre + post) with cycle ids prefixed ``teardown-``.
* T11 — Lane-C parity covered separately in the runner_teardown tests.
* T12 — when the commit helper reports ``accounting_degraded=True`` for
  intent #1, intent #2 still runs and the final ``TeardownResult`` shows
  ``accounting_degraded=True`` + ``accounting_degraded_count >= 1``.
* T13 — ``execute_teardown_via_manager``'s outer cycle-id swap is verified
  in ``runner_teardown`` integration; the commit helper's local swap is
  covered in ``test_teardown_commit``. Here we just assert the manager
  passes ``teardown-{teardown_id}`` through to ``commit``.

The TeardownManager is exercised directly (not via
``execute_teardown_via_manager``) so we don't need a full runner
test-stand. Lane-B integration (snapshot bracket + cycle-id outer swap) is
exercised by the next file (``test_runner_teardown_brackets.py``) — the two
together cover the wiring end-to-end.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.runner.teardown_commit import TeardownCommitOutcome
from almanak.framework.teardown.config import TeardownConfig
from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownMode,
    TeardownPositionSummary,
    TeardownState,
    TeardownStatus,
)
from almanak.framework.teardown.runner_helpers import TeardownRunnerHelpers
from almanak.framework.teardown.teardown_manager import TeardownManager


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


def _make_intent(intent_type_value: str = "SWAP") -> SimpleNamespace:
    """Minimal intent with attributes the teardown loop actually reads."""
    return SimpleNamespace(
        intent_type=SimpleNamespace(value=intent_type_value),
        protocol="uniswap_v3",
        chain="arbitrum",
        max_slippage=Decimal("0.005"),
        # to_dict for state persistence in skip-path
        to_dict=lambda: {"intent_type": intent_type_value},
    )


def _make_strategy() -> SimpleNamespace:
    return SimpleNamespace(
        strategy_id="strat-1",
        deployment_id="dep-1",
        chain="arbitrum",
        wallet_address="0xWALLET",
    )


def _successful_exec_result() -> SimpleNamespace:
    return SimpleNamespace(
        success=True,
        transaction_results=[SimpleNamespace(tx_hash="0xabc")],
        total_gas_used=120_000,
        gas_cost_usd="0.50",
        extracted_data={},
        error="",
    )


def _make_state(*, total_intents: int) -> TeardownState:
    now = datetime.now(UTC)
    return TeardownState(
        teardown_id="td-uuid-7",
        strategy_id="strat-1",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=total_intents,
        completed_intents=0,
        current_intent_index=0,
        started_at=now,
        updated_at=now,
    )


def _make_position_summary() -> TeardownPositionSummary:
    pos = PositionInfo(
        protocol="uniswap_v3",
        position_id="5459812",
        chain="arbitrum",
        position_type=PositionType.LP,
        value_usd=Decimal("4.0"),
    )
    return TeardownPositionSummary(
        strategy_id="strat-1",
        timestamp=datetime.now(UTC),
        positions=[pos],
        total_value_usd=Decimal("4.0"),
    )


@pytest.fixture
def fake_orchestrator() -> MagicMock:
    orch = MagicMock(name="ExecutionOrchestrator")
    orch.execute = AsyncMock(return_value=_successful_exec_result())
    return orch


@pytest.fixture
def fake_compiler() -> MagicMock:
    comp = MagicMock(name="IntentCompiler")
    bundle = SimpleNamespace(
        metadata={"expected_output_human": "1.5"},
    )
    comp.compile.return_value = SimpleNamespace(
        status=SimpleNamespace(value="SUCCESS"),
        action_bundle=bundle,
        error=None,
        is_transient=False,
        retry_after_seconds=0,
    )
    return comp


@pytest.fixture
def state_manager_mock() -> MagicMock:
    sm = MagicMock(name="state_manager")
    sm.save_teardown_state = AsyncMock()
    return sm


def _make_helpers(*, commit_outcomes: list[TeardownCommitOutcome] | None = None):
    """Build a TeardownRunnerHelpers whose ``commit`` callable returns the
    given outcomes in order. ``capture_snapshot`` is a no-op (tested
    separately in the runner_teardown integration tests).
    """
    commit_calls: list[dict] = []
    outcomes_iter = iter(
        commit_outcomes
        or [TeardownCommitOutcome(ledger_entry_id="ledger-1", accounting_degraded=False, degraded_reason=None)]
    )

    async def _commit(
        strategy,
        intent,
        *,
        execution_result,
        execution_context,
        bundle_metadata,
        teardown_cycle_id,
        **_kwargs,
    ):
        # Absorb pre_snapshot / recon kwargs added by VIB-3918 per-intent
        # balance capture so this thin test stub stays signature-compatible
        # with the real ``commit_teardown_intent``.
        commit_calls.append(
            {
                "strategy_id": strategy.strategy_id,
                "intent_type": intent.intent_type.value,
                "tx_hash": execution_result.transaction_results[0].tx_hash,
                "bundle_metadata": bundle_metadata,
                "teardown_cycle_id": teardown_cycle_id,
            }
        )
        try:
            return next(outcomes_iter)
        except StopIteration:
            return TeardownCommitOutcome(
                ledger_entry_id="ledger-x",
                accounting_degraded=False,
                degraded_reason=None,
            )

    return TeardownRunnerHelpers(commit=_commit, capture_snapshot=None), commit_calls


# ---------------------------------------------------------------------------
# T9 — manager passes the right args to commit_teardown_intent
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t9_manager_invokes_commit_with_correct_args(
    fake_orchestrator, fake_compiler, state_manager_mock
):
    helpers, commit_calls = _make_helpers()
    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=helpers,
        config=TeardownConfig.default(),
    )

    intent = _make_intent("SWAP")
    state = _make_state(total_intents=1)
    positions = _make_position_summary()
    strategy = _make_strategy()

    result = await mgr._execute_intents(
        teardown_id=state.teardown_id,
        strategy=strategy,
        intents=[intent],
        positions=positions,
        mode=TeardownMode.SOFT,
        teardown_state=state,
        is_auto_mode=True,
    )

    assert result.success is True
    assert result.intents_succeeded == 1
    assert len(commit_calls) == 1
    call = commit_calls[0]
    assert call["intent_type"] == "SWAP"
    assert call["tx_hash"] == "0xabc"
    assert call["bundle_metadata"] == {"expected_output_human": "1.5"}
    assert call["teardown_cycle_id"] == "teardown-td-uuid-7"


# ---------------------------------------------------------------------------
# T10 — happy-path 2-intent teardown: 2 commit calls, no degradation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t10_happy_path_two_intents_two_commits(
    fake_orchestrator, fake_compiler, state_manager_mock
):
    helpers, commit_calls = _make_helpers()
    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=helpers,
        config=TeardownConfig.default(),
    )

    intents = [_make_intent("LP_CLOSE"), _make_intent("SWAP")]
    state = _make_state(total_intents=2)
    positions = _make_position_summary()
    strategy = _make_strategy()

    result = await mgr._execute_intents(
        teardown_id=state.teardown_id,
        strategy=strategy,
        intents=intents,
        positions=positions,
        mode=TeardownMode.SOFT,
        teardown_state=state,
        is_auto_mode=True,
    )

    assert result.success is True
    assert result.intents_succeeded == 2
    assert result.accounting_degraded is False
    assert result.accounting_degraded_count == 0
    assert len(commit_calls) == 2
    assert [c["intent_type"] for c in commit_calls] == ["LP_CLOSE", "SWAP"]
    # All cycle ids carry the teardown prefix.
    assert all(c["teardown_cycle_id"].startswith("teardown-") for c in commit_calls)


# ---------------------------------------------------------------------------
# T12 — degraded commit on intent #1 → intent #2 still runs, result reports it
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_t12_degraded_commit_does_not_halt_unwind(
    fake_orchestrator, fake_compiler, state_manager_mock
):
    from almanak.framework.accounting.deferred_log import DeferredWrite

    degraded_record = DeferredWrite.now(
        kind="ledger",
        strategy_id="strat-1",
        deployment_id="dep-1",
        cycle_id="teardown-td-uuid-7",
        intent_type="LP_CLOSE",
        tx_hash="0xabc",
        error="forced ledger fail",
    )
    helpers, commit_calls = _make_helpers(
        commit_outcomes=[
            TeardownCommitOutcome(
                ledger_entry_id=None,
                accounting_degraded=True,
                degraded_reason="ledger: AccountingPersistenceError",
                degraded_writes=(degraded_record,),
            ),
            TeardownCommitOutcome(
                ledger_entry_id="ledger-2",
                accounting_degraded=False,
                degraded_reason=None,
            ),
        ]
    )
    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        runner_helpers=helpers,
        config=TeardownConfig.default(),
    )

    intents = [_make_intent("LP_CLOSE"), _make_intent("SWAP")]
    state = _make_state(total_intents=2)
    positions = _make_position_summary()
    strategy = _make_strategy()

    result = await mgr._execute_intents(
        teardown_id=state.teardown_id,
        strategy=strategy,
        intents=intents,
        positions=positions,
        mode=TeardownMode.SOFT,
        teardown_state=state,
        is_auto_mode=True,
    )

    # Both intents executed on-chain — the degraded contract did NOT halt.
    assert result.intents_succeeded == 2
    assert result.success is True  # chain-side OK
    # But the result flags the degraded accounting.
    assert result.accounting_degraded is True
    assert result.accounting_degraded_count >= 1
    assert len(commit_calls) == 2


# ---------------------------------------------------------------------------
# Backward compat: when runner_helpers is None / empty, manager still works
# (legacy behaviour preserved for tests that don't construct a runner).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_no_runner_helpers_legacy_path_still_succeeds(
    fake_orchestrator, fake_compiler, state_manager_mock
):
    mgr = TeardownManager(
        orchestrator=fake_orchestrator,
        compiler=fake_compiler,
        state_manager=state_manager_mock,
        config=TeardownConfig.default(),
    )

    intent = _make_intent("SWAP")
    state = _make_state(total_intents=1)
    positions = _make_position_summary()
    strategy = _make_strategy()

    result = await mgr._execute_intents(
        teardown_id=state.teardown_id,
        strategy=strategy,
        intents=[intent],
        positions=positions,
        mode=TeardownMode.SOFT,
        teardown_state=state,
        is_auto_mode=True,
    )
    assert result.success is True
    assert result.accounting_degraded is False
    assert result.accounting_degraded_count == 0
