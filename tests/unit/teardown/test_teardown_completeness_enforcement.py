"""Teardown completeness enforcement wiring + static anti-bypass (VIB-5469 / TD-11).

Two layers:

* **Behavioural** — ``TeardownManager.execute`` must FAIL LOUD (success=False,
  verification_status=FAILED) when a tracked-open position has no closing intent,
  even if every emitted intent executed and on-chain verification of the COVERED
  positions passed. A clean ``_empty_result`` success must never hide a stranded
  position (VIB-5417 / ALM-2900).

* **Static anti-bypass** — the coverage check must remain wired into the three
  teardown lanes (CLI manager, runner-via-manager helper, runner no-intents
  gate). A refactor that drops the call trips this guard before it ships.
"""

from __future__ import annotations

import inspect
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.intents import Intent
from almanak.framework.teardown.models import (
    ClosureVerification,
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
    VerificationStatus,
)
from almanak.framework.teardown.teardown_manager import TeardownManager


def _summary(positions: list[PositionInfo]) -> TeardownPositionSummary:
    return TeardownPositionSummary(
        deployment_id="spark-dep",
        timestamp=datetime.now(UTC),
        positions=positions,
    )


def _supply_and_borrow() -> TeardownPositionSummary:
    return _summary(
        [
            PositionInfo(
                position_type=PositionType.SUPPLY,
                position_id="spark_wsteth_collateral",
                chain="ethereum",
                protocol="spark",
                value_usd=Decimal("100"),
                details={"asset": "wstETH"},
            ),
            PositionInfo(
                position_type=PositionType.BORROW,
                position_id="spark_dai_debt",
                chain="ethereum",
                protocol="spark",
                value_usd=Decimal("50"),
                details={"asset": "DAI"},
            ),
        ]
    )


def _make_strategy(positions: TeardownPositionSummary, intents: list) -> MagicMock:
    strategy = MagicMock()
    strategy.deployment_id = "spark-dep"
    strategy.name = "Spark"
    strategy.chain = "ethereum"
    strategy.uses_safe_wallet = False
    strategy.pause = AsyncMock()
    strategy.get_open_positions.return_value = positions
    strategy.generate_teardown_intents.return_value = intents
    return strategy


def _executing_manager() -> TeardownManager:
    """A manager whose orchestrator/compiler/verifier all 'succeed'."""
    compiler = MagicMock()
    compiler.price_oracle = None
    compiler._using_placeholders = True
    compiler.update_prices = lambda prices: None
    compiler.restore_prices = lambda oracle, placeholders: None

    def _compile(_intent):
        result = MagicMock()
        result.status.value = "SUCCESS"
        result.action_bundle = MagicMock()
        return result

    compiler.compile = _compile

    orchestrator = MagicMock()
    orchestrator.execute = AsyncMock(
        return_value=MagicMock(success=True, transaction_results=[], total_gas_used=50_000)
    )

    manager = TeardownManager(orchestrator=orchestrator, compiler=compiler)
    manager.cancel_window.run_cancel_window = AsyncMock(return_value=MagicMock(was_cancelled=False))
    manager.safety_guard.validate_teardown_request = MagicMock(return_value=MagicMock(all_passed=True))
    # On-chain verification of the COVERED positions reports clean closure —
    # the completeness gap must STILL fail the teardown.
    manager._verify_closure_detailed = AsyncMock(
        return_value=ClosureVerification(
            all_closed=True,
            positions_total=2,
            positions_closed=2,
            has_position_breakdown=True,
            verification_status=VerificationStatus.UNVERIFIED,
        )
    )

    # TD-15 (VIB-5473): the lanes now compose the post-condition verification with
    # a fail-closed on-chain POST-teardown reconciliation. These completeness tests
    # exercise the TD-11 coverage gate, not the chain re-read (unit-tested directly
    # in test_td15_post_teardown_verification.py), so mirror the real method's
    # no-residual-signal behaviour: return the incoming verification unchanged.
    async def _verify_against_chain(_strategy, *, verification, **_kwargs):
        return verification

    manager.verify_closure_against_chain = AsyncMock(side_effect=_verify_against_chain)
    # No consolidation side effects (real outcome so fold_consolidation_outcome works).
    from almanak.framework.teardown.consolidation import ConsolidationOutcome

    manager.run_token_consolidation = AsyncMock(return_value=ConsolidationOutcome())
    return manager


@pytest.mark.asyncio
async def test_no_intents_with_open_position_fails_loud():
    """positions open + generate_teardown_intents() -> [] must NOT be a success."""
    strategy = _make_strategy(_supply_and_borrow(), [])
    manager = _executing_manager()

    result = await manager.execute(strategy=strategy, mode="graceful")

    assert result.success is False
    assert result.verification_status == VerificationStatus.FAILED
    assert "completeness" in (result.error or "").lower()


@pytest.mark.asyncio
async def test_repay_without_withdraw_fails_loud_even_when_execution_succeeds():
    """ALM-2900: repay the borrow but never withdraw collateral → fail loud.

    Execution + on-chain verification of the covered (BORROW) leg pass, yet the
    SUPPLY leg had no closing intent, so the teardown must be marked FAILED.
    """
    positions = _supply_and_borrow()
    intents = [Intent.repay(protocol="spark", token="DAI", repay_full=True, chain="ethereum")]
    strategy = _make_strategy(positions, intents)
    manager = _executing_manager()

    result = await manager.execute(strategy=strategy, mode="graceful")

    assert result.success is False
    assert result.verification_status == VerificationStatus.FAILED
    # The risk-reducing intent still executed (inverted failure semantics).
    assert manager.orchestrator.execute.await_count >= 1


@pytest.mark.asyncio
async def test_fully_covered_teardown_succeeds():
    """A complete plan must NOT be falsely failed by the coverage gate."""
    positions = _supply_and_borrow()
    intents = [
        Intent.repay(protocol="spark", token="DAI", repay_full=True, chain="ethereum"),
        Intent.withdraw(protocol="spark", token="wstETH", amount=Decimal("0"), withdraw_all=True, chain="ethereum"),
    ]
    strategy = _make_strategy(positions, intents)
    manager = _executing_manager()
    manager._verify_closure_detailed = AsyncMock(
        return_value=ClosureVerification(
            all_closed=True,
            positions_total=2,
            positions_closed=2,
            has_position_breakdown=True,
            verification_status=VerificationStatus.CHAIN_VERIFIED,
        )
    )

    result = await manager.execute(strategy=strategy, mode="graceful")

    assert result.success is True
    assert result.verification_status == VerificationStatus.CHAIN_VERIFIED


@pytest.mark.asyncio
async def test_no_intent_completeness_returns_none_when_enumeration_raises():
    """CR/VIB-5469 fail-open fix: an UNREADABLE known set must signal None (not a
    clean 'complete' report) so the no-intents gate fails loud rather than
    certifying a clean 'no positions' teardown over an unreadable position set.

    ``resolve_open_positions_with_registry`` surfaces ``get_open_positions``
    errors by design, so a strategy that raises there must produce None.
    """
    from almanak.framework.runner.runner_teardown import _check_no_intent_completeness

    strategy = MagicMock()
    strategy.deployment_id = "dep"
    strategy._state_manager = None
    strategy.get_open_positions.side_effect = RuntimeError("enumeration backend down")

    assert await _check_no_intent_completeness(strategy) is None


# ---------------------------------------------------------------------------
# Static anti-bypass: the coverage check stays wired into every teardown lane.
# ---------------------------------------------------------------------------


def test_completeness_check_wired_into_manager_execute():
    # Scope to the ACTUAL function body (not a whole-file grep) so the guard
    # trips if the call is removed from execute() even when the symbol survives
    # in an import / comment / helper / dead code (CR hardening, VIB-5469).
    src = inspect.getsource(TeardownManager.execute)
    assert "check_intent_coverage" in src, (
        "TeardownManager.execute must call check_intent_coverage — completeness enforcement removed (VIB-5469)"
    )


def test_completeness_check_wired_into_runner_via_manager():
    from almanak.framework.runner._teardown_helpers import execute_and_verify

    src = inspect.getsource(execute_and_verify)
    assert "check_intent_coverage" in src, (
        "execute_and_verify must call check_intent_coverage — completeness enforcement removed (VIB-5469)"
    )


def test_completeness_check_wired_into_runner_no_intents_gate():
    from almanak.framework.runner.runner_teardown import execute_teardown

    src = inspect.getsource(execute_teardown)
    assert "_check_no_intent_completeness" in src, (
        "execute_teardown no-intents gate must enforce completeness (VIB-5469)"
    )
    # The no-intents gate must consult the known set BEFORE reporting a clean
    # "no positions" success.
    no_positions = src.index('"no_positions"')
    gate = src.index("_check_no_intent_completeness")
    assert gate < no_positions, "completeness gate must run before the no_positions success path"


def test_manager_failed_result_can_carry_failed_verification_status():
    """The fail-loud no-intents path stamps verification_status=FAILED."""
    # _failed_result accepts a verification_status param ...
    sig = inspect.signature(TeardownManager._failed_result)
    assert "verification_status" in sig.parameters, (
        "_failed_result must accept verification_status so a coverage failure persists FAILED (VIB-5469)"
    )
    # ... and execute()'s no-intents gate passes FAILED.
    execute_src = inspect.getsource(TeardownManager.execute)
    assert "verification_status=VerificationStatus.FAILED" in execute_src


@pytest.mark.asyncio
async def test_uncovered_positions_carried_into_denominator_when_verifier_blind():
    """VIB-5469 (CodeRabbit findings 3 & 4): when on-chain verification has NO
    position breakdown (positions_total=0), the coverage gap must still carry the
    uncovered positions into ``positions_total`` so the persisted failure record
    reflects positions_failed > 0 instead of a self-contradicting ``0/0`` clean
    count on a teardown that FAILED for a stranded position.
    """
    positions = _supply_and_borrow()  # 2 enforceable positions
    # Repay the borrow but never withdraw the collateral → SUPPLY is uncovered.
    intents = [Intent.repay(protocol="spark", token="DAI", repay_full=True, chain="ethereum")]
    strategy = _make_strategy(positions, intents)
    manager = _executing_manager()
    # Verifier is blind: no breakdown, zero denominator.
    manager._verify_closure_detailed = AsyncMock(
        return_value=ClosureVerification(
            all_closed=True,
            positions_total=0,
            positions_closed=0,
            has_position_breakdown=False,
            verification_status=VerificationStatus.UNVERIFIED,
        )
    )

    result = await manager.execute(strategy=strategy, mode="graceful")

    assert result.success is False
    assert result.verification_status == VerificationStatus.FAILED
    # The denominator now reflects the true enforceable count, and the single
    # uncovered SUPPLY leg is recorded as not-closed.
    assert result.positions_total == 2
    assert result.positions_closed == 0
