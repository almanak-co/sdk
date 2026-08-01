"""An UNMEASURED teardown must not certify success (VIB-6285 / W0.1).

The defect: after a teardown the SDK could report SUCCESS with no on-chain
evidence that anything closed. Confirmed empirically — a GMX perp teardown
reported ``positions_closed=2, positions_failed=0`` while the chain showed the
positions had never existed. Two authorities are supposed to catch this and both
abstain in the same run:

* ``plan_a_reconciliation`` returns ``UNVERIFIABLE`` for PERP / VAULT / STAKE /
  TOKEN / CEX / PREDICTION ("no per-position Plan-A chain read"), and
  ``apply_post_teardown_to_verification_status`` treats ``UNVERIFIABLE`` as a
  deliberate no-op.
* The TD-14 post-condition loop ``continue``s past any hook returning
  ``unmeasured=True``.

Both success call sites then gated only on ``not verification.all_closed``, so a
teardown nothing measured certified.

The rule implemented and asserted here, GROUPED PER PROTOCOL::

    certify  iff  for EVERY protocol present in the teardown set:
                    (that protocol has >= 1 MEASURED CLOSED position)
              and  (no position anywhere is MEASURED OPEN)

Grouping is what stops a multi-protocol teardown certifying off an unrelated
protocol's evidence (a GMX perp + an Aave leg certifying on Aave's two
``DIVERGED_CLOSED`` reads while the perp is unmeasured).

Acceptance matrix (verified against real runs):

=============  =========================================  =================
Teardown       Evidence                                   Required outcome
=============  =========================================  =================
Uniswap V3 LP  Plan-A ``DIVERGED_CLOSED``                 certifies
Aave lending   Plan-A ``DIVERGED_CLOSED`` x2              certifies
Pendle PT      1 of 2 rows hook-proven, 0 measured open   certifies
GMX perp       nothing measured either way                BLOCKED (new)
GMX + Aave     only aave_v3 proven                        BLOCKED (new)
=============  =========================================  =================

The evidence set is only trustworthy because of the ``not_applicable`` change in
this same commit: the NFT-shaped uniswap_v3/v4 LP hooks used to return a bare
``closed=True`` for non-LP positions, fabricating a proof off zero chain reads.
See :class:`TestVib6285OutOfScopeHookIsNotEvidence` — none of the four acceptance
rows can catch that, which is why it has its own group.

RATCHET STAGE — knowingly incomplete. The end state is "*every position* must be
measured closed"; it is unsatisfiable until ``registry_enumeration._dedupe_key``
dedupes by physical identity (workplan W2.2), because today one physical position
is enumerated twice for non-lending/non-LP types and the phantom row can never
acquire evidence. Known hole, asserted below so it stays visible:
``test_vib6285_known_hole_is_now_scoped_to_one_protocol``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from almanak.framework.teardown.consolidation import ConsolidationOutcome
from almanak.framework.teardown.models import (
    CLOSURE_UNKNOWN_ERROR,
    ClosureVerification,
    PositionInfo,
    PositionType,
    TeardownMode,
    TeardownPositionSummary,
    TeardownResult,
    TeardownState,
    TeardownStatus,
    VerificationStatus,
)
from almanak.framework.teardown.plan_a_reconciliation import (
    PositionReconciliation,
    ReconciliationReport,
    ReconciliationVerdict,
)
from almanak.framework.teardown.teardown_manager import TeardownManager

CHAIN = "arbitrum"


# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------


def _entry(
    position_id: str,
    verdict: ReconciliationVerdict,
    *,
    protocol: str = "gmx_v2",
    chain: str = CHAIN,
    position_type: str = "PositionType.PERP",
) -> PositionReconciliation:
    return PositionReconciliation(
        position_type=position_type,
        position_id=position_id,
        chain=chain,
        protocol=protocol,
        verdict=verdict,
        detail="fixture",
    )


def _report(*entries: PositionReconciliation) -> ReconciliationReport:
    return ReconciliationReport(deployment_id="deployment:vib6285", entries=entries)


def _verification(
    *,
    positions_total: int = 2,
    positions_closed: int = 2,
    hook_proven: tuple[tuple[str, str, str], ...] = (),
    status: VerificationStatus = VerificationStatus.CHAIN_VERIFIED,
) -> ClosureVerification:
    return ClosureVerification(
        all_closed=True,
        positions_total=positions_total,
        positions_closed=positions_closed,
        has_position_breakdown=True,
        verification_status=status,
        hook_proven_position_keys=tuple(
            (p.strip().lower(), c.strip().lower(), pid.strip().lower()) for p, c, pid in hook_proven
        ),
    )


def _chain_verifier(closure_unknown: bool):
    """Stand-in for ``verify_closure_against_chain`` at the two success call sites.

    ``closure_unknown`` is DERIVED, never assignable, so the double drives it
    through the same evidence tuples production writes: a protocol present in the
    teardown set with no measured closure => unknown; the same protocol proven =>
    certified. Setting a flag directly would test a shape production cannot build.
    """
    from dataclasses import replace

    async def _verify_against_chain(_strategy, *, verification, **_kwargs):
        out = replace(
            verification,
            protocols_to_prove=("gmx_v2",),
            measured_closed_protocols=() if closure_unknown else ("gmx_v2",),
        )
        assert out.closure_unknown is closure_unknown, "fixture did not produce the intended state"
        return out

    return _verify_against_chain


def _manager_with_post_report(monkeypatch, report: ReconciliationReport | Exception) -> TeardownManager:
    mgr = TeardownManager()
    monkeypatch.setattr(mgr, "_teardown_gateway_client", lambda: MagicMock())
    monkeypatch.setattr(mgr, "_fresh_post_execution_market", lambda strategy, market: MagicMock())

    async def _fake_reconcile(**_kwargs):
        if isinstance(report, Exception):
            raise report
        return report

    monkeypatch.setattr(
        "almanak.framework.teardown.teardown_manager.reconcile_known_positions_against_chain",
        _fake_reconcile,
    )
    return mgr


def _strategy() -> SimpleNamespace:
    return SimpleNamespace(deployment_id="deployment:vib6285", _gateway_network=CHAIN)


async def _verify(mgr: TeardownManager, verification: ClosureVerification) -> ClosureVerification:
    return await mgr.verify_closure_against_chain(
        _strategy(),
        verification=verification,
        pre_execution_positions=SimpleNamespace(positions=[]),
        market=None,
    )


# ---------------------------------------------------------------------------
# The truth table on verify_closure_against_chain
# ---------------------------------------------------------------------------


class TestVib6285ClosureUnknownTruthTable:
    @pytest.mark.asyncio
    async def test_vib6285_measured_open_fails_and_is_not_unknown(self, monkeypatch):
        """MEASURED OPEN is a residual-risk claim, never an unknown one.

        Negative control on the OTHER polarity: the new signal must not
        cannibalise the existing fail-closed path.
        """
        mgr = _manager_with_post_report(
            monkeypatch,
            _report(_entry("pos-1", ReconciliationVerdict.CONFIRMED_OPEN, protocol="aave_v3")),
        )

        out = await _verify(mgr, _verification())

        assert out.all_closed is False
        assert out.verification_status is VerificationStatus.FAILED
        assert out.closure_unknown is False

    @pytest.mark.asyncio
    async def test_vib6285_measured_closed_via_plan_a_certifies(self, monkeypatch):
        """Uniswap V3 LP / Aave lending shape: Plan-A DIVERGED_CLOSED is evidence."""
        mgr = _manager_with_post_report(
            monkeypatch,
            _report(
                _entry("lp-1", ReconciliationVerdict.DIVERGED_CLOSED, protocol="uniswap_v3"),
                _entry("lp-2", ReconciliationVerdict.DIVERGED_CLOSED, protocol="uniswap_v3"),
            ),
        )

        out = await _verify(mgr, _verification())

        assert out.all_closed is True
        assert out.closure_unknown is False

    @pytest.mark.asyncio
    async def test_vib6285_measured_closed_via_hook_proof_certifies(self, monkeypatch):
        """A TD-14 hook proof is measured evidence even with a silent Plan-A read.

        VIB-5936 records a key ONLY for a hook that measured zero residual
        (unmeasured hooks are skipped before a key is recorded), so the tuple is
        exactly the measured-evidence set — never counted-by-execution.
        """
        mgr = _manager_with_post_report(
            monkeypatch,
            _report(_entry("pt-1", ReconciliationVerdict.UNVERIFIABLE, protocol="pendle")),
        )

        out = await _verify(mgr, _verification(hook_proven=(("pendle", CHAIN, "pt-1"),)))

        assert out.all_closed is True
        assert out.closure_unknown is False

    @pytest.mark.asyncio
    async def test_vib6285_gmx_nothing_measured_is_blocked(self, monkeypatch):
        """THE MOTIVATING DEFECT. Everything UNVERIFIABLE, no hook proof ⇒ unknown.

        This is the run that reported ``positions_closed=2, positions_failed=0``
        on positions that had never existed.
        """
        mgr = _manager_with_post_report(
            monkeypatch,
            _report(
                _entry("gmx-perp-eth", ReconciliationVerdict.UNVERIFIABLE),
                _entry("0xdeadbeef", ReconciliationVerdict.UNVERIFIABLE),
            ),
        )

        out = await _verify(mgr, _verification())

        assert out.closure_unknown is True
        # Only ADDS a signal — it must NOT invent a residual-open claim.
        assert out.all_closed is True

    @pytest.mark.asyncio
    async def test_vib6285_position_with_no_plan_a_entry_at_all_is_unknown(self, monkeypatch):
        """A position Plan-A never even examined still demands evidence.

        The teardown set is the UNION of the pre-execution positions and the
        Plan-A entries, so a protocol present only in the pre-exec snapshot is
        still a group that must be proven. Without the union this teardown would
        certify off an empty report — evidence by absence.
        """
        mgr = _manager_with_post_report(monkeypatch, _report())

        out = await mgr.verify_closure_against_chain(
            _strategy(),
            verification=_verification(positions_total=1, positions_closed=1),
            pre_execution_positions=SimpleNamespace(
                positions=[SimpleNamespace(protocol="gmx_v2", chain=CHAIN, position_id="perp-1")]
            ),
            market=None,
        )

        assert out.closure_unknown is True
        assert out.unproven_protocols == ("gmx_v2",)
        assert out.all_closed is True

    @pytest.mark.asyncio
    async def test_vib6285_nothing_nameable_derives_not_unknown(self, monkeypatch):
        """No positions and no entries anywhere ⇒ no protocol group to demand proof of.

        The empty ``protocols_to_prove`` tuple doubles as the "ratchet was never
        armed" sentinel — it is what every legacy path and the never-raises
        fail-open degrade to, so it MUST derive False or a reconciliation fault
        would flip a healthy teardown to failure. Pinned so a future tightening
        does not break the fail-open contract by accident.
        """
        mgr = _manager_with_post_report(monkeypatch, _report())

        out = await _verify(mgr, _verification())

        assert out.protocols_to_prove == ()
        assert out.closure_unknown is False

    @pytest.mark.asyncio
    async def test_vib6285_not_applicable_is_not_evidence_of_closure(self, monkeypatch):
        """NOT_APPLICABLE means 'this read is out of scope' — not 'it closed'."""
        mgr = _manager_with_post_report(
            monkeypatch,
            _report(_entry("pos-1", ReconciliationVerdict.NOT_APPLICABLE)),
        )

        out = await _verify(mgr, _verification())

        assert out.closure_unknown is True

    @pytest.mark.asyncio
    async def test_vib6285_reconciliation_exception_degrades_and_never_raises(self, monkeypatch):
        """A reconciliation fault must degrade to the INCOMING verification.

        The CHECK sits inside a fail-open contract; a hook/read exception must
        not fault the teardown lane, and must not synthesise ``closure_unknown``
        off a read that never ran.
        """
        mgr = _manager_with_post_report(monkeypatch, RuntimeError("gateway gone"))
        incoming = _verification()

        out = await _verify(mgr, incoming)

        assert out is incoming
        assert out.closure_unknown is False

    @pytest.mark.asyncio
    async def test_vib6285_zero_positions_is_never_unknown(self, monkeypatch):
        """A teardown with nothing to close is NOT an unverified teardown.

        The shape that matters is the LEGACY IN-MEMORY path
        (``_verify_closure_detailed`` last-resort branch): it returns
        ``positions_total=0`` with no hook keys **even though the summary holds
        positions**, so ``protocols_to_prove`` is NON-empty and only the
        ``positions_total == 0`` short-circuit saves it. Without that guard every
        balance-driven teardown that closed real positions but exposes no
        ``PositionInfo`` rows flips to failure — and it reaches CI through
        ``strat test`` and the demo gates.

        Constructed deliberately with positions present: an empty-summary version
        of this test passes with the short-circuit REMOVED (no protocol group to
        prove), i.e. for the wrong reason.
        """
        mgr = _manager_with_post_report(
            monkeypatch,
            _report(_entry("lp-1", ReconciliationVerdict.UNVERIFIABLE, protocol="uniswap_v3")),
        )

        out = await mgr.verify_closure_against_chain(
            _strategy(),
            verification=_verification(positions_total=0, positions_closed=0),
            pre_execution_positions=SimpleNamespace(
                positions=[SimpleNamespace(protocol="uniswap_v3", chain=CHAIN, position_id="lp-1")]
            ),
            market=None,
        )

        # The group IS present and unproven — only the zero-positions guard saves it.
        assert out.unproven_protocols == ("uniswap_v3",)
        assert out.closure_unknown is False
        assert out.all_closed is True

    @pytest.mark.asyncio
    async def test_vib6285_early_return_on_already_failed_is_preserved(self, monkeypatch):
        """``all_closed=False`` short-circuits before any chain read — unchanged."""

        def _boom():
            raise AssertionError("must not reach the chain read on an already-failed verification")

        mgr = _manager_with_post_report(monkeypatch, _report())
        monkeypatch.setattr(mgr, "_teardown_gateway_client", _boom)
        incoming = ClosureVerification(
            all_closed=False,
            positions_total=1,
            positions_closed=0,
            has_position_breakdown=True,
            verification_status=VerificationStatus.FAILED,
        )

        out = await _verify(mgr, incoming)

        assert out is incoming
        assert out.closure_unknown is False

    @pytest.mark.asyncio
    async def test_vib6285_unknown_survives_a_confidence_downgrade(self, monkeypatch):
        """The status-lowering return path must carry ``closure_unknown`` too.

        Regression guard for the two distinct return statements at the tail of
        ``verify_closure_against_chain``: a PRE-teardown report that lowers
        CHAIN_VERIFIED → UNVERIFIED takes a different return, and dropping the
        new flag there would silently re-open the hole for every downgraded run.
        """
        mgr = _manager_with_post_report(
            monkeypatch,
            _report(_entry("gmx-perp-eth", ReconciliationVerdict.UNVERIFIABLE)),
        )
        pre = _report(_entry("gmx-perp-eth", ReconciliationVerdict.UNVERIFIABLE))

        out = await mgr.verify_closure_against_chain(
            _strategy(),
            verification=_verification(),
            pre_execution_positions=SimpleNamespace(positions=[]),
            market=None,
            pre_teardown_reconciliation=pre,
        )

        assert out.verification_status is VerificationStatus.UNVERIFIED
        assert out.closure_unknown is True


class TestVib6285PendleShapeAndKnownHole:
    @pytest.mark.asyncio
    async def test_vib6285_pendle_one_hook_proven_one_phantom_row_certifies(self, monkeypatch):
        """Pendle acceptance row — MUST still certify (unchanged behaviour).

        ``registry_enumeration._dedupe_key`` has arms only for lending and LP and
        otherwise falls through to the raw ``position_id``, so ONE physical Pendle
        PT position is enumerated twice: the strategy-authored ``pendle_pt_0`` and
        the registry's ``pt-steth-30dec2027``. Only one row can acquire evidence;
        the phantom never can. The stricter "every position measured closed" rule
        would fail this completely clean teardown — which is why the shipped rule
        is deliberately the weaker one.
        """
        mgr = _manager_with_post_report(
            monkeypatch,
            _report(
                _entry("pendle_pt_0", ReconciliationVerdict.UNVERIFIABLE, protocol="pendle"),
                _entry("pt-steth-30dec2027", ReconciliationVerdict.UNVERIFIABLE, protocol="pendle"),
            ),
        )

        out = await _verify(
            mgr,
            _verification(hook_proven=(("pendle", CHAIN, "pt-steth-30dec2027"),)),
        )

        assert out.closure_unknown is False
        assert out.all_closed is True

    @pytest.mark.asyncio
    async def test_vib6285_known_hole_is_now_scoped_to_one_protocol(self, monkeypatch):
        """KNOWN HOLE, asserted so it cannot rot silently.

        WITHIN a protocol the rule is still existential: one measured-closed
        position certifies its unmeasured siblings. Closing this needs
        per-physical-identity dedupe (workplan W2.2) — until then the duplicate-row
        problem makes "every position measured closed" unsatisfiable. If this test
        ever starts FAILING, the strict rule has landed and it should be deleted,
        not weakened.
        """
        mgr = _manager_with_post_report(
            monkeypatch,
            _report(
                _entry("lp-1", ReconciliationVerdict.DIVERGED_CLOSED, protocol="uniswap_v3"),
                _entry("lp-2", ReconciliationVerdict.UNVERIFIABLE, protocol="uniswap_v3"),
                _entry("lp-3", ReconciliationVerdict.UNVERIFIABLE, protocol="uniswap_v3"),
            ),
        )

        out = await _verify(mgr, _verification(positions_total=3, positions_closed=3))

        assert out.closure_unknown is False

    @pytest.mark.asyncio
    async def test_vib6285_multi_protocol_certifies_only_when_every_group_is_proven(self, monkeypatch):
        """ALM-3038 in its realistic form: an unrelated protocol's proof must not carry.

        A strategy holding a GMX perp AND an Aave leg gets DIVERGED_CLOSED x2 from
        Aave. Under a GLOBAL existential that evidence certifies the whole teardown
        while the perp is unmeasured. Grouped per protocol, gmx_v2 has no evidence
        of its own and the teardown is blocked.
        """
        mgr = _manager_with_post_report(
            monkeypatch,
            _report(
                _entry("aave-supply", ReconciliationVerdict.DIVERGED_CLOSED, protocol="aave_v3"),
                _entry("aave-borrow", ReconciliationVerdict.DIVERGED_CLOSED, protocol="aave_v3"),
                _entry("gmx-perp-eth", ReconciliationVerdict.UNVERIFIABLE, protocol="gmx_v2"),
            ),
        )

        out = await _verify(mgr, _verification(positions_total=3, positions_closed=3))

        assert out.closure_unknown is True
        assert out.unproven_protocols == ("gmx_v2",)
        assert out.measured_closed_protocols == ("aave_v3",)
        # Still no claim of residual risk — this is absence of proof.
        assert out.all_closed is True

    @pytest.mark.asyncio
    async def test_vib6285_multi_protocol_certifies_when_both_groups_proven(self, monkeypatch):
        """Negative control for the grouping: every group proven ⇒ certifies."""
        mgr = _manager_with_post_report(
            monkeypatch,
            _report(
                _entry("aave-supply", ReconciliationVerdict.DIVERGED_CLOSED, protocol="aave_v3"),
                _entry("gmx-perp-eth", ReconciliationVerdict.UNVERIFIABLE, protocol="gmx_v2"),
            ),
        )

        out = await _verify(
            mgr,
            _verification(positions_total=2, positions_closed=2, hook_proven=(("gmx_v2", CHAIN, "gmx-perp-eth"),)),
        )

        assert out.closure_unknown is False

    @pytest.mark.asyncio
    async def test_vib6285_blank_protocol_plan_a_entry_is_not_evidence(self, monkeypatch):
        """A blank protocol can never acquire evidence — on EITHER arm.

        ``PositionReconciliation.protocol`` is built unconditionally as
        ``str(position.protocol or "")`` and ``_reconcile_one`` computes the
        verdict without consulting it, so a blank-protocol ``DIVERGED_CLOSED``
        entry is reachable. The hook arm always filtered blanks; the Plan-A arm
        did not, so ``""`` removed itself from ``unproven_protocols`` and
        certified an unnamed position (CodeRabbit, PR #3531).

        This is worse than an instance of the known within-protocol hole: ``""``
        is a CATCH-ALL, so one measured-closed unnamed position would vouch for
        structurally unrelated unnamed siblings.
        """
        mgr = _manager_with_post_report(
            monkeypatch,
            _report(_entry("mystery-1", ReconciliationVerdict.DIVERGED_CLOSED, protocol="")),
        )

        out = await _verify(mgr, _verification(positions_total=1, positions_closed=1))

        assert out.measured_closed_protocols == ()
        assert out.unproven_protocols == ("",)
        assert out.closure_unknown is True
        # Absence of proof, never a residual-risk claim.
        assert out.all_closed is True

    @pytest.mark.asyncio
    async def test_vib6285_blank_protocol_does_not_taint_a_named_group(self, monkeypatch):
        """Negative control: dropping blanks must not cost a real protocol its proof."""
        mgr = _manager_with_post_report(
            monkeypatch,
            _report(
                _entry("lp-1", ReconciliationVerdict.DIVERGED_CLOSED, protocol="uniswap_v3"),
                _entry("mystery-1", ReconciliationVerdict.DIVERGED_CLOSED, protocol=""),
            ),
        )

        out = await _verify(mgr, _verification(positions_total=2, positions_closed=2))

        assert out.measured_closed_protocols == ("uniswap_v3",)
        # The named group keeps its proof; only the unnamed one still blocks.
        assert out.unproven_protocols == ("",)
        assert out.closure_unknown is True


# ---------------------------------------------------------------------------
# Call site 1 — CLI execute lane (TeardownManager.execute)
# ---------------------------------------------------------------------------


def _cli_strategy():
    strategy = SimpleNamespace(
        deployment_id="dep-vib6285",
        name="vib6285_test",
        chain=CHAIN,
        wallet_address="0xWALLET",
        uses_safe_wallet=False,
        get_teardown_profile=lambda: SimpleNamespace(natural_exit_assets=[], original_entry_assets=[]),
    )
    strategy.pause = AsyncMock()
    strategy.get_open_positions = _cli_positions
    intent = SimpleNamespace(
        intent_type="PERP_CLOSE",
        chain=CHAIN,
        amount=None,
        to_dict=lambda: {"intent_type": "PERP_CLOSE", "chain": CHAIN},
    )
    strategy.generate_teardown_intents = lambda mode, market=None: [intent]
    return strategy


def _cli_positions() -> TeardownPositionSummary:
    return TeardownPositionSummary(
        deployment_id="dep-vib6285",
        timestamp=datetime.now(UTC),
        positions=[
            PositionInfo(
                position_type=PositionType.PERP,
                position_id="gmx-perp-eth",
                chain=CHAIN,
                protocol="gmx_v2",
                value_usd=Decimal("18"),
            )
        ],
    )


def _exec_result(success: bool = True) -> TeardownResult:
    return TeardownResult(
        success=success,
        deployment_id="dep-vib6285",
        mode="graceful",
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        duration_seconds=1.0,
        intents_total=1,
        intents_succeeded=1 if success else 0,
        intents_failed=0 if success else 1,
        starting_value_usd=Decimal("18"),
        final_value_usd=Decimal("18"),
        total_costs_usd=Decimal("0"),
        final_balances={},
        error=None if success else "boom",
    )


class TestVib6285CliLaneCallSite:
    async def _run(self, closure_unknown: bool) -> TeardownResult:
        from almanak.framework.teardown.config import TeardownConfig

        mgr = TeardownManager(config=TeardownConfig.default())
        mgr._execute_intents = AsyncMock(return_value=_exec_result())
        mgr._verify_closure_detailed = AsyncMock(
            return_value=ClosureVerification(
                all_closed=True, positions_total=1, positions_closed=1, has_position_breakdown=True
            )
        )

        mgr.verify_closure_against_chain = AsyncMock(side_effect=_chain_verifier(closure_unknown))
        mgr.run_token_consolidation = AsyncMock(return_value=ConsolidationOutcome(planned=0, succeeded=0, failed=0))
        return await mgr.execute(_cli_strategy(), mode="graceful", is_auto_mode=True)

    @pytest.mark.asyncio
    async def test_vib6285_cli_lane_unknown_blocks_success(self):
        result = await self._run(closure_unknown=True)
        assert result.success is False

    @pytest.mark.asyncio
    async def test_vib6285_cli_lane_unknown_never_claims_positions_are_open(self):
        """VIB-6198 false-failure class: 'unknown' must never read as 'still open'."""
        result = await self._run(closure_unknown=True)
        assert result.error == CLOSURE_UNKNOWN_ERROR
        assert "still open" not in (result.error or "").lower()
        assert "not proven" in (result.error or "").lower()

    @pytest.mark.asyncio
    async def test_vib6285_cli_lane_unknown_carries_operator_guidance(self):
        """The outcome is TERMINAL, not automatically retryable: ``mark_failed`` makes the
        request terminal and the crash watchdog only re-queues rows stuck at ``executing``.
        ``recovery_options`` is operator guidance for a MANUAL re-run, not a retry contract."""
        result = await self._run(closure_unknown=True)
        assert result.recovery_options == ["Verify positions on-chain", "Re-run teardown"]

    @pytest.mark.asyncio
    async def test_vib6285_cli_lane_measured_closure_still_certifies(self):
        """Negative control: the branch must not fire when evidence exists."""
        result = await self._run(closure_unknown=False)
        assert result.success is True
        assert result.error is None


# ---------------------------------------------------------------------------
# Call site 2 — runner signal-driven lane (execute_and_verify)
# ---------------------------------------------------------------------------


def _runner_state() -> TeardownState:
    import json

    now = datetime.now(UTC)
    return TeardownState(
        teardown_id="td_vib6285",
        deployment_id="dep-vib6285",
        mode=TeardownMode.SOFT,
        status=TeardownStatus.EXECUTING,
        total_intents=1,
        completed_intents=0,
        current_intent_index=0,
        started_at=now,
        updated_at=now,
        pending_intents_json=json.dumps([{"intent_type": "PERP_CLOSE"}]),
    )


class TestVib6285RunnerLaneCallSite:
    async def _run(self, closure_unknown: bool) -> tuple[TeardownResult, TeardownState, MagicMock]:
        from almanak.framework.runner import _teardown_helpers as _h

        mgr = MagicMock(name="TeardownManager")
        mgr._execute_intents = AsyncMock(return_value=_exec_result())
        mgr._verify_closure_detailed = AsyncMock(
            return_value=ClosureVerification(
                all_closed=True, positions_total=1, positions_closed=1, has_position_breakdown=True
            )
        )

        mgr.verify_closure_against_chain = AsyncMock(side_effect=_chain_verifier(closure_unknown))
        mgr.run_token_consolidation = AsyncMock(return_value=ConsolidationOutcome(planned=0, succeeded=0, failed=0))

        state = _runner_state()
        adapter = MagicMock()
        adapter.save_teardown_state = AsyncMock()
        state_manager = MagicMock()

        result = await _h.execute_and_verify(
            MagicMock(),  # runner
            mgr,
            adapter,
            state,
            _cli_strategy(),
            [{"intent_type": "PERP_CLOSE"}],
            _cli_positions(),
            TeardownMode.SOFT,
            None,  # teardown_market
            True,  # is_auto_mode
            None,  # price_oracle
            MagicMock(),  # request
            state_manager,
        )
        return result, state, state_manager

    @pytest.mark.asyncio
    async def test_vib6285_runner_lane_unknown_blocks_success(self):
        result, _, _ = await self._run(closure_unknown=True)
        assert result.success is False

    @pytest.mark.asyncio
    async def test_vib6285_runner_lane_unknown_never_claims_positions_are_open(self):
        result, _, _ = await self._run(closure_unknown=True)
        assert result.error == CLOSURE_UNKNOWN_ERROR
        assert "still open" not in (result.error or "").lower()

    @pytest.mark.asyncio
    async def test_vib6285_runner_lane_unknown_persists_failed_status(self):
        """FAILED is the only non-success terminal member; it means 'the teardown
        operation failed', NOT 'positions are open'. Paired with the honest error
        message it is the correct persisted state — see the PR rationale."""
        _, state, _ = await self._run(closure_unknown=True)
        assert state.status is TeardownStatus.FAILED

    @pytest.mark.asyncio
    async def test_vib6285_runner_lane_unknown_skips_token_consolidation(self):
        """Consolidation must not run on an unproven closure — the residual-token
        swap must never race a possibly-unwound-possibly-not position."""
        result, _, state_manager = await self._run(closure_unknown=True)
        assert result.success is False
        state_manager.update_progress.assert_not_called()

    @pytest.mark.asyncio
    async def test_vib6285_runner_lane_measured_closure_still_certifies(self):
        """Negative control: the branch must not fire when evidence exists."""
        result, state, _ = await self._run(closure_unknown=False)
        assert result.success is True
        assert state.status is not TeardownStatus.FAILED


# ---------------------------------------------------------------------------
# The evidence set itself — a fabricated proof inverts the rule to fail-OPEN
# ---------------------------------------------------------------------------


def _lp_hook_position(position_type: str, protocol: str, position_id: str = "42") -> SimpleNamespace:
    return SimpleNamespace(
        position_type=position_type,
        protocol=protocol,
        chain=CHAIN,
        position_id=position_id,
        details={},
    )


class TestVib6285OutOfScopeHookIsNotEvidence:
    """The NFT-shaped LP hooks must not fabricate a proof for non-LP positions.

    ``protocol="uniswap_v3"`` is shared between LP NFT positions and the
    ``PositionType.TOKEN`` rows swap-only strategies surface (``uniswap_rsi``
    etc.). Both NFT hooks used to return a bare ``closed=True`` for those — zero
    chain reads, yet counted chain-verified and recorded as a hook proof. Under
    the certification rule a fabricated proof satisfies its whole protocol group,
    inverting the rule to fail-OPEN for an entire strategy class. LP / Aave /
    Pendle / GMX all pass while it does, so the four acceptance rows cannot catch
    it — hence this dedicated group.
    """

    @pytest.mark.parametrize(
        ("hook_path", "protocol"),
        [
            ("almanak.framework.teardown.post_conditions._uniswap_v3_post_condition", "uniswap_v3"),
            ("almanak.connectors.uniswap_v4.teardown_post_condition.uniswap_v4_post_condition", "uniswap_v4"),
        ],
    )
    def test_vib6285_token_position_on_lp_slug_is_not_applicable(self, hook_path, protocol):
        import importlib

        module_name, _, attr = hook_path.rpartition(".")
        hook = getattr(importlib.import_module(module_name), attr)

        result = hook(position=_lp_hook_position("TOKEN", protocol), wallet_address="0xWALLET")

        # not_applicable is the load-bearing assertion — ``closed`` stays True for
        # back-compat, so asserting only on ``closed`` would pass with the fix
        # reverted (the exact hole this closes).
        assert result.not_applicable is True
        # And NOT unmeasured: out-of-scope contributes no doubt either, or every
        # swap-strategy teardown would be blocked for the wrong reason.
        assert result.unmeasured is False

    @pytest.mark.parametrize(
        ("hook_path", "protocol"),
        [
            ("almanak.framework.teardown.post_conditions._uniswap_v3_post_condition", "uniswap_v3"),
            ("almanak.connectors.uniswap_v4.teardown_post_condition.uniswap_v4_post_condition", "uniswap_v4"),
        ],
    )
    def test_vib6285_lp_position_is_still_in_scope(self, hook_path, protocol):
        """Negative control: the gate must only fire on non-LP positions."""
        import importlib

        module_name, _, attr = hook_path.rpartition(".")
        hook = getattr(importlib.import_module(module_name), attr)

        result = hook(position=_lp_hook_position("LP", protocol), wallet_address="0xWALLET")

        assert result.not_applicable is False

    @pytest.mark.asyncio
    async def test_vib6285_out_of_scope_skip_contributes_no_hook_proof(self, monkeypatch):
        """End-to-end through the verifier: the skip must not enter the evidence set."""
        from almanak.connectors._strategy_base.teardown_post_condition import ClosureCheckResult

        mgr = TeardownManager()
        monkeypatch.setattr(mgr, "_teardown_gateway_client", lambda: MagicMock())
        monkeypatch.setattr(mgr, "_teardown_rpc_url", lambda: None)
        monkeypatch.setattr(TeardownManager, "_teardown_wallet_address", staticmethod(lambda _s: "0xWALLET"))
        monkeypatch.setattr(
            "almanak.framework.teardown.post_conditions.get_teardown_post_condition",
            lambda _protocol: (
                lambda **_kw: ClosureCheckResult(closed=True, not_applicable=True, protocol="uniswap_v3")
            ),
        )

        out = await mgr._verify_closure_detailed(
            SimpleNamespace(deployment_id="d", get_open_positions=lambda: SimpleNamespace(positions=[])),
            pre_execution_positions=SimpleNamespace(positions=[_lp_hook_position("TOKEN", "uniswap_v3", "tok-0")]),
        )

        assert out.hook_proven_position_keys == ()
        # Not counted chain-verified either — an out-of-scope skip proves nothing.
        assert out.verification_status is VerificationStatus.UNVERIFIED

    @pytest.mark.asyncio
    async def test_vib6285_measured_closure_still_records_a_hook_proof(self, monkeypatch):
        """Negative control: a REAL measured closure must still enter the evidence set."""
        from almanak.connectors._strategy_base.teardown_post_condition import ClosureCheckResult

        mgr = TeardownManager()
        monkeypatch.setattr(mgr, "_teardown_gateway_client", lambda: MagicMock())
        monkeypatch.setattr(mgr, "_teardown_rpc_url", lambda: None)
        monkeypatch.setattr(TeardownManager, "_teardown_wallet_address", staticmethod(lambda _s: "0xWALLET"))
        monkeypatch.setattr(
            "almanak.framework.teardown.post_conditions.get_teardown_post_condition",
            lambda _protocol: (lambda **_kw: ClosureCheckResult(closed=True, protocol="uniswap_v3")),
        )

        out = await mgr._verify_closure_detailed(
            SimpleNamespace(deployment_id="d", get_open_positions=lambda: SimpleNamespace(positions=[])),
            pre_execution_positions=SimpleNamespace(positions=[_lp_hook_position("LP", "uniswap_v3", "42")]),
        )

        assert out.hook_proven_position_keys == (("uniswap_v3", CHAIN, "42"),)
        assert out.verification_status is VerificationStatus.CHAIN_VERIFIED

    @pytest.mark.asyncio
    async def test_vib6285_measured_closure_with_empty_position_id_is_a_false_unknown(self, monkeypatch):
        """Deliberately conservative: no id ⇒ no key ⇒ no evidence ⇒ blocked.

        A hook that MEASURED closure on a position carrying an EMPTY
        ``position_id`` records no key (the ``if proven_id:`` guard). If that were
        a teardown's only evidence the result is a FALSE UNKNOWN — blocked rather
        than certified. That fails in the SAFE direction and must stay: appending
        unconditionally would put an unidentified entry into the evidence set,
        which can only ever be a free proof — the fail-OPEN this guard closes.
        False-unknown is acceptable; false-proven is not.
        """
        from almanak.connectors._strategy_base.teardown_post_condition import ClosureCheckResult

        mgr = TeardownManager()
        monkeypatch.setattr(mgr, "_teardown_gateway_client", lambda: MagicMock())
        monkeypatch.setattr(mgr, "_teardown_rpc_url", lambda: None)
        monkeypatch.setattr(TeardownManager, "_teardown_wallet_address", staticmethod(lambda _s: "0xWALLET"))
        monkeypatch.setattr(
            "almanak.framework.teardown.post_conditions.get_teardown_post_condition",
            lambda _protocol: (lambda **_kw: ClosureCheckResult(closed=True, protocol="uniswap_v3")),
        )
        positions = SimpleNamespace(positions=[_lp_hook_position("LP", "uniswap_v3", "")])

        verification = await mgr._verify_closure_detailed(
            SimpleNamespace(deployment_id="d", get_open_positions=lambda: SimpleNamespace(positions=[])),
            pre_execution_positions=positions,
        )
        assert verification.hook_proven_position_keys == ()

        monkeypatch.setattr(mgr, "_fresh_post_execution_market", lambda strategy, market: MagicMock())

        async def _fake_reconcile(**_kwargs):
            return _report(_entry("", ReconciliationVerdict.UNVERIFIABLE, protocol="uniswap_v3"))

        monkeypatch.setattr(
            "almanak.framework.teardown.teardown_manager.reconcile_known_positions_against_chain",
            _fake_reconcile,
        )

        out = await mgr.verify_closure_against_chain(
            _strategy(),
            verification=verification,
            pre_execution_positions=positions,
            market=None,
        )

        assert out.closure_unknown is True
        # Fails SAFE: never a claim that positions are open.
        assert out.all_closed is True


class TestVib6285TokenClosureAuthority:
    """The TOKEN primitive's framework-default on-chain closure authority.

    Third instance of the pattern set by the ERC-4626 vault default (VIB-5573)
    and the fungible-ERC-20-LP default (VIB-5795): give a whole primitive a real
    gateway chain read, degrading to ``unmeasured`` rather than fabricating.

    Deliberately NOT a strategy self-report: a strategy's own
    ``get_open_positions()`` is the evidence class the ratchet exists to reject
    (the GMX canary certified two positions off rows the strategy wrote itself).
    """

    @staticmethod
    def _token_position(details: dict, position_id: str = "uniswap_v3_weth_position") -> SimpleNamespace:
        return SimpleNamespace(
            position_type=SimpleNamespace(value="token"),
            protocol="uniswap_v3",
            chain=CHAIN,
            position_id=position_id,
            details=details,
        )

    @staticmethod
    def _gateway(balance):
        return SimpleNamespace(query_erc20_balance=lambda **_kw: balance)

    def _hook(self, position):
        from almanak.framework.teardown.post_conditions import position_type_post_condition

        hook = position_type_post_condition(position)
        assert hook is not None, "TOKEN must resolve a framework-default closure authority"
        return hook

    def test_vib6285_zero_balance_is_a_measured_closure(self):
        """The real strategy shape: ``details['asset']`` is a SYMBOL (uniswap_rsi, lido_staker)."""
        position = self._token_position({"asset": "WETH", "balance": "0"})
        result = self._hook(position)(
            position=position, wallet_address="0x" + "11" * 20, gateway_client=self._gateway(0)
        )

        assert result.closed is True
        assert result.unmeasured is False
        assert result.not_applicable is False

    def test_vib6285_nonzero_balance_is_a_measured_residual_not_unknown(self):
        """POSITIVE-DIRECTION CONTROL.

        A balance read that can only ever say "closed" is the fabrication we just
        removed wearing a different hat. A measured non-zero balance must produce
        a measured RESIDUAL — ``closed=False`` with the balance in ``residual`` —
        which drives ``all_closed=False``, not ``closure_unknown``.
        """
        position = self._token_position({"asset": "WETH"})
        result = self._hook(position)(
            position=position, wallet_address="0x" + "11" * 20, gateway_client=self._gateway(5 * 10**18)
        )

        assert result.closed is False
        assert result.unmeasured is False
        assert result.residual["balance"] == str(5 * 10**18)

    def test_vib6285_unresolvable_token_address_is_unmeasured(self):
        """Empty ≠ Zero: no resolvable address ⇒ unmeasured, never a fabricated closure."""
        position = self._token_position({}, position_id="phantom")
        result = self._hook(position)(
            position=position, wallet_address="0x" + "11" * 20, gateway_client=self._gateway(0)
        )

        assert result.unmeasured is True
        assert result.closed is False
        assert "none resolvable" in (result.error or "")

    def test_vib6285_read_fault_is_unmeasured_never_a_residual(self):
        """A None/non-numeric read after retry is a read fault, not a residual."""
        position = self._token_position({"asset": "WETH"})
        result = self._hook(position)(
            position=position, wallet_address="0x" + "11" * 20, gateway_client=self._gateway(None)
        )

        assert result.unmeasured is True
        assert result.residual == {}

    def test_vib6285_missing_gateway_client_is_unmeasured(self):
        """Gateway boundary: no client ⇒ no read ⇒ unmeasured (never a self-report fallback)."""
        position = self._token_position({"asset": "WETH"})
        result = self._hook(position)(position=position, wallet_address="0x" + "11" * 20, gateway_client=None)

        assert result.unmeasured is True

    @pytest.mark.parametrize("bad_wallet", ["", None, "not-an-address", "0xdeadbeef"])
    def test_vib6285_unusable_wallet_is_unmeasured_never_a_closure(self, bad_wallet):
        """The WALLET is an input to the measurement, so Empty ≠ Zero applies to it too.

        ``_teardown_wallet_address`` returns ``""`` for a strategy exposing no
        wallet, so this is reachable. Without the guard the gateway may coerce the
        value (e.g. to the zero address), return 0, and the caller would record a
        hook proof and CERTIFY the protocol group off a read that measured nothing
        about the real wallet (CodeRabbit, PR #3531).

        The gateway here returns 0 — the fabrication-friendly answer. A guard that
        only worked when the read happened to fail would prove nothing.
        """
        position = self._token_position({"asset": "WETH"})
        result = self._hook(position)(position=position, wallet_address=bad_wallet, gateway_client=self._gateway(0))

        assert result.unmeasured is True
        assert result.closed is False
        assert "wallet_address" in (result.error or "")

    def test_vib6285_token_detail_key_resolves_as_a_symbol(self):
        """``details={"token": "WETH"}`` must measure, not silently go unmeasured.

        ``token`` was an ADDRESS-only key, so this very common shape failed the
        address pass, was never tried as a symbol, and resolved to "" (Codex P2).
        12+ TOKEN-typed positions write it, including the accounting reference
        fixture ``strategies/accounting/ta/strategy.py``. Harmless before VIB-6285;
        under the per-protocol rule an unmeasured TOKEN row REFUSES TO CERTIFY a
        successful unwind, so the gap became a block.
        """
        position = self._token_position({"token": "WETH"})
        result = self._hook(position)(
            position=position, wallet_address="0x" + "11" * 20, gateway_client=self._gateway(0)
        )

        assert result.unmeasured is False, "the 'token' detail key must resolve as a symbol"
        assert result.closed is True

    def test_vib6285_token_detail_key_still_resolves_as_an_address(self):
        """Negative control: adding ``token`` to the SYMBOL keys must not break the ADDRESS path."""
        position = self._token_position({"token": "0x" + "ab" * 20})
        result = self._hook(position)(
            position=position, wallet_address="0x" + "11" * 20, gateway_client=self._gateway(0)
        )

        assert result.unmeasured is False
        assert result.closed is True

    @pytest.mark.asyncio
    async def test_vib6285_hook_reads_the_positions_own_chain_wallet(self, monkeypatch):
        """The hook must read the PER-CHAIN wallet, not the hoisted primary.

        Multi-chain teardown is supported and per-chain wallets can legitimately
        differ. Every other wallet consumer in ``teardown_manager`` already resolves
        per chain; this TD-14 loop was the last one that did not (Codex P1 +
        claude-pr-auditor, PR #3531).

        VIB-6285 is what makes it a SAFETY bug: a wrong-wallet ``balanceOf`` returns
        0, which used to be a harmless "no residual" and is now affirmative evidence
        certifying the whole protocol group. The double therefore returns a RESIDUAL
        for the correct wallet and ZERO for the primary — so reading the wrong one
        certifies and the assertion below fails.
        """
        primary = "0x" + "11" * 20
        secondary = "0x" + "22" * 20
        seen: list[str] = []

        def _balance(**kw):
            seen.append(str(kw.get("wallet_address") or ""))
            return 0 if str(kw.get("wallet_address") or "").lower() == primary.lower() else 7 * 10**18

        mgr = TeardownManager()
        monkeypatch.setattr(mgr, "_teardown_gateway_client", lambda: SimpleNamespace(query_erc20_balance=_balance))
        monkeypatch.setattr(mgr, "_teardown_rpc_url", lambda: None)
        monkeypatch.setattr(TeardownManager, "_teardown_wallet_address", staticmethod(lambda _s: primary))
        position = self._token_position({"asset": "WETH"})
        strategy = SimpleNamespace(
            deployment_id="d",
            get_open_positions=lambda: SimpleNamespace(positions=[]),
            wallet_address=primary,
            get_wallet_for_chain=lambda _chain: secondary,
        )

        out = await mgr._verify_closure_detailed(
            strategy, pre_execution_positions=SimpleNamespace(positions=[position])
        )

        assert seen == [secondary], f"hook read the wrong wallet: {seen}"
        # The secondary wallet holds a residual, so this must NOT certify.
        assert out.hook_proven_position_keys == ()
        assert out.all_closed is False

    @pytest.mark.parametrize("junk", [object(), 12345, ["0x" + "33" * 20]])
    def test_vib6285_non_string_chain_wallet_falls_back_to_the_primary(self, junk):
        """A per-chain wallet that is not a string is not a wallet.

        ``get_wallet_for_chain`` is contracted as ``str | None``, but the resolver
        used to accept ANY truthy object and ``str()`` it — so a registry (or a
        ``MagicMock`` double) returning a non-string produced a plausible-looking
        but meaningless address that reached calldata and made every read on that
        leg fail as "unmeasured". Falling back to the primary wallet is the honest
        answer (PR #3531).

        Caught by the existing lending suite when the per-chain resolution landed:
        ``str(<MagicMock>)`` silently became the wallet.
        """
        from almanak.framework.teardown.teardown_manager import _teardown_wallet_for_chain

        primary = "0x" + "11" * 20
        strategy = SimpleNamespace(wallet_address=primary, get_wallet_for_chain=lambda _c: junk)

        assert _teardown_wallet_for_chain(strategy, CHAIN) == primary

    def test_vib6285_real_string_chain_wallet_still_wins(self):
        """Negative control: the type guard must not disable per-chain resolution."""
        from almanak.framework.teardown.teardown_manager import _teardown_wallet_for_chain

        secondary = "0x" + "22" * 20
        strategy = SimpleNamespace(wallet_address="0x" + "11" * 20, get_wallet_for_chain=lambda _c: f"  {secondary} ")

        assert _teardown_wallet_for_chain(strategy, CHAIN) == secondary

    @pytest.mark.asyncio
    async def test_vib6285_token_hook_supplies_the_missing_closure_evidence(self, monkeypatch):
        """NEGATIVE CONTROL for the whole TOKEN authority.

        End-to-end: a TOKEN row on an LP slug. The protocol hook is out of scope
        (``not_applicable``), so the position-type default takes over and its
        measured zero becomes the hook proof that lets the teardown certify.
        Revert the TOKEN authority and this position yields no evidence at all —
        the swap-only class blocks again.
        """
        mgr = TeardownManager()
        monkeypatch.setattr(mgr, "_teardown_gateway_client", lambda: self._gateway(0))
        monkeypatch.setattr(mgr, "_teardown_rpc_url", lambda: None)
        monkeypatch.setattr(TeardownManager, "_teardown_wallet_address", staticmethod(lambda _s: "0x" + "11" * 20))
        position = self._token_position({"asset": "WETH"})

        out = await mgr._verify_closure_detailed(
            SimpleNamespace(deployment_id="d", get_open_positions=lambda: SimpleNamespace(positions=[])),
            pre_execution_positions=SimpleNamespace(positions=[position]),
        )

        assert out.hook_proven_position_keys == (("uniswap_v3", CHAIN, "uniswap_v3_weth_position"),)
        assert out.verification_status is VerificationStatus.CHAIN_VERIFIED

    @pytest.mark.asyncio
    async def test_vib6285_token_residual_fails_closed_end_to_end(self, monkeypatch):
        """The other polarity end-to-end: a measured TOKEN residual FAILS the teardown."""
        mgr = TeardownManager()
        monkeypatch.setattr(mgr, "_teardown_gateway_client", lambda: self._gateway(5 * 10**18))
        monkeypatch.setattr(mgr, "_teardown_rpc_url", lambda: None)
        monkeypatch.setattr(TeardownManager, "_teardown_wallet_address", staticmethod(lambda _s: "0x" + "11" * 20))
        position = self._token_position({"asset": "WETH"})

        out = await mgr._verify_closure_detailed(
            SimpleNamespace(deployment_id="d", get_open_positions=lambda: SimpleNamespace(positions=[])),
            pre_execution_positions=SimpleNamespace(positions=[position]),
        )

        assert out.all_closed is False
        assert out.verification_status is VerificationStatus.FAILED
        assert out.closure_unknown is False


class TestVib6285StaleRowCannotCertifyAnotherProtocol:
    """W4: a phantom / never-existed row must not certify an unmeasured protocol.

    The concern: a stale row reads POST ``DIVERGED_CLOSED`` — which PRE-teardown
    is TD-15 AC-(b)'s "distrust this enumeration" signal — and W0.1 promotes that
    same verdict to certifying evidence. The named scenario was a phantom
    already-closed row certifying alongside an unmeasurable GMX perp.

    Unreachable by construction, for two INDEPENDENT reasons, both verified at
    source and both pinned below so neither can regress silently:

    1. Per-protocol grouping — a phantom row certifies only its OWN protocol
       group, so it can never reach ``gmx_v2``.
    2. ``DIVERGED_CLOSED`` has exactly two producers, ``_reconcile_lp`` and
       ``_reconcile_lending``. PERP falls through to ``UNVERIFIABLE`` ("no
       per-position Plan-A chain read"), so no ``gmx_v2`` row can produce that
       verdict in the first place.
    """

    @pytest.mark.asyncio
    async def test_vib6285_phantom_diverged_row_does_not_certify_another_protocol(self, monkeypatch):
        """Reason 1: grouping confines the phantom's evidence to its own protocol."""
        mgr = _manager_with_post_report(
            monkeypatch,
            _report(
                # The phantom: a never-existed LP row the chain reports closed.
                _entry("phantom-lp", ReconciliationVerdict.DIVERGED_CLOSED, protocol="uniswap_v3"),
                # The real risk: an unmeasurable perp.
                _entry("gmx-perp-eth", ReconciliationVerdict.UNVERIFIABLE, protocol="gmx_v2"),
            ),
        )

        out = await _verify(mgr, _verification(positions_total=2, positions_closed=2))

        assert out.closure_unknown is True
        assert out.unproven_protocols == ("gmx_v2",)
        assert out.measured_closed_protocols == ("uniswap_v3",)

    def test_vib6285_every_open_state_declarer_refuses_to_fabricate_closure(self):
        """Reason 2, re-derived: a PERP read may certify ONLY if it measured.

        This replaces an earlier ``len(producers) == 2`` source grep over
        ``plan_a_reconciliation``. That form was a tripwire on the assumption
        that PERP had no Plan-A read at all, and VIB-6254 (#3511) legitimately
        retired the assumption by giving PERP a real gateway-backed read gated on
        an opt-in ``supports_open_state_reconciliation`` capability. A count can
        only be silenced or incremented, and incrementing it would have asserted
        nothing whatsoever about the new producer — so it is deleted, not raised.

        The invariant that actually protects the ratchet is narrower and does not
        expire: a declarer earns the right to certify by *measuring*, so when the
        read cannot be performed it must report ``unmeasured`` (or raise, which
        Plan-A's own guard lowers to UNVERIFIABLE) and must never return a bare
        ``closed=True``. Asserted as a census over the live registry rather than
        a list, so a connector that opts in tomorrow is covered on the day it
        registers — the omission this suite exists to make impossible.
        """
        # Import for the side effect: this is what hydrates connector-published
        # hooks into the registry, so an empty census would mean a broken import,
        # not an absence of declarers.
        from almanak.connectors._strategy_base import teardown_post_condition as tpc
        from almanak.framework.teardown import post_conditions as _hydrate  # noqa: F401

        declarers = {
            protocol: hook
            for protocol, hook in tpc._REGISTRY.items()
            if getattr(hook, "supports_open_state_reconciliation", False) is True
        }
        # gmx_v2 is the capability's only declarer today. The assertion is that
        # the census FOUND something — an empty dict would pass every loop below
        # vacuously, which is the canary-passing-for-the-wrong-reason case.
        assert declarers, "no supports_open_state_reconciliation declarers found — registry not hydrated?"

        class _ExplodingGateway:
            def __getattr__(self, name):
                raise RuntimeError(f"gateway unreachable ({name})")

        position = SimpleNamespace(
            position_type=PositionType.PERP,
            position_id="0x" + "cd" * 32,
            chain="arbitrum",
            protocol="gmx_v2",
            details={},
            market="ETH/USD",
        )

        # The three ways a read can fail to happen: no client, no wallet, and a
        # client that faults. None may yield a measured closure.
        unreadable_cases = (
            ("no gateway client", {"wallet_address": "0xwallet", "gateway_client": None}),
            ("blank wallet address", {"wallet_address": "   ", "gateway_client": object()}),
            ("gateway faults", {"wallet_address": "0xwallet", "gateway_client": _ExplodingGateway()}),
        )

        for protocol, hook in declarers.items():
            for label, kwargs in unreadable_cases:
                try:
                    result = hook(position=position, rpc_url=None, block=None, **kwargs)
                except Exception:  # noqa: BLE001 — raising IS fail-closed; Plan-A lowers it to UNVERIFIABLE
                    continue
                assert result.closed is not True, (
                    f"{protocol} fabricated closed=True with {label} — an unmeasured read "
                    f"would certify a teardown that read nothing on-chain (VIB-6285)"
                )
                assert result.unmeasured is True, (
                    f"{protocol} returned neither unmeasured nor a raise with {label}; "
                    f"got {result!r}. A declarer must be honest about a read it could not perform."
                )

    @pytest.mark.asyncio
    async def test_vib6285_stale_pre_teardown_report_still_lowers_confidence(self, monkeypatch):
        """AC-(b) is untouched: a not-clean PRE report still lowers CHAIN_VERIFIED."""
        mgr = _manager_with_post_report(
            monkeypatch,
            _report(_entry("phantom-lp", ReconciliationVerdict.DIVERGED_CLOSED, protocol="uniswap_v3")),
        )
        pre = _report(_entry("phantom-lp", ReconciliationVerdict.DIVERGED_CLOSED, protocol="uniswap_v3"))

        out = await mgr.verify_closure_against_chain(
            _strategy(),
            verification=_verification(positions_total=1, positions_closed=1),
            pre_execution_positions=SimpleNamespace(positions=[]),
            market=None,
            pre_teardown_reconciliation=pre,
        )

        assert out.verification_status is VerificationStatus.UNVERIFIED


class TestVib6285LendingHealthEmptyIsNotZero:
    """W2: a missing health value must not become a fabricated measured zero.

    ``_derive_live_lending_position`` guarded ``health is None`` and named the
    hazard exactly — "otherwise the all-zero getattr defaults below would silently
    report 'closed' and the caller would strand a live position" — but stopped one
    level short: a health object that EXISTS with a missing/``None`` value field
    coerced to ``Decimal("0")``, which ``_reconcile_lending`` reads as at/under
    dust ⇒ ``DIVERGED_CLOSED``. Since W0.1 that verdict CERTIFIES the protocol
    group, so a fabricated zero would certify a live lending position closed.
    """

    @staticmethod
    def _derive(health):
        from almanak.framework.teardown.live_position_reads import redrive_lending_position

        market = SimpleNamespace(position_health=lambda *a, **k: health, price=lambda *a, **k: None)
        return redrive_lending_position(
            market=market,
            protocol="aave_v3",
            market_id="m-1",
            collateral_token="WETH",
            borrow_token="USDC",
        )

    def test_vib6285_missing_collateral_value_is_unavailable(self):
        health = SimpleNamespace(debt_value_usd="0", health_factor=None)  # no collateral_value_usd
        assert self._derive(health) is None

    def test_vib6285_none_collateral_value_is_unavailable(self):
        health = SimpleNamespace(collateral_value_usd=None, debt_value_usd="0", health_factor=None)
        assert self._derive(health) is None

    def test_vib6285_none_debt_value_is_unavailable(self):
        health = SimpleNamespace(collateral_value_usd="0", debt_value_usd=None, health_factor=None)
        assert self._derive(health) is None

    def test_vib6285_present_numeric_zero_is_a_real_measured_zero(self):
        """NEGATIVE CONTROL: the guard must not swallow a genuine measured zero.

        A cleanly measured all-zero position IS a closed market and must still
        derive — otherwise the fix would turn every real lending closure into
        'unmeasured' and block it, the opposite failure.
        """
        health = SimpleNamespace(collateral_value_usd="0", debt_value_usd="0", health_factor=None)
        derived = self._derive(health)

        assert derived is not None
        assert derived.collateral_value_usd == Decimal("0")
        assert derived.debt_value_usd == Decimal("0")

    def test_vib6285_present_nonzero_value_still_derives(self):
        health = SimpleNamespace(collateral_value_usd="1500.25", debt_value_usd="700", health_factor="1.8")
        derived = self._derive(health)

        assert derived is not None
        assert derived.collateral_value_usd == Decimal("1500.25")


class TestVib6285ReverseMutationControls:
    """Controls that catch a canary passing for the WRONG reason."""

    @pytest.mark.asyncio
    async def test_vib6285_unknown_does_not_become_failed_verification_status(self, monkeypatch):
        """A mutation mapping unknown → FAILED must be CAUGHT here.

        ``closure_unknown`` blocks certification without asserting residual risk.
        If a future change routes it through the FAILED machinery instead, the
        record would claim a measured residual that was never measured — the
        VIB-6198 false-failure class. Pin the three observable consequences.
        """
        mgr = _manager_with_post_report(
            monkeypatch,
            _report(_entry("gmx-perp-eth", ReconciliationVerdict.UNVERIFIABLE)),
        )

        out = await _verify(mgr, _verification(status=VerificationStatus.UNVERIFIED))

        assert out.closure_unknown is True
        assert out.verification_status is VerificationStatus.UNVERIFIED
        assert out.verification_status is not VerificationStatus.FAILED
        # positions_failed is derived as total - closed by the persist path; an
        # unknown closure measured NO failures, so the two must stay equal.
        assert out.positions_total - out.positions_closed == 0

    @pytest.mark.asyncio
    async def test_vib6285_unknown_error_message_names_absence_not_residual(self):
        """The message must never contain a residual-risk claim."""
        lowered = CLOSURE_UNKNOWN_ERROR.lower()
        assert "still open" not in lowered
        assert "residual" not in lowered
        assert "not proven" in lowered

    @pytest.mark.asyncio
    async def test_vib6285_raising_hook_still_reports_all_closed(self, monkeypatch):
        """VIB-5573 regression pin: a RAISING hook is unmeasured, never a residual.

        Guarded here as well as in ``test_verify_closure_post_conditions`` so the
        fabricated-FAILED regression cannot return under cover of this change.
        """

        def _boom(**_kw):
            raise RuntimeError("gateway blip")

        mgr = TeardownManager()
        monkeypatch.setattr(mgr, "_teardown_gateway_client", lambda: MagicMock())
        monkeypatch.setattr(mgr, "_teardown_rpc_url", lambda: None)
        monkeypatch.setattr(TeardownManager, "_teardown_wallet_address", staticmethod(lambda _s: "0xWALLET"))
        monkeypatch.setattr(
            "almanak.framework.teardown.post_conditions.get_teardown_post_condition",
            lambda _protocol: _boom,
        )

        out = await mgr._verify_closure_detailed(
            SimpleNamespace(deployment_id="d", get_open_positions=lambda: SimpleNamespace(positions=[])),
            pre_execution_positions=SimpleNamespace(positions=[_lp_hook_position("LP", "uniswap_v3", "42")]),
        )

        assert out.all_closed is True
        assert out.verification_status is VerificationStatus.UNVERIFIED
        assert out.hook_proven_position_keys == ()


# ---------------------------------------------------------------------------
# Call site 3 — `strat test` ladder pass criterion (_teardown_step_ok)
# ---------------------------------------------------------------------------


class TestVib6285TeardownStepOk:
    def _step(self, **extra) -> dict:
        from almanak.framework.runner import IterationStatus

        return {"status": IterationStatus.TEARDOWN.value, **extra}

    def test_vib6285_measured_clean_passes(self):
        from almanak.framework.cli._run_modes import _teardown_step_ok

        assert _teardown_step_ok(self._step()) is True

    def test_vib6285_measured_residual_fails(self):
        from almanak.framework.cli._run_modes import _teardown_step_ok

        assert _teardown_step_ok(self._step(open_positions_after_teardown=[{"position_id": "x"}])) is False

    def test_vib6285_unmeasured_check_does_not_pass(self):
        """THE THIRD INSTANCE of the defect: an unmeasured post-teardown read
        yielded ``teardown_passed = True``."""
        from almanak.framework.cli._run_modes import _teardown_step_ok

        assert _teardown_step_ok(self._step(open_positions_check="unmeasured: RuntimeError('gone')")) is False

    def test_vib6285_bad_status_fails(self):
        from almanak.framework.cli._run_modes import _teardown_step_ok
        from almanak.framework.runner import IterationStatus

        assert _teardown_step_ok({"status": IterationStatus.STRATEGY_ERROR.value}) is False

    def test_vib6285_measured_and_unmeasured_states_stay_distinct(self):
        """A consumer must still be able to tell 'residual found' from 'could not
        measure' — the two keys are never collapsed into one."""
        from almanak.framework.cli._run_modes import _measure_open_positions_after_teardown

        broken = MagicMock()
        broken.get_open_positions = MagicMock(side_effect=RuntimeError("gateway gone"))
        residuals, error = _measure_open_positions_after_teardown(broken)

        # Empty ≠ Zero: the failed read reports NO residuals and a reason, so the
        # caller writes ``open_positions_check`` and never fabricates a residual.
        assert residuals == []
        assert error is not None and "gateway gone" in error
