"""TD-15 whole-account attribution (VIB-5936).

A POST-teardown ``CONFIRMED_OPEN`` whose evidence is a WHOLE-account aggregate
read (BENQI's summed qiToken markets; the Aave family's ``getUserAccountData``)
cannot be attributed to a specific tracked position — the residual may belong
entirely to OTHER markets the wallet holds (pre-existing history unrelated to
the strategy; the benqi dogfooding-wallet case that flipped a hook-proven-closed
teardown to FAILED). When the position's own TD-14 post-condition hook MEASURED
it closed, the finer-grained proof is final: the aggregate entry downgrades to
``NOT_APPLICABLE`` (the designed no-op) with the residual kept visible in the
detail. Hook-less positions and market-scoped reads keep the conservative
fail-closed path untouched.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry
from almanak.framework.teardown.models import ClosureVerification, VerificationStatus
from almanak.framework.teardown.plan_a_reconciliation import (
    PositionReconciliation,
    ReconciliationReport,
    ReconciliationVerdict,
)
from almanak.framework.teardown.teardown_manager import TeardownManager


# ---------------------------------------------------------------------------
# Registry predicate: whole_account_read is connector-declared, never name-based
# ---------------------------------------------------------------------------


class TestVib5936WholeAccountReadPredicate:
    def test_vib5936_benqi_is_whole_account(self):
        # Publishes a market table, but with ONE fixed synthetic id covering the
        # whole account — the spec declares whole_account=True.
        assert LendingReadRegistry.whole_account_read("benqi") is True

    def test_vib5936_aave_family_is_whole_account_by_construction(self):
        # No market table at all → whole-account by construction.
        assert LendingReadRegistry.whole_account_read("aave_v3") is True

    @pytest.mark.parametrize("protocol", ["morpho_blue", "silo_v2", "euler_v2", "compound_v3"])
    def test_vib5936_market_scoped_protocols_are_not(self, protocol):
        assert LendingReadRegistry.whole_account_read(protocol) is False

    def test_vib5936_unknown_protocol_is_not(self):
        assert LendingReadRegistry.whole_account_read("no_such_protocol") is False


# ---------------------------------------------------------------------------
# Report transform: downgrade_unattributable_confirmed_open
# ---------------------------------------------------------------------------


def _key(entry: PositionReconciliation) -> tuple[str, str, str]:
    return (entry.protocol.strip().lower(), entry.chain.strip().lower(), entry.position_id.strip())


def _entry(position_id: str, verdict: ReconciliationVerdict, protocol: str = "benqi") -> PositionReconciliation:
    return PositionReconciliation(
        position_type="PositionType.SUPPLY",
        position_id=position_id,
        chain="avalanche",
        protocol=protocol,
        verdict=verdict,
        detail="collateral value $7.597 on-chain",
    )


class TestVib5936ReportDowngrade:
    def test_vib5936_named_confirmed_open_downgrades_to_not_applicable(self):
        report = ReconciliationReport(
            deployment_id="d",
            entries=(_entry("pos-1", ReconciliationVerdict.CONFIRMED_OPEN),),
        )
        out = report.downgrade_unattributable_confirmed_open(
            frozenset({("benqi", "avalanche", "pos-1")}), "unattributable (VIB-5936)"
        )
        assert out.entries[0].verdict is ReconciliationVerdict.NOT_APPLICABLE
        # The residual stays visible — appended, never erased.
        assert "collateral value $7.597 on-chain" in out.entries[0].detail
        assert "unattributable (VIB-5936)" in out.entries[0].detail
        assert not out.has_confirmed_open
        # NOT_APPLICABLE is excluded from the confidence-lowering trigger (VIB-5522).
        assert not out.has_unverifiable
        assert (
            out.apply_post_teardown_to_verification_status(VerificationStatus.CHAIN_VERIFIED)
            is VerificationStatus.CHAIN_VERIFIED
        )

    def test_vib5936_unnamed_and_other_verdicts_pass_through(self):
        confirmed_other = _entry("pos-2", ReconciliationVerdict.CONFIRMED_OPEN)
        diverged = _entry("pos-3", ReconciliationVerdict.DIVERGED_CLOSED)
        report = ReconciliationReport(deployment_id="d", entries=(confirmed_other, diverged))
        out = report.downgrade_unattributable_confirmed_open(frozenset({("benqi", "avalanche", "pos-1")}), "reason")
        assert out.entries == (confirmed_other, diverged)
        assert out.has_confirmed_open  # pos-2 untouched — still the fail trigger

    def test_vib5936_empty_ids_is_identity(self):
        report = ReconciliationReport(
            deployment_id="d",
            entries=(_entry("pos-1", ReconciliationVerdict.CONFIRMED_OPEN),),
        )
        assert report.downgrade_unattributable_confirmed_open(frozenset(), "reason") is report

    def test_vib5936_never_touches_diverged_even_when_named(self):
        # Only CONFIRMED_OPEN attribution is in scope — a named DIVERGED_CLOSED
        # (the GOOD post-teardown outcome) must pass through byte-identical.
        diverged = _entry("pos-1", ReconciliationVerdict.DIVERGED_CLOSED)
        report = ReconciliationReport(deployment_id="d", entries=(diverged,))
        out = report.downgrade_unattributable_confirmed_open(frozenset({_key(diverged)}), "reason")
        assert out.entries == (diverged,)

    def test_vib5936_never_touches_unverifiable_even_when_named(self):
        """Spec-critique hardening (UAT card D5.6): suppression is restricted to a
        MEASURED, VISIBLE residual (CONFIRMED_OPEN). A named UNVERIFIABLE — the
        post-read faulted / could not measure — must pass through byte-identical:
        broadening the transform to swallow measurement failures would silently
        convert a genuine read fault into certainty. (Whether UNVERIFIABLE lowers
        the verdict is the fold's pre-existing contract, owned elsewhere; this
        transform must simply never touch it.)"""
        unverifiable = _entry("pos-1", ReconciliationVerdict.UNVERIFIABLE)
        report = ReconciliationReport(deployment_id="d", entries=(unverifiable,))
        out = report.downgrade_unattributable_confirmed_open(frozenset({_key(unverifiable)}), "reason")
        assert out.entries == (unverifiable,)
        # And the report-level signal is unchanged: still an unverifiable entry,
        # never rebadged NOT_APPLICABLE (which is excluded from confidence checks).
        assert out.has_unverifiable


# ---------------------------------------------------------------------------
# TD-15 fold: hook-proven + whole-account suppresses; everything else fails
# ---------------------------------------------------------------------------


def _chain_verified_verification(*hook_proven: tuple[str, str, str]) -> ClosureVerification:
    # Normalize exactly as the real hook loop stores proofs (lowercased triple),
    # so tests can pass human-readable mixed-case ids and still exercise the match.
    normalized = tuple((p.strip().lower(), c.strip().lower(), pid.strip().lower()) for p, c, pid in hook_proven)
    return ClosureVerification(
        all_closed=True,
        positions_total=len(hook_proven) or 2,
        positions_closed=len(hook_proven) or 2,
        has_position_breakdown=True,
        verification_status=VerificationStatus.CHAIN_VERIFIED,
        hook_proven_position_keys=normalized,
    )


def _manager_with_post_report(monkeypatch, report: ReconciliationReport) -> TeardownManager:
    mgr = TeardownManager()
    monkeypatch.setattr(mgr, "_teardown_gateway_client", lambda: MagicMock())
    monkeypatch.setattr(mgr, "_fresh_post_execution_market", lambda strategy, market: MagicMock())

    async def _fake_reconcile(**_kwargs):
        return report

    monkeypatch.setattr(
        "almanak.framework.teardown.teardown_manager.reconcile_known_positions_against_chain",
        _fake_reconcile,
    )
    return mgr


def _strategy() -> SimpleNamespace:
    return SimpleNamespace(deployment_id="deployment:vib5936", _gateway_network="")


@pytest.mark.asyncio
async def test_vib5936_hook_proven_benqi_whole_account_residual_does_not_fail(monkeypatch):
    """The proof-run scenario: both benqi legs hook-proven closed; the whole-account
    re-read reports the wallet's PRE-EXISTING unrelated exposure. The teardown must
    stay CHAIN_VERIFIED — the residual is not attributable to these positions."""
    report = ReconciliationReport(
        deployment_id="deployment:vib5936",
        entries=(
            _entry("benqi-collateral-AVAX-avalanche", ReconciliationVerdict.CONFIRMED_OPEN),
            _entry("benqi-borrow-USDC-avalanche", ReconciliationVerdict.CONFIRMED_OPEN),
        ),
    )
    mgr = _manager_with_post_report(monkeypatch, report)
    verification = _chain_verified_verification(
        ("benqi", "avalanche", "benqi-collateral-AVAX-avalanche"),
        ("benqi", "avalanche", "benqi-borrow-USDC-avalanche"),
    )

    out = await mgr.verify_closure_against_chain(
        _strategy(),
        verification=verification,
        pre_execution_positions=SimpleNamespace(positions=[]),
        market=None,
    )

    assert out.all_closed is True
    assert out.verification_status is VerificationStatus.CHAIN_VERIFIED
    assert out.positions_closed == 2


@pytest.mark.asyncio
async def test_vib5936_not_hook_proven_whole_account_residual_still_fails(monkeypatch):
    """Hook-less whole-account positions (aave family until VIB-5910) keep the
    conservative fail-closed: without a finer-grained proof, aggregate residual
    IS treated as the position still open."""
    report = ReconciliationReport(
        deployment_id="deployment:vib5936",
        entries=(_entry("aave-supply-USDC-ethereum", ReconciliationVerdict.CONFIRMED_OPEN, protocol="aave_v3"),),
    )
    mgr = _manager_with_post_report(monkeypatch, report)
    verification = _chain_verified_verification()  # hook_proven empty

    out = await mgr.verify_closure_against_chain(
        _strategy(),
        verification=verification,
        pre_execution_positions=SimpleNamespace(positions=[]),
        market=None,
    )

    assert out.all_closed is False
    assert out.verification_status is VerificationStatus.FAILED


@pytest.mark.asyncio
async def test_vib5936_market_scoped_residual_fails_even_when_hook_proven(monkeypatch):
    """A market-scoped read (silo_v2) IS position-relevant evidence: a residual it
    reports must fail the teardown even if a hook claimed the position closed —
    two position-scoped reads disagreeing is exactly what fail-closed is for."""
    report = ReconciliationReport(
        deployment_id="deployment:vib5936",
        entries=(_entry("silo-supply-USDC-avalanche", ReconciliationVerdict.CONFIRMED_OPEN, protocol="silo_v2"),),
    )
    mgr = _manager_with_post_report(monkeypatch, report)
    verification = _chain_verified_verification(("silo_v2", "avalanche", "silo-supply-USDC-avalanche"))

    out = await mgr.verify_closure_against_chain(
        _strategy(),
        verification=verification,
        pre_execution_positions=SimpleNamespace(positions=[]),
        market=None,
    )

    assert out.all_closed is False
    assert out.verification_status is VerificationStatus.FAILED


@pytest.mark.asyncio
async def test_vib5936_same_id_different_protocol_is_not_shared_proof(monkeypatch):
    """Codex P1 regression: a benqi hook proof for position_id X must not suppress a
    whole-account CONFIRMED_OPEN carrying the SAME bare id under a different
    protocol/chain — proofs match on the FULL (protocol, chain, position_id)."""
    report = ReconciliationReport(
        deployment_id="deployment:vib5936",
        entries=(_entry("pos-x", ReconciliationVerdict.CONFIRMED_OPEN, protocol="aave_v3"),),
    )
    mgr = _manager_with_post_report(monkeypatch, report)
    # Hook proof exists for the SAME bare id, but under benqi — must not transfer.
    verification = _chain_verified_verification(("benqi", "avalanche", "pos-x"))

    out = await mgr.verify_closure_against_chain(
        _strategy(),
        verification=verification,
        pre_execution_positions=SimpleNamespace(positions=[]),
        market=None,
    )

    assert out.all_closed is False
    assert out.verification_status is VerificationStatus.FAILED


@pytest.mark.asyncio
async def test_vib5936_position_id_case_insensitive_match(monkeypatch):
    """CodeRabbit: synthetic lending position_ids embed EVM addresses whose case is a
    checksum, not identity. A hook proof recorded with a checksummed id must still
    suppress a reconciliation entry carrying the same id lowercased (and vice-versa) —
    the SAME position, so the residual is correctly attributed away."""
    report = ReconciliationReport(
        deployment_id="deployment:vib5936",
        entries=(
            _entry("benqi-collateral-0xAbC-avalanche", ReconciliationVerdict.CONFIRMED_OPEN),
        ),
    )
    mgr = _manager_with_post_report(monkeypatch, report)
    # Proof recorded with a DIFFERENT case for the same id.
    verification = _chain_verified_verification(("benqi", "avalanche", "benqi-collateral-0xabc-avalanche"))

    out = await mgr.verify_closure_against_chain(
        _strategy(),
        verification=verification,
        pre_execution_positions=SimpleNamespace(positions=[]),
        market=None,
    )

    assert out.all_closed is True
    assert out.verification_status is VerificationStatus.CHAIN_VERIFIED


@pytest.mark.asyncio
async def test_vib5936_empty_position_id_never_matches_a_proof(monkeypatch):
    """CodeRabbit major regression: an empty position_id must never be treated as
    hook-proven — "" matching "" would fail-open an unidentified residual."""
    report = ReconciliationReport(
        deployment_id="deployment:vib5936",
        entries=(_entry("", ReconciliationVerdict.CONFIRMED_OPEN),),
    )
    mgr = _manager_with_post_report(monkeypatch, report)
    verification = _chain_verified_verification(("benqi", "avalanche", ""))

    out = await mgr.verify_closure_against_chain(
        _strategy(),
        verification=verification,
        pre_execution_positions=SimpleNamespace(positions=[]),
        market=None,
    )

    assert out.all_closed is False
    assert out.verification_status is VerificationStatus.FAILED
