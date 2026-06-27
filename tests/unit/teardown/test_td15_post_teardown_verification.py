"""TD-15 / VIB-5473 — fail-closed on-chain POST-teardown verification.

Pins :meth:`TeardownManager.verify_closure_against_chain`, the seam that
composes the TD-14 post-condition verdict with a FRESH POST-teardown Plan-A
reconciliation (TD-08) and the PRE-teardown reconciliation report. The contract:

* AC-(a) — a KNOWN position the chain STILL reports OPEN after every closing
  intent fired flips the teardown to ``all_closed=False`` + ``FAILED``. This
  covers the hook-less lending strand the post-condition path counts
  closed-by-execution (UNVERIFIED), which is the false-success class TD-14 alone
  could not see.
* AC-(b) — a position the PRE-teardown ledger believed open but the chain
  reported closed/unconfirmable (never-existed / stale enumeration) is never
  certified CHAIN_VERIFIED — it is lowered to UNVERIFIED.
* Inverted semantics — the check runs AFTER closure and NEVER raises: a
  reconciliation fault degrades to the incoming verification.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from almanak.framework.teardown import live_position_reads
from almanak.framework.teardown.models import (
    ClosureVerification,
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
    VerificationStatus,
)
from almanak.framework.teardown.plan_a_reconciliation import (
    PositionReconciliation,
    ReconciliationReport,
    ReconciliationVerdict,
)
from almanak.framework.teardown.teardown_manager import TeardownManager


def _mgr() -> TeardownManager:
    mgr = TeardownManager()
    # _teardown_gateway_client probes compiler/orchestrator for a client; give it
    # a non-None one so the LP read path is reached (the chain read itself is
    # monkeypatched per-test).
    mgr.compiler = SimpleNamespace(_gateway_client=object(), is_connected=True)
    return mgr


class _Strategy:
    deployment_id = "deployment:td15"
    _gateway_network = "arbitrum"


class _Health:
    def __init__(self, collateral_value_usd, debt_value_usd):
        self.collateral_value_usd = collateral_value_usd
        self.debt_value_usd = debt_value_usd
        self.health_factor = None


class _Market:
    def __init__(self, health):
        self._health = health

    def position_health(self, protocol, market_id, *, collateral_price_usd=None, debt_price_usd=None):
        return self._health

    def price(self, token):  # pragma: no cover - amounts unused by the CHECK
        raise KeyError(token)


def _lp_position(position_id: str = "999") -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=position_id,
        chain="arbitrum",
        protocol="uniswap_v3",
        value_usd=Decimal("0"),
        details={"source": "position_registry"},
    )


def _lending_position(leg: PositionType = PositionType.BORROW) -> PositionInfo:
    return PositionInfo(
        position_type=leg,
        position_id="0xmkt",
        chain="ethereum",
        protocol="aave_v3",
        value_usd=Decimal("0"),
        details={"source": "position_registry", "market_id": "0xmkt", "asset_symbol": "USDC"},
    )


def _summary(*positions: PositionInfo) -> TeardownPositionSummary:
    return TeardownPositionSummary(
        deployment_id="deployment:td15", timestamp=datetime.now(UTC), positions=list(positions)
    )


def _verified(status: VerificationStatus = VerificationStatus.CHAIN_VERIFIED, total: int = 1) -> ClosureVerification:
    return ClosureVerification(
        all_closed=True,
        positions_total=total,
        positions_closed=total,
        has_position_breakdown=True,
        verification_status=status,
    )


# ---------------------------------------------------------------------------
# AC-(a): residual OPEN after teardown → FAILED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_residual_open_lp_flips_to_failed(monkeypatch, caplog):
    async def _still_open(*, gateway_client, position, network=""):
        return True  # chain says LP liquidity > 0 — STILL OPEN

    monkeypatch.setattr(live_position_reads, "chain_verify_lp_open", _still_open)
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),
        verification=_verified(VerificationStatus.CHAIN_VERIFIED),
        pre_execution_positions=_summary(_lp_position()),
        market=None,
    )
    assert out.all_closed is False
    assert out.verification_status is VerificationStatus.FAILED
    assert out.positions_closed == 0
    assert any("STILL OPEN on-chain" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_residual_open_lending_hookless_flips_to_failed():
    """The new-value case: lending has no post-condition hook, so TD-14 reports
    UNVERIFIED (closed-by-execution). TD-15's lending chain read catches the
    still-open debt leg and fails closed."""
    market = _Market(_Health(Decimal("0"), Decimal("500")))  # debt still owed
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),
        # TD-14 would have produced UNVERIFIED here (no hook), all_closed=True.
        verification=_verified(VerificationStatus.UNVERIFIED),
        pre_execution_positions=_summary(_lending_position(PositionType.BORROW)),
        market=market,
    )
    assert out.all_closed is False
    assert out.verification_status is VerificationStatus.FAILED


@pytest.mark.asyncio
async def test_residual_open_dominates_partial_clean(monkeypatch):
    """One residual-open position fails the teardown even if another closed."""
    market = _Market(_Health(Decimal("0"), Decimal("500")))  # lending still open

    async def _lp_closed(*, gateway_client, position, network=""):
        return False  # LP closed

    monkeypatch.setattr(live_position_reads, "chain_verify_lp_open", _lp_closed)
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),
        verification=_verified(VerificationStatus.UNVERIFIED, total=2),
        pre_execution_positions=_summary(_lp_position(), _lending_position(PositionType.BORROW)),
        market=market,
    )
    assert out.all_closed is False
    assert out.verification_status is VerificationStatus.FAILED
    assert out.positions_closed == 1  # the LP closed; the lending leg is residual


# ---------------------------------------------------------------------------
# Clean close + confidence composition
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clean_close_stays_chain_verified(monkeypatch):
    async def _closed(*, gateway_client, position, network=""):
        return False  # chain confirms LP closed — the GOOD outcome

    monkeypatch.setattr(live_position_reads, "chain_verify_lp_open", _closed)
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),
        verification=_verified(VerificationStatus.CHAIN_VERIFIED),
        pre_execution_positions=_summary(_lp_position()),
        market=None,
    )
    assert out.all_closed is True
    assert out.verification_status is VerificationStatus.CHAIN_VERIFIED


@pytest.mark.asyncio
async def test_unverifiable_post_does_not_lower_chain_verified(monkeypatch):
    """A burned LP NFT reads back as UNVERIFIABLE ('not found') post-close — that
    is the success signal, NOT a doubt. TD-14 already proved closure
    (CHAIN_VERIFIED); TD-15's coarser re-read must not drag it to UNVERIFIED."""

    async def _unknown(*, gateway_client, position, network=""):
        return None  # NFT not found on NPM — the burned-NFT case

    monkeypatch.setattr(live_position_reads, "chain_verify_lp_open", _unknown)
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),
        verification=_verified(VerificationStatus.CHAIN_VERIFIED),
        pre_execution_positions=_summary(_lp_position()),
        market=None,
    )
    assert out.all_closed is True  # unknown ≠ open — does not fail
    assert out.verification_status is VerificationStatus.CHAIN_VERIFIED  # TD-14 proof preserved


# ---------------------------------------------------------------------------
# AC-(b): never-existed / stale enumeration is not certified CHAIN_VERIFIED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pre_teardown_divergence_blocks_false_chain_verified(monkeypatch):
    """A never-existed position: PRE-teardown the chain said CLOSED. POST-teardown
    it is (still) closed, so there is no residual — but the closure must NOT be
    certified CHAIN_VERIFIED off a stale enumeration."""

    async def _closed(*, gateway_client, position, network=""):
        return False

    monkeypatch.setattr(live_position_reads, "chain_verify_lp_open", _closed)
    pre = ReconciliationReport(
        deployment_id="deployment:td15",
        entries=(
            PositionReconciliation(
                "PositionType.LP", "999", "arbitrum", "uniswap_v3", ReconciliationVerdict.DIVERGED_CLOSED
            ),
        ),
    )
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),
        verification=_verified(VerificationStatus.CHAIN_VERIFIED),
        pre_execution_positions=_summary(_lp_position()),
        market=None,
        pre_teardown_reconciliation=pre,
    )
    assert out.all_closed is True  # no residual risk — a never-existed position posed none
    assert out.verification_status is VerificationStatus.UNVERIFIED  # but never certified


# ---------------------------------------------------------------------------
# Inverted semantics: short-circuit + never-raise
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_already_failed_short_circuits_without_chain_read(monkeypatch):
    called = False

    async def _spy(*, gateway_client, position, network=""):
        nonlocal called
        called = True
        return True

    monkeypatch.setattr(live_position_reads, "chain_verify_lp_open", _spy)
    failing = ClosureVerification(
        all_closed=False,
        positions_total=1,
        positions_closed=0,
        has_position_breakdown=True,
        verification_status=VerificationStatus.FAILED,
    )
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),
        verification=failing,
        pre_execution_positions=_summary(_lp_position()),
        market=None,
    )
    assert out is failing  # unchanged — original residual error is the actionable one
    assert called is False  # no redundant chain read on the already-failed path


@pytest.mark.asyncio
async def test_reconciliation_fault_degrades_to_incoming(monkeypatch):
    from almanak.framework.teardown import teardown_manager as tm

    async def _boom(**_kwargs):
        raise RuntimeError("gateway exploded")

    monkeypatch.setattr(tm, "reconcile_known_positions_against_chain", _boom)
    incoming = _verified(VerificationStatus.CHAIN_VERIFIED)
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),
        verification=incoming,
        pre_execution_positions=_summary(_lp_position()),
        market=None,
    )
    # Never raises; degrades to the TD-14 verdict (the CHECK must not fault the lane).
    assert out is incoming


@pytest.mark.asyncio
async def test_empty_position_set_passes_through(monkeypatch):
    """No KNOWN positions ⇒ nothing to re-read ⇒ verification untouched."""
    out = await _mgr().verify_closure_against_chain(
        _Strategy(),
        verification=_verified(VerificationStatus.CHAIN_VERIFIED, total=0),
        pre_execution_positions=_summary(),
        market=None,
    )
    assert out.all_closed is True
    assert out.verification_status is VerificationStatus.CHAIN_VERIFIED


# ---------------------------------------------------------------------------
# AC-(b) CLI lane: execute() computes a PRE-teardown reconciliation inline.
# The runner lane gets it from runner._teardown_reconciliation (TD-08); the CLI
# lane has no runner, so _pre_teardown_reconciliation reads chain BEFORE the
# closing intents fire. These cover the helper that closes the CLI-lane gap.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pre_teardown_reconciliation_reads_chain(monkeypatch):
    """The CLI-lane helper returns the PRE-teardown report from the chain read."""
    from almanak.framework.teardown import teardown_manager as tm

    sentinel = object()
    captured = {}

    async def _fake_reconcile(**kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(tm, "reconcile_known_positions_against_chain", _fake_reconcile)
    summary = _summary(_lp_position())
    out = await _mgr()._pre_teardown_reconciliation(_Strategy(), summary, market=None)
    # The helper threads the KNOWN set + the strategy's gateway network into the CHECK.
    assert out is sentinel
    assert captured["summary"] is summary
    assert captured["network"] == "arbitrum"


@pytest.mark.asyncio
async def test_pre_teardown_reconciliation_fault_returns_none(monkeypatch):
    """A chain-read fault must NOT fault the teardown lane — returns None (no AC-(b) downgrade)."""
    from almanak.framework.teardown import teardown_manager as tm

    async def _boom(**_kwargs):
        raise RuntimeError("gateway exploded")

    monkeypatch.setattr(tm, "reconcile_known_positions_against_chain", _boom)
    out = await _mgr()._pre_teardown_reconciliation(_Strategy(), _summary(_lp_position()), market=None)
    assert out is None
