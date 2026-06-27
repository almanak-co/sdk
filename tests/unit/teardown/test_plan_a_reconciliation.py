"""VIB-5466 / TD-08 — Plan-A on-chain reconciliation as a CHECK (not an action).

Pins the structured-divergence signal the TD-15 fail-closed verification consumes:

* ``reconcile_known_positions_against_chain`` does a protocol-scoped chain read
  per KNOWN position and classifies it CONFIRMED_OPEN / DIVERGED_CLOSED /
  UNVERIFIABLE, emitting a LOUD log on divergence.
* The CHECK never closes/sweeps and is strictly position-scoped (it iterates only
  the enumerated set; it never reaches for a wallet scan).
* ``ReconciliationReport.apply_to_verification_status`` composes with the TD-14
  ``VerificationStatus`` (only ever lowers confidence, never raises it).
"""

from __future__ import annotations

import logging
from decimal import Decimal

import pytest

from almanak.framework.teardown import live_position_reads
from almanak.framework.teardown.models import (
    PositionInfo,
    PositionType,
    TeardownPositionSummary,
    VerificationStatus,
)
from almanak.framework.teardown.plan_a_reconciliation import (
    PositionReconciliation,
    ReconciliationReport,
    ReconciliationVerdict,
    reconcile_known_positions_against_chain,
)


def _lp(position_id: str = "12345", chain: str = "arbitrum") -> PositionInfo:
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=position_id,
        chain=chain,
        protocol="lp",
        value_usd=Decimal("0"),
        details={"source": "position_registry"},
    )


def _lending(leg: PositionType, *, market_id: str = "0xmkt", symbol: str = "USDC") -> PositionInfo:
    return PositionInfo(
        position_type=leg,
        position_id=market_id,
        chain="ethereum",
        protocol="aave_v3",
        value_usd=Decimal("0"),
        details={"source": "position_registry", "market_id": market_id, "asset_symbol": symbol},
    )


def _summary(*positions: PositionInfo) -> TeardownPositionSummary:
    from datetime import UTC, datetime

    return TeardownPositionSummary(
        deployment_id="deployment:abc", timestamp=datetime.now(UTC), positions=list(positions)
    )


class _Health:
    def __init__(self, collateral_value_usd, debt_value_usd, health_factor=None):
        self.collateral_value_usd = collateral_value_usd
        self.debt_value_usd = debt_value_usd
        self.health_factor = health_factor


class _FakeMarket:
    def __init__(self, *, health=None, raise_health=False):
        self._health = health
        self._raise_health = raise_health

    def position_health(self, protocol, market_id, *, collateral_price_usd=None, debt_price_usd=None):
        if self._raise_health:
            raise RuntimeError("gateway down")
        return self._health

    def price(self, token):  # pragma: no cover - amounts unused by the CHECK
        raise KeyError(token)


# ---------------------------------------------------------------------------
# LP reconciliation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lp_confirmed_open(monkeypatch):
    async def _verify(*, gateway_client, position, network=""):
        return True

    monkeypatch.setattr(live_position_reads, "chain_verify_lp_open", _verify)
    report = await reconcile_known_positions_against_chain(
        summary=_summary(_lp()), gateway_client=object(), market=None
    )
    assert report.checked_count == 1
    assert not report.has_divergence
    assert report.is_clean
    assert report.entries[0].verdict is ReconciliationVerdict.CONFIRMED_OPEN


@pytest.mark.asyncio
async def test_lp_divergence_closed_is_loud(monkeypatch, caplog):
    async def _verify(*, gateway_client, position, network=""):
        return False  # chain says closed, ledger believes open

    monkeypatch.setattr(live_position_reads, "chain_verify_lp_open", _verify)
    with caplog.at_level(logging.ERROR):
        report = await reconcile_known_positions_against_chain(
            summary=_summary(_lp()), gateway_client=object(), market=None
        )
    assert report.has_divergence
    assert len(report.diverged) == 1
    assert report.diverged[0].verdict is ReconciliationVerdict.DIVERGED_CLOSED
    # LOUD: a structured ERROR naming the divergence was emitted.
    assert any("DIVERGENCE" in rec.message and rec.levelno == logging.ERROR for rec in caplog.records)
    # The structured signal carries the per-position entry for TD-15.
    assert report.to_dict()["diverged"] == 1


@pytest.mark.asyncio
async def test_lp_unverifiable_without_gateway():
    report = await reconcile_known_positions_against_chain(summary=_summary(_lp()), gateway_client=None, market=None)
    assert report.has_unverifiable
    assert report.entries[0].verdict is ReconciliationVerdict.UNVERIFIABLE


@pytest.mark.asyncio
async def test_lp_read_raise_degrades_to_unverifiable(monkeypatch):
    async def _verify(*, gateway_client, position, network=""):
        raise RuntimeError("decode fault")

    monkeypatch.setattr(live_position_reads, "chain_verify_lp_open", _verify)
    report = await reconcile_known_positions_against_chain(
        summary=_summary(_lp()), gateway_client=object(), market=None
    )
    # Empty != Zero: a raised read is unknown, never treated as closed.
    assert report.entries[0].verdict is ReconciliationVerdict.UNVERIFIABLE
    assert not report.has_divergence


# ---------------------------------------------------------------------------
# Lending reconciliation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lending_collateral_confirmed_open():
    market = _FakeMarket(health=_Health(Decimal("1000"), Decimal("0")))
    report = await reconcile_known_positions_against_chain(
        summary=_summary(_lending(PositionType.SUPPLY)), gateway_client=None, market=market
    )
    assert report.entries[0].verdict is ReconciliationVerdict.CONFIRMED_OPEN


@pytest.mark.asyncio
async def test_lending_debt_diverged_when_zero(caplog):
    market = _FakeMarket(health=_Health(Decimal("0"), Decimal("0")))
    with caplog.at_level(logging.ERROR):
        report = await reconcile_known_positions_against_chain(
            summary=_summary(_lending(PositionType.BORROW)), gateway_client=None, market=market
        )
    assert report.has_divergence
    assert report.entries[0].verdict is ReconciliationVerdict.DIVERGED_CLOSED


@pytest.mark.asyncio
async def test_lending_unverifiable_when_health_unavailable():
    market = _FakeMarket(raise_health=True)
    report = await reconcile_known_positions_against_chain(
        summary=_summary(_lending(PositionType.SUPPLY)), gateway_client=None, market=market
    )
    assert report.entries[0].verdict is ReconciliationVerdict.UNVERIFIABLE


@pytest.mark.asyncio
async def test_lending_unverifiable_without_market():
    report = await reconcile_known_positions_against_chain(
        summary=_summary(_lending(PositionType.SUPPLY)), gateway_client=None, market=None
    )
    assert report.entries[0].verdict is ReconciliationVerdict.UNVERIFIABLE


# ---------------------------------------------------------------------------
# Unsupported primitives + empty
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_perp_is_unverifiable_not_fabricated_confirmed():
    perp = PositionInfo(
        position_type=PositionType.PERP,
        position_id="0xkey",
        chain="arbitrum",
        protocol="gmx_v2",
        value_usd=Decimal("0"),
    )
    report = await reconcile_known_positions_against_chain(summary=_summary(perp), gateway_client=object(), market=None)
    assert report.entries[0].verdict is ReconciliationVerdict.UNVERIFIABLE


@pytest.mark.asyncio
async def test_empty_summary_is_empty_report():
    report = await reconcile_known_positions_against_chain(summary=_summary(), gateway_client=object(), market=None)
    assert report.checked_count == 0
    assert not report.has_divergence
    assert not report.is_clean  # nothing checked -> not "clean confirmed"


# ---------------------------------------------------------------------------
# Verification-status composition (compose with TD-14, never fight it)
# ---------------------------------------------------------------------------


def _report(verdict: ReconciliationVerdict) -> ReconciliationReport:
    return ReconciliationReport(
        deployment_id="d",
        entries=(PositionReconciliation("PositionType.LP", "1", "arbitrum", "lp", verdict),),
    )


@pytest.mark.parametrize(
    "verdict,proposed,expected",
    [
        # Divergence downgrades a chain-verified claim to unverified.
        (ReconciliationVerdict.DIVERGED_CLOSED, VerificationStatus.CHAIN_VERIFIED, VerificationStatus.UNVERIFIED),
        # Unverifiable also downgrades chain-verified.
        (ReconciliationVerdict.UNVERIFIABLE, VerificationStatus.CHAIN_VERIFIED, VerificationStatus.UNVERIFIED),
        # A clean confirmation leaves chain-verified untouched.
        (ReconciliationVerdict.CONFIRMED_OPEN, VerificationStatus.CHAIN_VERIFIED, VerificationStatus.CHAIN_VERIFIED),
        # Never upgrades: FAILED stays FAILED even on a clean report.
        (ReconciliationVerdict.CONFIRMED_OPEN, VerificationStatus.FAILED, VerificationStatus.FAILED),
        # Divergence does not promote a non-chain-verified status.
        (ReconciliationVerdict.DIVERGED_CLOSED, VerificationStatus.UNVERIFIED, VerificationStatus.UNVERIFIED),
        (ReconciliationVerdict.DIVERGED_CLOSED, VerificationStatus.NOT_RUN, VerificationStatus.NOT_RUN),
    ],
)
def test_apply_to_verification_status(verdict, proposed, expected):
    assert _report(verdict).apply_to_verification_status(proposed) is expected


# ---------------------------------------------------------------------------
# Runner wiring: reconcile_known_positions stashes the structured report
# ---------------------------------------------------------------------------


class _FakeStrategy:
    def __init__(self):
        self.deployment_id = "deployment:abc"
        self._gateway_client = None
        self._gateway_network = ""


class _FakeRunner:
    pass


@pytest.mark.asyncio
async def test_runner_helper_returns_report(monkeypatch):
    from almanak.framework.runner import runner_teardown
    from almanak.framework.teardown import registry_enumeration

    async def _enum(_strategy):
        return _summary(_lending(PositionType.SUPPLY))

    # Lending leg with measured-zero collateral -> a divergence the CHECK flags.
    monkeypatch.setattr(registry_enumeration, "resolve_open_positions_with_registry", _enum)
    market = _FakeMarket(health=_Health(Decimal("0"), Decimal("0")))
    report = await runner_teardown.reconcile_known_positions(_FakeRunner(), _FakeStrategy(), market)
    assert report is not None
    assert report.has_divergence


@pytest.mark.asyncio
async def test_runner_helper_none_when_enumeration_fails(monkeypatch):
    from almanak.framework.runner import runner_teardown
    from almanak.framework.teardown import registry_enumeration

    async def _boom(_strategy):
        raise RuntimeError("registry unavailable")

    monkeypatch.setattr(registry_enumeration, "resolve_open_positions_with_registry", _boom)
    report = await runner_teardown.reconcile_known_positions(_FakeRunner(), _FakeStrategy(), None)
    assert report is None
