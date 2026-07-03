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


def _lp(position_id: str = "12345", chain: str = "arbitrum", protocol: str = "uniswap_v3") -> PositionInfo:
    # ``protocol="uniswap_v3"`` (an NFT-based, V3_NPM-family protocol) by
    # default so these fixtures actually reach the NFT-scoped
    # ``chain_verify_lp_open`` delegate the LP-reconciliation tests below
    # exercise (VIB-5522 scopes that delegate to NFT-family protocols only).
    return PositionInfo(
        position_type=PositionType.LP,
        position_id=position_id,
        chain=chain,
        protocol=protocol,
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
# VIB-5522 — non-NFT (ERC-1155 / LB) LP positions are NOT_APPLICABLE to the
# NFT-only Plan-A LP read, never folded into UNVERIFIABLE.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_lp_non_nft_protocol_is_not_applicable(monkeypatch):
    """A TraderJoe V2 (Liquidity Book, ERC-1155) LP position can never be

    confirmed OR denied by the NFT-only ``chain_verify_lp_open`` read, so it
    must reconcile as NOT_APPLICABLE, and the doomed-to-fail NFT read must
    never even be attempted (ALM-2807 repro root cause).
    """

    async def _boom(*, gateway_client, position, network=""):  # pragma: no cover - must not be called
        raise AssertionError("chain_verify_lp_open must not be attempted for a non-NFT LP protocol")

    monkeypatch.setattr(live_position_reads, "chain_verify_lp_open", _boom)
    report = await reconcile_known_positions_against_chain(
        summary=_summary(_lp(position_id="traderjoe_crisis_lp_0", protocol="traderjoe_v2")),
        gateway_client=object(),
        market=None,
    )
    assert report.entries[0].verdict is ReconciliationVerdict.NOT_APPLICABLE
    assert report.entries[0].not_applicable
    assert not report.entries[0].unverifiable
    # The critical distinction: NOT_APPLICABLE must NOT count toward the
    # fail-closed ``has_unverifiable`` trigger TD-15 reads.
    assert not report.has_unverifiable
    assert not report.has_divergence
    assert len(report.not_applicable) == 1


@pytest.mark.asyncio
async def test_lp_non_nft_protocol_not_applicable_even_without_gateway():
    """NOT_APPLICABLE is decided by protocol membership alone — it must win

    over (and short-circuit before) the "no gateway client" UNVERIFIABLE
    branch, since the read would be structurally inapplicable regardless.
    """
    report = await reconcile_known_positions_against_chain(
        summary=_summary(_lp(position_id="traderjoe_crisis_lp_0", protocol="traderjoe_v2")),
        gateway_client=None,
        market=None,
    )
    assert report.entries[0].verdict is ReconciliationVerdict.NOT_APPLICABLE


@pytest.mark.asyncio
async def test_lp_missing_protocol_is_unverifiable_not_not_applicable(monkeypatch):
    """An LP position with no resolvable protocol is fail-closed UNVERIFIABLE,

    NOT NOT_APPLICABLE (Gemini HIGH, PR #3178). NOT_APPLICABLE asserts "a known
    non-NFT LP, deferred to its own post-condition"; an unknown protocol may have
    no post-condition either, so classifying it NOT_APPLICABLE would leave it
    neither verified nor confidence-lowered (fail-open). It must lower confidence
    via ``has_unverifiable``.
    """

    async def _boom(*, gateway_client, position, network=""):  # pragma: no cover - must not be called
        raise AssertionError("chain_verify_lp_open must not be attempted for a protocol-less LP")

    monkeypatch.setattr(live_position_reads, "chain_verify_lp_open", _boom)
    report = await reconcile_known_positions_against_chain(
        summary=_summary(_lp(position_id="orphan_lp_0", protocol="")),
        gateway_client=object(),
        market=None,
    )
    assert report.entries[0].verdict is ReconciliationVerdict.UNVERIFIABLE
    assert report.entries[0].unverifiable
    assert not report.entries[0].not_applicable
    # The fail-closed contract: an unidentifiable LP MUST trigger TD-15's downgrade.
    assert report.has_unverifiable


@pytest.mark.asyncio
async def test_lp_mixed_case_nft_protocol_is_not_not_applicable(monkeypatch):
    """A mixed-case NFT LP protocol slug (custom strategy / non-canonical entry)

    must still be recognised as an NFT LP and go through the real read — not be
    mis-classified NOT_APPLICABLE, which would skip its residual check (Gemini
    MEDIUM, PR #3178). Membership is lower-cased before the registry test.
    """
    called = {"n": 0}

    async def _verify(*, gateway_client, position, network=""):
        called["n"] += 1
        return False  # NPM reports liquidity == 0 → DIVERGED_CLOSED (real read ran)

    monkeypatch.setattr(live_position_reads, "chain_verify_lp_open", _verify)
    report = await reconcile_known_positions_against_chain(
        summary=_summary(_lp(position_id="99", protocol="Uniswap_V3")),
        gateway_client=object(),
        market=None,
    )
    assert called["n"] == 1  # the real NFT read WAS attempted (not short-circuited)
    assert report.entries[0].verdict is ReconciliationVerdict.DIVERGED_CLOSED
    assert report.entries[0].verdict is not ReconciliationVerdict.NOT_APPLICABLE


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
        # VIB-5522: NOT_APPLICABLE (a structurally-inapplicable Plan-A read,
        # e.g. the NFT-only LP check against a non-NFT LP position) must NEVER
        # downgrade a proposed CHAIN_VERIFIED — this is the ALM-2807 fix: a
        # PASSED protocol post-condition is authoritative and an inapplicable
        # reconciliation path is a no-op, not a downgrade.
        (ReconciliationVerdict.NOT_APPLICABLE, VerificationStatus.CHAIN_VERIFIED, VerificationStatus.CHAIN_VERIFIED),
        (ReconciliationVerdict.NOT_APPLICABLE, VerificationStatus.UNVERIFIED, VerificationStatus.UNVERIFIED),
        (ReconciliationVerdict.NOT_APPLICABLE, VerificationStatus.NOT_RUN, VerificationStatus.NOT_RUN),
    ],
)
def test_apply_to_verification_status(verdict, proposed, expected):
    assert _report(verdict).apply_to_verification_status(proposed) is expected


# ---------------------------------------------------------------------------
# POST-teardown composition (TD-15 / VIB-5473) — inverted directions
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "verdict,proposed,expected",
    [
        # CONFIRMED_OPEN POST-teardown = residual on-chain risk → FAILED, always.
        (ReconciliationVerdict.CONFIRMED_OPEN, VerificationStatus.CHAIN_VERIFIED, VerificationStatus.FAILED),
        (ReconciliationVerdict.CONFIRMED_OPEN, VerificationStatus.UNVERIFIED, VerificationStatus.FAILED),
        (ReconciliationVerdict.CONFIRMED_OPEN, VerificationStatus.NOT_RUN, VerificationStatus.FAILED),
        # DIVERGED_CLOSED POST-teardown = the GOOD outcome (closing intent worked) → untouched.
        (ReconciliationVerdict.DIVERGED_CLOSED, VerificationStatus.CHAIN_VERIFIED, VerificationStatus.CHAIN_VERIFIED),
        (ReconciliationVerdict.DIVERGED_CLOSED, VerificationStatus.UNVERIFIED, VerificationStatus.UNVERIFIED),
        # UNVERIFIABLE POST-teardown is a NO-OP: a KNOWN position Plan-A cannot
        # re-read after closure (e.g. a burned LP NFT = "not found") is the success
        # signal, not a doubt — TD-14's post-condition owns that closure proof, so
        # this must NOT drag a chain-verified close down to unverified.
        (ReconciliationVerdict.UNVERIFIABLE, VerificationStatus.CHAIN_VERIFIED, VerificationStatus.CHAIN_VERIFIED),
        (ReconciliationVerdict.UNVERIFIABLE, VerificationStatus.UNVERIFIED, VerificationStatus.UNVERIFIED),
        # VIB-5522: NOT_APPLICABLE is likewise a no-op POST-teardown — it never
        # carried information about openness either way, so it must not
        # participate in the fail-closed AC-(a) trigger nor lower confidence.
        (ReconciliationVerdict.NOT_APPLICABLE, VerificationStatus.CHAIN_VERIFIED, VerificationStatus.CHAIN_VERIFIED),
        (ReconciliationVerdict.NOT_APPLICABLE, VerificationStatus.UNVERIFIED, VerificationStatus.UNVERIFIED),
    ],
)
def test_apply_post_teardown_to_verification_status(verdict, proposed, expected):
    assert _report(verdict).apply_post_teardown_to_verification_status(proposed) is expected


def test_post_teardown_confirmed_open_dominates_mixed_report():
    """One residual CONFIRMED_OPEN fails the whole report even amid clean closes."""
    report = ReconciliationReport(
        deployment_id="d",
        entries=(
            PositionReconciliation("PositionType.LP", "1", "arbitrum", "lp", ReconciliationVerdict.DIVERGED_CLOSED),
            PositionReconciliation("PositionType.SUPPLY", "2", "ethereum", "aave_v3", ReconciliationVerdict.CONFIRMED_OPEN),
        ),
    )
    assert report.has_confirmed_open
    assert report.apply_post_teardown_to_verification_status(VerificationStatus.CHAIN_VERIFIED) is VerificationStatus.FAILED


def test_post_teardown_empty_report_passes_through():
    """Nothing read POST-teardown ⇒ no signal ⇒ proposed status is untouched."""
    empty = ReconciliationReport(deployment_id="d", entries=())
    assert not empty.has_confirmed_open
    for status in VerificationStatus:
        assert empty.apply_post_teardown_to_verification_status(status) is status


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
