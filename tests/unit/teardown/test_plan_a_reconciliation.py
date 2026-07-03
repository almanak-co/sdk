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
    _lending_market_id,
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


def _compound_v3_position(leg: PositionType, *, token: str = "WETH", market: str = "usdc") -> PositionInfo:
    """A Compound V3 lending leg exactly as ``compound_v3_lifecycle`` (and every
    sibling Compound V3 strategy) emits it from ``get_open_positions()``
    (VIB-5518): ``position_id`` is a synthetic, human-readable label
    (``compound-collateral-<token>-<chain>`` / ``compound-borrow-<token>-<chain>``),
    never a valid Comet market key, and the real market key lives under the
    ``"market"`` detail — not ``"market_id"``.
    """
    kind = "collateral" if leg is PositionType.SUPPLY else "borrow"
    details: dict[str, str] = {"asset": token, "market": market}
    if leg is PositionType.SUPPLY:
        details["type"] = "collateral"
    return PositionInfo(
        position_type=leg,
        position_id=f"compound-{kind}-{token}-base",
        chain="base",
        protocol="compound_v3",
        value_usd=Decimal("0"),
        details=details,
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


class _MarketKeyStrictMarket:
    """Reproduces Compound V3's real ``position_health`` failure mode (VIB-5518).

    ``PositionHealthProvider._get_market_health`` raises
    ``ValueError(f"{protocol} market '{market_id}' not found on {self._chain}.")``
    for any market key it does not recognize (``almanak/framework/data/
    position_health.py``). This stub reproduces exactly that fail-closed
    behaviour so the reconciliation test proves the FULL resolution path — not
    just the ``_lending_market_id`` unit — rejects the synthetic
    ``position_id`` and accepts only the real Comet market key.
    """

    def __init__(self, *, expected_market_id: str, health) -> None:
        self._expected_market_id = expected_market_id
        self._health = health

    def position_health(self, protocol, market_id, *, collateral_price_usd=None, debt_price_usd=None):
        if market_id != self._expected_market_id:
            raise ValueError(f"{protocol} market {market_id!r} not found on base.")
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
# Compound V3 position-id -> market-key resolution (VIB-5518)
# ---------------------------------------------------------------------------


def test_lending_market_id_prefers_explicit_market_id():
    """Registry-derived / Morpho-style ``market_id`` wins over ``position_id``."""
    pos = PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="unrelated-label",
        chain="ethereum",
        protocol="morpho_blue",
        value_usd=Decimal("0"),
        details={"market_id": "0xbytes32mkt"},
    )
    assert _lending_market_id(pos) == "0xbytes32mkt"


def test_lending_market_id_falls_back_to_market_key():
    """Compound V3's convention (``details['market']``, no ``market_id``) resolves
    to the real Comet key, NOT the synthetic ``position_id`` label."""
    pos = _compound_v3_position(PositionType.SUPPLY, token="WETH", market="usdc")
    assert pos.position_id == "compound-collateral-WETH-base"  # sanity: id is NOT a market key
    assert _lending_market_id(pos) == "usdc"


def test_lending_market_id_market_id_beats_market_when_both_present():
    pos = PositionInfo(
        position_type=PositionType.BORROW,
        position_id="irrelevant",
        chain="base",
        protocol="compound_v3",
        value_usd=Decimal("0"),
        details={"market_id": "0xmkt", "market": "usdc"},
    )
    assert _lending_market_id(pos) == "0xmkt"


def test_lending_market_id_last_resort_is_position_id():
    """No known detail key at all -> unchanged legacy fallback (Aave-family: the
    market id is ignored for whole-account protocols, so this stays harmless)."""
    pos = PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="aave-supply-WETH-polygon",
        chain="polygon",
        protocol="aave_v3",
        value_usd=Decimal("0"),
        details={"asset": "WETH"},
    )
    assert _lending_market_id(pos) == "aave-supply-WETH-polygon"


@pytest.mark.asyncio
async def test_compound_v3_collateral_confirmed_open_via_market_detail():
    """End-to-end (VIB-5518): a Compound V3 SUPPLY leg reconciles CONFIRMED_OPEN
    using the real Comet market key resolved from ``details['market']``. Before
    the fix, the synthetic ``position_id`` would have been sent as the market
    key and ``_MarketKeyStrictMarket`` would raise -> UNVERIFIABLE forever."""
    market = _MarketKeyStrictMarket(expected_market_id="usdc", health=_Health(Decimal("1000"), Decimal("0")))
    report = await reconcile_known_positions_against_chain(
        summary=_summary(_compound_v3_position(PositionType.SUPPLY)), gateway_client=None, market=market
    )
    assert report.entries[0].verdict is ReconciliationVerdict.CONFIRMED_OPEN


@pytest.mark.asyncio
async def test_compound_v3_debt_diverged_closed_via_market_detail():
    """A cleanly-closed Compound V3 BORROW leg (debt back to zero) resolves the
    real market key and reconciles DIVERGED_CLOSED — the GOOD post-teardown
    outcome — instead of being stuck UNVERIFIABLE by a bogus market key."""
    market = _MarketKeyStrictMarket(expected_market_id="usdc", health=_Health(Decimal("0"), Decimal("0")))
    report = await reconcile_known_positions_against_chain(
        summary=_summary(_compound_v3_position(PositionType.BORROW)), gateway_client=None, market=market
    )
    assert report.entries[0].verdict is ReconciliationVerdict.DIVERGED_CLOSED


@pytest.mark.asyncio
async def test_compound_v3_residual_debt_confirmed_open_flips_teardown_failed():
    """The TD-15 fail-closed contract this ticket restores: a genuine residual
    Compound V3 debt leg post-teardown must compose to FAILED, not be silently
    swallowed as UNVERIFIABLE (which the pre-fix bug made unconditional)."""
    market = _MarketKeyStrictMarket(expected_market_id="usdc", health=_Health(Decimal("0"), Decimal("500")))
    report = await reconcile_known_positions_against_chain(
        summary=_summary(_compound_v3_position(PositionType.BORROW)), gateway_client=None, market=market
    )
    assert report.entries[0].verdict is ReconciliationVerdict.CONFIRMED_OPEN
    assert report.has_confirmed_open
    assert report.apply_post_teardown_to_verification_status(VerificationStatus.UNVERIFIED) is VerificationStatus.FAILED


@pytest.mark.asyncio
async def test_compound_v3_bogus_position_id_market_key_reproduces_pre_fix_bug():
    """Guards the regression itself: if the market key resolved to the synthetic
    ``position_id`` (the pre-fix behaviour), the strict-market stub raises and
    the CHECK degrades to UNVERIFIABLE. Pins the exact failure this ticket
    fixes, so a future regression that reverts the fallback order is caught."""
    market = _MarketKeyStrictMarket(expected_market_id="usdc", health=_Health(Decimal("1000"), Decimal("0")))
    bogus = PositionInfo(
        position_type=PositionType.SUPPLY,
        position_id="compound-collateral-WETH-base",
        chain="base",
        protocol="compound_v3",
        value_usd=Decimal("0"),
        details={"asset": "WETH"},  # no market_id, no market -> forced to fall back to position_id
    )
    report = await reconcile_known_positions_against_chain(summary=_summary(bogus), gateway_client=None, market=market)
    assert report.entries[0].verdict is ReconciliationVerdict.UNVERIFIABLE


@pytest.mark.asyncio
async def test_compound_v3_symbol_resolved_from_asset_detail_not_empty():
    """VIB-5518 (secondary): the token symbol feeding the best-effort price
    lookup is resolved from ``details['asset']`` — the key Compound V3 (and
    Aave/Benqi) strategies use — never left empty. An empty symbol makes
    ``market.price('')`` fire a doomed lookup that can block 15-30s per leg. The
    CHECK verdict is unaffected either way (amounts unused), so this pins the
    efficiency contract: the resolved symbol reaches the price read."""
    priced: list[str] = []

    class _SpyMarket:
        def position_health(self, protocol, market_id, *, collateral_price_usd=None, debt_price_usd=None):
            return _Health(Decimal("1000"), Decimal("0"))

        def price(self, token):
            priced.append(token)
            return Decimal("2000")

    report = await reconcile_known_positions_against_chain(
        summary=_summary(_compound_v3_position(PositionType.SUPPLY, token="WETH")),
        gateway_client=None,
        market=_SpyMarket(),
    )
    assert report.entries[0].verdict is ReconciliationVerdict.CONFIRMED_OPEN
    # The real token symbol from details['asset'] reached the price read; the
    # empty-string doomed lookup never happened.
    assert "WETH" in priced
    assert "" not in priced


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
