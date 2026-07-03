"""Plan-A on-chain reconciliation as a CHECK (not an action) — VIB-5466 / TD-08.

After teardown ENUMERATION produces the KNOWN open-position set (TD-01..TD-05 —
the strategy's ``get_open_positions`` reconciled against the ``position_registry``
WARM tier), this module does a **protocol-scoped chain read per KNOWN position**
and compares the chain's answer to the WARM ledger's belief. Divergence — the
WARM ledger believes a position is OPEN but the chain says it is CLOSED, or the
chain cannot confirm it — is flagged **loudly** with a **structured signal** that
the TD-15 fail-closed verification step consumes (see
:meth:`ReconciliationReport.apply_to_verification_status`, which composes with the
TD-14 :class:`VerificationStatus` rather than fighting it).

**This is a CHECK, not an action.** Nothing here closes, sweeps, repays, or
withdraws anything. It emits no :class:`Intent`. It reads chain state for the
positions the framework *already knows* and returns an observation. The teardown
risk-reducing intents are unaffected by a divergence — teardown's inverted
failure semantics (blueprint 14 §Teardown) say the CHECK is loud but must never
block the next risk-reducing intent.

**Position-scoped, NEVER a wallet-wide sweep.** Every read here is scoped to an
identity the framework already enumerated — a single LP NFT ``token_id``, a
config-known lending market. The wallet-wide on-chain discovery that finds
*unknown* token ids / commingled balances is **Plan B**
(``teardown.discovery`` / ``teardown.lp_recovery``), a separate lane. This module
imports neither.

**Gateway boundary (CLAUDE.md §Gateway boundary).** All chain reads are delegated
to the already-gateway-routed TD-05 primitives in
:mod:`almanak.framework.teardown.live_position_reads`
(``chain_verify_lp_open`` for LP, ``redrive_lending_position`` for lending). No
direct RPC / HTTP is opened here, and no new egress is introduced.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from almanak.framework.teardown.models import PositionType, VerificationStatus

if TYPE_CHECKING:
    from almanak.framework.market import MarketSnapshot
    from almanak.framework.teardown.models import PositionInfo, TeardownPositionSummary

logger = logging.getLogger(__name__)

# A leg whose live USD value is at/under this threshold is treated as
# measured-closed, mirroring ``LiveLendingPosition.has_exposure``'s default so a
# market the WARM ledger still lists is not flagged "diverged" over rounding dust.
_DUST_USD = Decimal("0.01")


class ReconciliationVerdict(StrEnum):
    """How a single KNOWN position's WARM-ledger belief reconciled against chain.

    The WARM ledger belief for an *enumerated* position is always "OPEN" (it
    would not be in the teardown set otherwise), so the verdict records what the
    protocol-scoped chain read said about that belief:

    - ``CONFIRMED_OPEN`` — the chain agrees the position is live. No divergence.
    - ``DIVERGED_CLOSED`` — the chain reports the position CLOSED (LP liquidity
      ``== 0``; lending leg value at/under dust). The WARM ledger is stale: a
      LOUD, structured divergence. Risk is *lower* than the ledger believed (the
      position is already closed), so the teardown plans a harmless no-op for it —
      but the count the operator sees is optimistic, so TD-15 must not certify the
      teardown ``CHAIN_VERIFIED`` off a stale enumeration.
    - ``UNVERIFIABLE`` — a chain read WAS attempted for this position (the
      primitive is in Plan-A scope) but came back inconclusive (no gateway
      client, no market, a transient read fault). ``None`` means *unknown*,
      never *closed* — the position stays in the teardown set (Empty ≠ Zero),
      but the closure cannot be proven, so TD-15 must not certify
      ``CHAIN_VERIFIED`` either.
    - ``NOT_APPLICABLE`` — this Plan-A chain read is **structurally scoped to a
      different position shape** and was never attempted (VIB-5522). The LP
      reconciliation delegates to the NFT-only ``positions(tokenId)`` read
      (``chain_verify_lp_open``), which can only answer for ERC-721 V3-family
      LPs; a non-NFT LP position (TraderJoe V2 Liquidity Book's ERC-1155 bins,
      Uniswap V4's distinct PositionManager, ...) can never be confirmed OR
      denied by it — attempting it always resolves to "not found", which is
      indistinguishable from a genuine read failure. ``NOT_APPLICABLE`` is
      **not** the same signal as ``UNVERIFIABLE``: it carries zero information
      about the position's open-ness either way, so — unlike ``UNVERIFIABLE`` —
      it must never participate in TD-15's confidence-lowering
      (:attr:`ReconciliationReport.has_unverifiable` excludes it). The
      authoritative closure signal for a position this read cannot reach is its
      own registered TD-14 post-condition (Surface A); a measured PASS there is
      final, and this inapplicable Plan-A path is a no-op, never a downgrade.
    """

    CONFIRMED_OPEN = "confirmed_open"
    DIVERGED_CLOSED = "diverged_closed"
    UNVERIFIABLE = "unverifiable"
    NOT_APPLICABLE = "not_applicable"


@dataclass(frozen=True)
class PositionReconciliation:
    """Reconciliation outcome for one KNOWN position (WARM ledger vs chain)."""

    position_type: str
    position_id: str
    chain: str
    protocol: str
    verdict: ReconciliationVerdict
    detail: str = ""

    @property
    def diverged(self) -> bool:
        """True iff the chain contradicted the WARM ledger's OPEN belief."""
        return self.verdict is ReconciliationVerdict.DIVERGED_CLOSED

    @property
    def unverifiable(self) -> bool:
        """True iff the chain could not confirm the WARM ledger's OPEN belief."""
        return self.verdict is ReconciliationVerdict.UNVERIFIABLE

    @property
    def not_applicable(self) -> bool:
        """True iff this Plan-A read is structurally out of scope for this position.

        VIB-5522: e.g. the NFT-only LP read attempted against a non-NFT
        (ERC-1155) LP position. Deliberately distinct from
        :attr:`unverifiable`: it must never feed TD-15's confidence-lowering.
        """
        return self.verdict is ReconciliationVerdict.NOT_APPLICABLE

    def to_dict(self) -> dict[str, Any]:
        """Structured form for logging / a future TD-15 persistence surface."""
        return {
            "position_type": self.position_type,
            "position_id": self.position_id,
            "chain": self.chain,
            "protocol": self.protocol,
            "verdict": self.verdict.value,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class ReconciliationReport:
    """Structured Plan-A reconciliation signal — the field TD-15 consumes.

    Carries one :class:`PositionReconciliation` per KNOWN position the check
    examined. ``has_divergence`` / ``has_unverifiable`` are the two fail-closed
    triggers a downstream verifier reads; :meth:`apply_to_verification_status`
    folds both into the TD-14 :class:`VerificationStatus` without ever upgrading
    it (compose, not fight).
    """

    deployment_id: str
    entries: tuple[PositionReconciliation, ...] = field(default_factory=tuple)

    @property
    def checked_count(self) -> int:
        """Number of KNOWN positions the check examined."""
        return len(self.entries)

    @property
    def confirmed(self) -> tuple[PositionReconciliation, ...]:
        """Positions the chain confirmed open (no divergence)."""
        return tuple(e for e in self.entries if e.verdict is ReconciliationVerdict.CONFIRMED_OPEN)

    @property
    def diverged(self) -> tuple[PositionReconciliation, ...]:
        """Positions the WARM ledger believed open but the chain reports CLOSED."""
        return tuple(e for e in self.entries if e.diverged)

    @property
    def unverifiable(self) -> tuple[PositionReconciliation, ...]:
        """Positions whose open-ness the chain could not confirm."""
        return tuple(e for e in self.entries if e.unverifiable)

    @property
    def not_applicable(self) -> tuple[PositionReconciliation, ...]:
        """Positions this Plan-A read is structurally out of scope for (VIB-5522).

        Kept separate from :attr:`unverifiable` for observability (both are
        logged/reported), but — unlike ``unverifiable`` — never feeds
        ``has_unverifiable`` / the TD-15 confidence-lowering it triggers.
        """
        return tuple(e for e in self.entries if e.not_applicable)

    @property
    def has_divergence(self) -> bool:
        """True iff at least one KNOWN position diverged (ledger-open, chain-closed)."""
        return any(e.diverged for e in self.entries)

    @property
    def has_unverifiable(self) -> bool:
        """True iff at least one KNOWN position could not be confirmed on-chain.

        Deliberately excludes ``NOT_APPLICABLE`` entries (VIB-5522): a
        structurally out-of-scope read (the NFT-only LP check against a
        non-NFT LP position) carries no information about openness either
        way, so it must never trigger TD-15's fail-closed downgrade — that
        downgrade is reserved for a read that COULD have answered but did
        not (Empty ≠ Zero, generalised to read *applicability*).
        """
        return any(e.unverifiable for e in self.entries)

    @property
    def has_confirmed_open(self) -> bool:
        """True iff at least one KNOWN position is STILL chain-CONFIRMED open.

        Read in the POST-teardown direction (TD-15 / VIB-5473). PRE-teardown a
        ``CONFIRMED_OPEN`` verdict is the expected, healthy state. AFTER every
        closing intent has fired, a position the chain still reports OPEN is
        residual on-chain risk the teardown failed to remove — the fail-closed
        trigger. See :meth:`apply_post_teardown_to_verification_status`.
        """
        return any(e.verdict is ReconciliationVerdict.CONFIRMED_OPEN for e in self.entries)

    @property
    def is_clean(self) -> bool:
        """True iff every examined position was chain-CONFIRMED open."""
        return self.checked_count > 0 and not self.has_divergence and not self.has_unverifiable

    def apply_to_verification_status(self, proposed: VerificationStatus) -> VerificationStatus:
        """Compose this reconciliation with a TD-14 verification status (fail-closed).

        The contract TD-15 relies on: a Plan-A reconciliation that found a
        divergence (ledger-open / chain-closed) **or** could not confirm a known
        position must never let a teardown be certified ``CHAIN_VERIFIED`` — the
        enumeration the verifier counted against was stale or unprovable. So a
        proposed ``CHAIN_VERIFIED`` is downgraded to ``UNVERIFIED`` whenever this
        report is not clean; every other status passes through untouched (this
        only ever *lowers* confidence, never raises it — it composes with TD-14,
        it does not fight it). ``FAILED`` stays ``FAILED``; ``UNVERIFIED`` /
        ``NOT_RUN`` are unchanged.
        """
        if proposed is VerificationStatus.CHAIN_VERIFIED and (self.has_divergence or self.has_unverifiable):
            return VerificationStatus.UNVERIFIED
        return proposed

    def apply_post_teardown_to_verification_status(self, proposed: VerificationStatus) -> VerificationStatus:
        """Compose a POST-teardown reconciliation with a TD-14 status (fail-closed).

        The POST-teardown mirror of :meth:`apply_to_verification_status` — the
        contract the TD-15 fail-closed verifier
        (:meth:`TeardownManager.verify_closure_against_chain`) relies on. Once the
        closing intents have fired the verdict directions invert relative to the
        pre-teardown CHECK:

        - ``CONFIRMED_OPEN`` — the chain STILL reports the position open: residual
          on-chain risk the teardown did not remove. Returns ``FAILED`` regardless
          of the proposed status — this is the AC-(a) fail-closed trigger (a
          stranded LP / collateral / debt eliminates the "reports success while a
          position is still open" false-success class).
        - ``DIVERGED_CLOSED`` / ``UNVERIFIABLE`` — neither lowers confidence.
          ``DIVERGED_CLOSED`` is the GOOD POST-teardown outcome (the closing intent
          worked). ``UNVERIFIABLE`` is deliberately a **no-op**: post-teardown a
          KNOWN position that Plan-A cannot re-read is overwhelmingly a position
          that was *closed and removed* — a burned Uniswap-V3 LP NFT reads back as
          "not found", which is the SUCCESS signal, not a doubt. The closure
          confidence for a primitive Plan-A cannot re-read is already owned by the
          TD-14 post-condition hook (the authority for the protocols it covers);
          letting an UNVERIFIABLE re-read drag a TD-14-proven ``CHAIN_VERIFIED``
          down to ``UNVERIFIED`` would mislabel every clean V3 LP teardown. The
          "never-existed / stale-enumeration" downgrade is owned by the PRE-teardown
          report (:meth:`apply_to_verification_status`, AC-(b)) — a different signal.

        Only ever *fails* (on residual open); never lowers a non-failed status and
        never raises one. ``checked_count == 0`` (nothing read) passes through.
        """
        if self.has_confirmed_open:
            return VerificationStatus.FAILED
        return proposed

    def to_dict(self) -> dict[str, Any]:
        """Structured form for the loud divergence log / future TD-15 surface."""
        return {
            "deployment_id": self.deployment_id,
            "checked": self.checked_count,
            "confirmed": len(self.confirmed),
            "diverged": len(self.diverged),
            "unverifiable": len(self.unverifiable),
            "not_applicable": len(self.not_applicable),
            "has_divergence": self.has_divergence,
            "entries": [e.to_dict() for e in self.entries],
        }


def _lending_market_id(position: PositionInfo) -> str:
    """Resolve a lending position's market identifier for ``position_health``.

    Registry-derived lending rows carry ``details['market_id']``; a strategy's
    own ``PositionInfo`` may instead use ``position_id`` as the market anchor.
    Prefer the explicit detail, fall back to the id.
    """
    details = position.details if isinstance(position.details, dict) else {}
    market_id = details.get("market_id")
    if market_id:
        return str(market_id)
    return str(position.position_id)


def _is_nft_lp_protocol(protocol: str) -> bool:
    """True iff ``protocol`` is an NFT-based (ERC-721) V3-family LP position.

    Sourced from the connector-owned ``AddressRegistry`` under
    :attr:`AbiFamily.V3_NPM` — the SAME membership
    ``teardown.discovery._NPM_PROTOCOLS`` uses to build the NPM walker
    ``chain_verify_lp_open`` delegates to — so this module never hardcodes a
    protocol slug and the applicability check can never drift out of sync
    with what the NFT read actually covers (VIB-5522).

    A non-member LP protocol (TraderJoe V2 Liquidity Book's ERC-1155 bins,
    Uniswap V4's distinct ``PositionManager`` rather than the classic V3
    NPM, and any future non-NFT LP shape) cannot be answered — confirmed OR
    denied — by the NFT-scoped ``positions(tokenId)`` read: attempting it
    always resolves to "not found on any registered NPM", which is
    structurally indistinguishable from a genuine read failure. Callers must
    treat a non-member as :attr:`ReconciliationVerdict.NOT_APPLICABLE`, never
    ``UNVERIFIABLE`` — the two are different signals (see the enum docstring).

    ``protocol`` is lower-cased before the membership test: registry slugs are
    canonical lower-case, so a position carrying a mixed-case protocol (a custom
    strategy / non-canonical registry entry) must not be mis-classified
    ``NOT_APPLICABLE`` — that would skip the residual check for a genuine V3 LP.
    """
    from almanak.connectors._strategy_base.address_registry import AbiFamily, AddressRegistry

    return protocol.lower() in AddressRegistry.protocols_with_abi(AbiFamily.V3_NPM)


async def _reconcile_lp(
    *, position: PositionInfo, gateway_client: Any, network: str
) -> tuple[ReconciliationVerdict, str]:
    """Protocol-scoped LP reconciliation via the gateway-routed TD-05 read.

    Scoped to NFT-based (ERC-721) V3-family LP protocols (VIB-5522): the
    delegate read, ``chain_verify_lp_open``, can only answer
    ``positions(tokenId).liquidity`` on a registered NonfungiblePositionManager.
    A non-NFT LP position is ``NOT_APPLICABLE`` — this Plan-A NFT read is
    skipped entirely (no wasted, guaranteed-null gateway round-trip) and the
    verdict carries no confidence-lowering weight; that position's closure is
    proven (or not) by its own registered TD-14 post-condition instead
    (Surface A — e.g. ``traderjoe_v2_post_condition``).
    """
    protocol = str(position.protocol or "")
    if not protocol:
        # Empty ≠ Zero, fail-closed: an LP position whose protocol we cannot even
        # determine is NOT ``NOT_APPLICABLE`` (which asserts "a known non-NFT LP,
        # deferred to its own post-condition"). Without a protocol there may be no
        # post-condition either — so NOT_APPLICABLE would leave it neither verified
        # nor confidence-lowered (fail-open). An unknown protocol is genuinely
        # UNVERIFIABLE — it must lower confidence via ``has_unverifiable``.
        return (
            ReconciliationVerdict.UNVERIFIABLE,
            "LP position has no protocol — cannot determine reconciliation "
            "applicability; fail-closed (UNVERIFIABLE, not NOT_APPLICABLE)",
        )
    if not _is_nft_lp_protocol(protocol):
        return (
            ReconciliationVerdict.NOT_APPLICABLE,
            f"protocol {position.protocol!r} is not an NFT-based (ERC-721) LP position — "
            "this Plan-A NFT read cannot verify it; deferring to its registered TD-14 "
            "post-condition",
        )
    if gateway_client is None:
        return ReconciliationVerdict.UNVERIFIABLE, "no gateway client to chain-verify LP"
    from almanak.framework.teardown.live_position_reads import chain_verify_lp_open

    verdict = await chain_verify_lp_open(gateway_client=gateway_client, position=position, network=network)
    if verdict is True:
        return ReconciliationVerdict.CONFIRMED_OPEN, "NPM reports liquidity > 0"
    if verdict is False:
        return ReconciliationVerdict.DIVERGED_CLOSED, "NPM reports liquidity == 0 (closed/burned)"
    return ReconciliationVerdict.UNVERIFIABLE, "LP NFT not found on a registered NPM / read unavailable"


def _reconcile_lending(*, position: PositionInfo, market: MarketSnapshot | None) -> tuple[ReconciliationVerdict, str]:
    """Protocol-scoped lending reconciliation via the gateway-routed TD-05 read."""
    if market is None:
        return ReconciliationVerdict.UNVERIFIABLE, "no market snapshot to read position_health"
    from almanak.framework.teardown.live_position_reads import redrive_lending_position

    details = position.details if isinstance(position.details, dict) else {}
    symbol = str(details.get("asset_symbol") or "")
    live = redrive_lending_position(
        market=market,
        protocol=position.protocol,
        market_id=_lending_market_id(position),
        collateral_token=symbol,
        borrow_token=symbol,
    )
    if live is None:
        return ReconciliationVerdict.UNVERIFIABLE, "position_health unavailable (unmeasured)"
    if position.position_type is PositionType.BORROW:
        leg_value = live.debt_value_usd
        leg = "debt"
    else:  # SUPPLY (collateral)
        leg_value = live.collateral_value_usd
        leg = "collateral"
    if leg_value > _DUST_USD:
        return ReconciliationVerdict.CONFIRMED_OPEN, f"{leg} value ${leg_value} on-chain"
    return ReconciliationVerdict.DIVERGED_CLOSED, f"{leg} value ${leg_value} at/under dust (closed)"


async def _reconcile_one(
    *, position: PositionInfo, gateway_client: Any, market: MarketSnapshot | None, network: str
) -> tuple[ReconciliationVerdict, str]:
    """Dispatch one KNOWN position to its protocol-scoped chain read.

    Never raises — a read fault degrades to ``UNVERIFIABLE`` (Empty ≠ Zero: an
    unknown is never treated as closed) so the CHECK can never fault the teardown
    lane (blueprint 14 §Teardown — the check is loud but must not block risk
    reduction).
    """
    try:
        if position.position_type is PositionType.LP:
            return await _reconcile_lp(position=position, gateway_client=gateway_client, network=network)
        if position.position_type in (PositionType.SUPPLY, PositionType.BORROW):
            return _reconcile_lending(position=position, market=market)
        # PERP / VAULT / STAKE / TOKEN / CEX / PREDICTION have no per-position
        # Plan-A chain-read capability yet — be honest: UNVERIFIABLE, never a
        # fabricated CONFIRMED. (Their per-position verify is owned by their own
        # cutover / post-condition tickets, not this read-path check.)
        return (
            ReconciliationVerdict.UNVERIFIABLE,
            f"no per-position Plan-A chain read for {position.position_type} positions",
        )
    except Exception:  # noqa: BLE001 — the CHECK must never fault the teardown lane
        logger.debug(
            "TD-08 reconciliation: read raised for %s %s — treating as UNVERIFIABLE",
            position.position_type,
            position.position_id,
            exc_info=True,
        )
        return ReconciliationVerdict.UNVERIFIABLE, "chain read raised (treated as unverifiable)"


async def reconcile_known_positions_against_chain(
    *,
    summary: TeardownPositionSummary,
    gateway_client: Any,
    market: MarketSnapshot | None,
    network: str = "",
) -> ReconciliationReport:
    """Plan-A reconciliation CHECK: confirm each KNOWN position's live chain state.

    For every position in the enumerated ``summary`` (the WARM-ledger belief), do
    a protocol-scoped chain read and compare. Emits a LOUD, structured signal on
    any divergence / unverifiable position and returns a :class:`ReconciliationReport`
    the TD-15 fail-closed verification consumes.

    **CHECK only** — closes/sweeps nothing, emits no intent, and is scoped to the
    positions already enumerated (never a wallet-wide scan; that is Plan B).

    Args:
        summary: The enumerated KNOWN open-position set (strategy enumeration
            reconciled against the ``position_registry`` WARM tier).
        gateway_client: A connected gateway client for the gateway-routed LP read.
            ``None`` ⇒ LP positions reconcile as ``UNVERIFIABLE``.
        market: A live :class:`MarketSnapshot` for the gateway-routed lending
            read. ``None`` ⇒ lending positions reconcile as ``UNVERIFIABLE``.
        network: Gateway network override (``""`` uses the gateway's configured
            network — the fork on a managed-Anvil run).

    Returns:
        A :class:`ReconciliationReport`. An empty ``summary`` yields an empty
        report (``checked_count == 0``) — nothing to reconcile.

    Never raises — reconciliation must never fault the teardown lane.
    """
    positions = list(getattr(summary, "positions", []) or [])
    entries: list[PositionReconciliation] = []
    # Reconcile sequentially (not via asyncio.gather): the KNOWN set is small
    # (typically 1-5 positions), so concurrency buys nothing measurable, and
    # sequential reads (a) keep the per-position loud divergence logs in a
    # deterministic order an operator can follow, and (b) avoid a burst of
    # concurrent gateway reads exactly while the gateway is busy servicing the
    # teardown's risk-reducing unwind RPCs.
    for position in positions:
        verdict, detail = await _reconcile_one(
            position=position, gateway_client=gateway_client, market=market, network=network
        )
        entry = PositionReconciliation(
            position_type=str(position.position_type),
            position_id=str(position.position_id),
            chain=str(position.chain or "").lower(),
            protocol=str(position.protocol or ""),
            verdict=verdict,
            detail=detail,
        )
        entries.append(entry)
        if entry.diverged:
            logger.error(
                "🛑 TD-08 reconciliation DIVERGENCE: WARM ledger believes %s %s (%s) on %s is OPEN "
                "but chain reports CLOSED — %s. CHECK only: not closing/sweeping; signalling for "
                "fail-closed verification.",
                entry.protocol,
                entry.position_type,
                entry.position_id,
                entry.chain,
                entry.detail,
            )
        elif entry.unverifiable:
            logger.warning(
                "🛑 TD-08 reconciliation UNVERIFIABLE: could not confirm %s %s (%s) on %s open on-chain — "
                "%s. Position retained in teardown set (Empty ≠ Zero); closure cannot be certified.",
                entry.protocol,
                entry.position_type,
                entry.position_id,
                entry.chain,
                entry.detail,
            )
        elif entry.not_applicable:
            # VIB-5522: NOT informative for TD-15 confidence — deliberately
            # debug-level (not warning/error), since this is the expected,
            # healthy shape for every non-NFT LP protocol, not an anomaly.
            logger.debug(
                "TD-08 reconciliation NOT_APPLICABLE: %s %s (%s) on %s is out of scope for this Plan-A "
                "read — %s. Deferring to its own TD-14 post-condition (no confidence impact).",
                entry.protocol,
                entry.position_type,
                entry.position_id,
                entry.chain,
                entry.detail,
            )

    report = ReconciliationReport(
        deployment_id=str(getattr(summary, "deployment_id", "") or ""), entries=tuple(entries)
    )
    if report.has_divergence:
        logger.error(
            "🛑 TD-08 Plan-A reconciliation found %d divergence(s) across %d known position(s): %s",
            len(report.diverged),
            report.checked_count,
            report.to_dict(),
        )
    elif report.checked_count:
        logger.info(
            "🛑 TD-08 Plan-A reconciliation: %d/%d known positions chain-confirmed open, %d unverifiable, "
            "%d not-applicable",
            len(report.confirmed),
            report.checked_count,
            len(report.unverifiable),
            len(report.not_applicable),
        )
    return report


__all__ = [
    "PositionReconciliation",
    "ReconciliationReport",
    "ReconciliationVerdict",
    "reconcile_known_positions_against_chain",
]
