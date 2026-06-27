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
    - ``UNVERIFIABLE`` — no per-position chain read is available for this position
      (no gateway client, no market, an unsupported primitive in Plan-A scope, or
      a transient read fault). ``None`` means *unknown*, never *closed* — the
      position stays in the teardown set (Empty ≠ Zero), but the closure cannot be
      proven, so TD-15 must not certify ``CHAIN_VERIFIED`` either.
    """

    CONFIRMED_OPEN = "confirmed_open"
    DIVERGED_CLOSED = "diverged_closed"
    UNVERIFIABLE = "unverifiable"


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
    def has_divergence(self) -> bool:
        """True iff at least one KNOWN position diverged (ledger-open, chain-closed)."""
        return any(e.diverged for e in self.entries)

    @property
    def has_unverifiable(self) -> bool:
        """True iff at least one KNOWN position could not be confirmed on-chain."""
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


async def _reconcile_lp(
    *, position: PositionInfo, gateway_client: Any, network: str
) -> tuple[ReconciliationVerdict, str]:
    """Protocol-scoped LP reconciliation via the gateway-routed TD-05 read."""
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
            "🛑 TD-08 Plan-A reconciliation: %d/%d known positions chain-confirmed open, %d unverifiable",
            len(report.confirmed),
            report.checked_count,
            len(report.unverifiable),
        )
    return report


__all__ = [
    "PositionReconciliation",
    "ReconciliationReport",
    "ReconciliationVerdict",
    "reconcile_known_positions_against_chain",
]
