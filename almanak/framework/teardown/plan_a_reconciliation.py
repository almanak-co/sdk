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

**Severity is phase-aware; verdicts are not (VIB-5923).** The same function
serves the PRE-execution callers and the TD-15 POST-teardown re-read. PRE, a
``DIVERGED_CLOSED`` verdict means the enumeration is STALE — an anomaly worth an
ERROR page. POST (``phase="post"``), the exact same verdict is the EXPECTED
SUCCESS signal for a position that just closed, so it logs INFO. Only the log
severity/wording branches on ``phase``: verdicts, the returned
:class:`ReconciliationReport`, and both ``apply_*_to_verification_status`` folds
are identical in either phase.

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
from dataclasses import dataclass, field, replace
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal

from almanak.framework.teardown.models import PositionType, VerificationStatus

if TYPE_CHECKING:
    from almanak.framework.market import MarketSnapshot
    from almanak.framework.teardown.models import PositionInfo, TeardownPositionSummary

logger = logging.getLogger(__name__)

# A leg whose live USD value is at/under this threshold is treated as
# measured-closed, mirroring ``LiveLendingPosition.has_exposure``'s default so a
# market the WARM ledger still lists is not flagged "diverged" over rounding dust.
_DUST_USD = Decimal("0.01")

# Which teardown lane called the CHECK (VIB-5923). This selects **log severity
# only** — verdicts, the returned :class:`ReconciliationReport`, and the
# ``apply_*_to_verification_status`` folds are byte-identical in both phases.
#
# - ``"pre"`` (default) — the PRE-execution callers (runner lane
#   ``runner_teardown``; CLI lane ``TeardownManager._pre_teardown_reconciliation``).
#   There, "the WARM ledger believes OPEN but chain reports CLOSED" means the
#   enumeration is STALE — an anomaly that must page: ERROR.
# - ``"post"`` — the TD-15 POST-teardown caller
#   (``TeardownManager.verify_closure_against_chain``), which deliberately
#   re-reads the SAME pre-execution KNOWN set AFTER every closing intent fired.
#   There, "chain reports CLOSED" is the EXPECTED SUCCESS signal for every
#   properly closed position, so paging ERROR on it false-alarms on every
#   healthy teardown. Post-phase divergence is INFO.
ReconciliationPhase = Literal["pre", "post"]
_PHASE_PRE: ReconciliationPhase = "pre"
_PHASE_POST: ReconciliationPhase = "post"
_VALID_PHASES: tuple[ReconciliationPhase, ...] = (_PHASE_PRE, _PHASE_POST)


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

    def downgrade_unattributable_confirmed_open(
        self, position_keys: frozenset[tuple[str, str, str]], reason: str
    ) -> ReconciliationReport:
        """Return a copy with the named ``CONFIRMED_OPEN`` entries downgraded to ``NOT_APPLICABLE``.

        VIB-5936: a POST-teardown ``CONFIRMED_OPEN`` whose evidence is a
        WHOLE-account aggregate read (BENQI's summed qiToken markets; the Aave
        family's ``getUserAccountData``) is structurally unscoped — a nonzero
        aggregate may belong entirely to OTHER markets the wallet holds
        (pre-existing history unrelated to the strategy). When the position's own
        TD-14 post-condition hook already MEASURED it closed on-chain, the
        finer-grained proof is final (the ``NOT_APPLICABLE`` contract above:
        Surface-A authority, no-op here) and the aggregate residual must not
        re-open it. The CALLER owns both predicates (hook-proven + whole-account
        read) and passes only the position identities that satisfy them —
        lowercased ``(protocol, chain, position_id)`` triples, matching on the
        FULL identity because a bare ``position_id`` can collide across
        protocols/chains and an empty id must never match; entries not
        named, or not ``CONFIRMED_OPEN``, pass through byte-identical.

        Only ever downgrades attribution — never flips a verdict toward closed,
        never touches ``DIVERGED_CLOSED`` / ``UNVERIFIABLE``, and the residual
        itself stays visible in the entry ``detail`` (appended ``reason``).
        """
        if not position_keys:
            return self

        def _key(e: PositionReconciliation) -> tuple[str, str, str]:
            # All three components lowercased for a case-insensitive match
            # (CodeRabbit): synthetic lending position_ids embed EVM addresses,
            # whose case is a checksum, not identity — two ids differing only by
            # case are the SAME position. The caller normalizes identically.
            return (
                str(e.protocol or "").strip().lower(),
                str(e.chain or "").strip().lower(),
                str(e.position_id or "").strip().lower(),
            )

        entries = tuple(
            replace(
                e,
                verdict=ReconciliationVerdict.NOT_APPLICABLE,
                detail=f"{e.detail} — {reason}" if e.detail else reason,
            )
            if e.verdict is ReconciliationVerdict.CONFIRMED_OPEN
            and str(e.position_id or "").strip()
            and _key(e) in position_keys
            else e
            for e in self.entries
        )
        return replace(self, entries=entries)

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

    Registry-derived lending rows carry ``details['market_id']``. Strategy-owned
    ``get_open_positions()`` implementations use two different detail-key
    conventions today (VIB-5518): Morpho Blue's lifecycle sets ``market_id``
    directly (the bytes32 market id), matching the registry convention; every
    Compound V3 strategy (``compound_v3_lifecycle`` and its per-chain/per-market
    siblings) instead sets ``market`` — the Comet market key (``"usdc"``,
    ``"weth"``, ``"usdc_e"``) that is the SAME string the strategy already threads
    through its own ``Intent.supply(..., market_id=self.market, ...)`` /
    ``Intent.borrow(..., market_id=self.market, ...)`` calls. Try both known
    conventions first.

    **Synthetic-market fallback (VIB-5795).** Euler V2 / Silo V2 / BENQI positions
    carry NEITHER detail key: their ``get_open_positions()`` emits a single-leg
    ``details={"asset": <symbol>, "type": ...}`` and a synthetic ``position_id``
    (``"euler_v2-collateral-WETH-ethereum"``) that is not a market key. Their real
    id is a token-derived synthetic id (euler/silo: ``"<col>/<loan>"`` or the
    collateral-only ``"<col>"``; BENQI: a fixed whole-account id). Before the
    ``position_id`` last-resort, reconstruct it from the position's own asset via
    the SAME VIB-5775 resolution seam the valuation guard and ``get_health`` use
    (:meth:`~almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry.resolve_market_id`)
    — a SUPPLY leg names the collateral, a BORROW leg names the loan. The resolver
    is contracted to fail **closed** to ``None`` (never guess) on ambiguity (a
    BORROW-only ref whose debt token is backed by several collaterals), an
    uncatalogued token, or a protocol with no ref resolver — so those keep today's
    fail-closed behaviour below rather than binding a wrong market (Empty ≠ Zero).

    ``position_id`` is a **last-resort** fallback, not a market key. For
    whole-account protocols (the Aave family) ``position_health`` ignores
    ``market_id`` entirely, so an arbitrary ``position_id`` string is harmless
    there. But for a per-market protocol whose position carries neither detail key
    AND whose asset the resolver could not turn into a catalogued market,
    ``position_health`` raises ``market '<position_id>' not found`` — caught by
    :func:`~almanak.framework.teardown.live_position_reads.redrive_lending_position`
    and surfaced as a fail-closed ``UNVERIFIABLE`` Plan-A verdict (an unresolvable
    market never masquerades as a confirmed reconciliation), never a wrong-market
    false verify.
    """
    details = position.details if isinstance(position.details, dict) else {}
    for key in ("market_id", "market"):
        value = details.get(key)
        if value:
            return str(value)
    resolved = _resolve_synthetic_market_id(position, details)
    if resolved:
        return resolved
    return str(position.position_id)


def _resolve_synthetic_market_id(position: PositionInfo, details: dict[str, Any]) -> str | None:
    """Reconstruct a synthetic-market id from a single-leg lending position (VIB-5795).

    Builds a :class:`~almanak.connectors._strategy_base.lending_read_base.LendingPositionRef`
    from the position's ``protocol`` / ``chain`` and its one known leg token
    (``details['asset']`` — the collateral for a SUPPLY, the loan for a BORROW) and
    routes it through the VIB-5775
    :meth:`~almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry.resolve_market_id`
    seam. Returns the resolved id, or ``None`` when the seam fails closed
    (ambiguous / uncatalogued token, no ref resolver, unknown protocol) — the
    caller then keeps its ``position_id`` last-resort (Empty ≠ Zero: never a
    guessed market). BENQI's whole-account resolver returns its fixed id for any
    (or no) token, so a BENQI leg resolves regardless of which asset it names.
    """
    protocol = str(getattr(position, "protocol", "") or "")
    chain = str(getattr(position, "chain", "") or "")
    if not protocol or not chain:
        return None
    asset = details.get("asset") or details.get("asset_symbol")
    asset = str(asset) if asset else None
    # ``==`` (not ``is``): PositionType is a StrEnum, and a position rebuilt
    # from persisted state may carry the plain string — identity would silently
    # misroute a "SUPPLY" string to the loan leg (gemini/CodeRabbit, PR #3336).
    is_supply = position.position_type == PositionType.SUPPLY
    try:
        from almanak.connectors._strategy_base.lending_read_base import LendingPositionRef
        from almanak.connectors._strategy_base.lending_read_registry import LendingReadRegistry

        ref = LendingPositionRef(
            protocol=protocol,
            chain=chain,
            collateral_token=asset if is_supply else None,
            loan_token=asset if not is_supply else None,
        )
        return LendingReadRegistry.resolve_market_id(ref)
    except Exception:  # noqa: BLE001 — resolution must never fault the teardown lane
        logger.debug(
            "TD-15 _lending_market_id: synthetic market-id resolution raised for %s %s on %s "
            "— falling through to position_id (UNVERIFIABLE)",
            protocol,
            position.position_id,
            chain,
            exc_info=True,
        )
        return None


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
        return ReconciliationVerdict.CONFIRMED_OPEN, "own-protocol NPM reports liquidity > 0"
    if verdict is False:
        return (
            ReconciliationVerdict.DIVERGED_CLOSED,
            "own-protocol NPM reports liquidity == 0 (measured closed/burned)",
        )
    return (
        ReconciliationVerdict.UNVERIFIABLE,
        "no NPM registered for the position's protocol on this chain / read faulted",
    )


def _reconcile_lending(*, position: PositionInfo, market: MarketSnapshot | None) -> tuple[ReconciliationVerdict, str]:
    """Protocol-scoped lending reconciliation via the gateway-routed TD-05 read."""
    if market is None:
        return ReconciliationVerdict.UNVERIFIABLE, "no market snapshot to read position_health"
    from almanak.framework.teardown.live_position_reads import redrive_lending_position

    details = position.details if isinstance(position.details, dict) else {}
    # Same detail-key-convention gap as _lending_market_id (VIB-5518): registry
    # rows carry "asset_symbol", but strategy-owned lending PositionInfo (Compound
    # V3, Aave, Benqi, ...) stores the token under "asset". Resolve either. The
    # symbol only feeds the best-effort USD->token-amount conversion inside
    # redrive_lending_position, which the CHECK never reads — but an empty symbol
    # makes market.price("") fire a doomed lookup that can block 15-30s per leg
    # before failing. Resolving the real symbol keeps the price lookup cheap;
    # leaving it empty is still correct (amounts unused), just slow.
    symbol = str(details.get("asset_symbol") or details.get("asset") or "")
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


def _log_reconciliation_summary(report: ReconciliationReport, *, is_post: bool) -> None:
    """Emit the one-line phase-appropriate summary for a finished CHECK.

    Severity mirrors what the verdict MEANS in each lane (VIB-5923) — the
    verdicts themselves are phase-invariant:

    - PRE: divergence = stale enumeration ⇒ ERROR; otherwise "chain-confirmed
      open" is the healthy reading ⇒ INFO. Byte-identical to pre-VIB-5923.
    - POST: still-OPEN is the anomaly (residual on-chain risk at the moment of
      maximum exposure), closed is the expected success signal. So the summary
      is keyed on ``has_confirmed_open``, not on divergence: any residual open
      ⇒ WARNING, otherwise ⇒ INFO.

    **Division of responsibility**: POST-teardown this function never ERRORs on
    a residual-open position. The loud fail-closed per-position
    "🛑 TD-15 fail-closed: … STILL OPEN" ERROR and the ``FAILED`` flip belong to
    the TD-15 caller (``TeardownManager.verify_closure_against_chain``), which
    runs immediately after; paging here as well would double-page the operator.
    """
    if not report.checked_count:
        return
    if is_post:
        if report.has_confirmed_open:
            logger.warning(
                "TD-08 post-teardown reconciliation: %d/%d known positions STILL read OPEN on-chain "
                "after teardown — residual risk; TD-15 fail-closed verification owns the page. "
                "%d chain-confirmed CLOSED, %d unverifiable, %d not-applicable.",
                len(report.confirmed),
                report.checked_count,
                len(report.diverged),
                len(report.unverifiable),
                len(report.not_applicable),
            )
        else:
            logger.info(
                "TD-08 Plan-A post-teardown reconciliation: %d/%d known positions chain-confirmed CLOSED "
                "post-teardown (expected closure signal), 0 still open, %d unverifiable (deliberate "
                "no-op), %d not-applicable.",
                len(report.diverged),
                report.checked_count,
                len(report.unverifiable),
                len(report.not_applicable),
            )
        return
    if report.has_divergence:
        logger.error(
            "🛑 TD-08 Plan-A reconciliation found %d divergence(s) across %d known position(s): %s",
            len(report.diverged),
            report.checked_count,
            report.to_dict(),
        )
        return
    logger.info(
        "🛑 TD-08 Plan-A reconciliation: %d/%d known positions chain-confirmed open, %d unverifiable, "
        "%d not-applicable",
        len(report.confirmed),
        report.checked_count,
        len(report.unverifiable),
        len(report.not_applicable),
    )


async def reconcile_known_positions_against_chain(
    *,
    summary: TeardownPositionSummary,
    gateway_client: Any,
    market: MarketSnapshot | None,
    network: str = "",
    phase: ReconciliationPhase = "pre",
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
        phase: Which teardown lane is calling — ``"pre"`` (default; before any
            closing intent fired) or ``"post"`` (the TD-15 re-read after every
            closing intent fired). **Log severity only** (VIB-5923): a
            ``DIVERGED_CLOSED`` verdict is an anomaly PRE-teardown (stale
            enumeration → ERROR) but the EXPECTED SUCCESS signal POST-teardown
            (the position closed → INFO), while a still-``CONFIRMED_OPEN``
            position is healthy PRE-teardown but residual risk POST-teardown
            (→ WARNING summary; the fail-closed ERROR page belongs to the TD-15
            caller). Verdicts, the returned report, and the
            ``apply_*_to_verification_status`` folds are identical in both phases.
            An unrecognised value logs ERROR and degrades to ``"pre"``.

    Returns:
        A :class:`ReconciliationReport`. An empty ``summary`` yields an empty
        report (``checked_count == 0``) — nothing to reconcile.

    Never raises — reconciliation must never fault the teardown lane. That
    includes an unrecognised ``phase``: both callers wrap this call in a broad
    ``except Exception`` that fails **OPEN** (the TD-15 caller would silently
    skip its AC-(a) residual-open → ``FAILED`` fold), so raising here would
    convert a typo into a swallowed money-safety check. An unrecognised value
    logs ERROR and degrades to ``"pre"`` — the loud/conservative severity.
    """
    if phase not in _VALID_PHASES:
        # Fail LOUD but never fault the lane: degrade to the conservative
        # (pre) severity rather than raise into the callers' fail-open
        # ``except Exception`` handlers (VIB-5923 audit round 2).
        logger.error(
            "TD-08 reconciliation called with invalid phase %r — treating as %r "
            "(loud/conservative severity). Valid phases: %r.",
            phase,
            _PHASE_PRE,
            _VALID_PHASES,
        )
        phase = _PHASE_PRE
    is_post = phase == _PHASE_POST
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
        if entry.diverged and is_post:
            # POST-teardown: this is the happy path — the position the ledger
            # believed open before teardown is now chain-confirmed CLOSED. INFO,
            # no 🛑, no page (VIB-5923).
            logger.info(
                "TD-08 post-teardown reconciliation: %s %s (%s) on %s chain-confirmed CLOSED after "
                "teardown — expected closure signal (was OPEN pre-teardown). %s",
                entry.protocol,
                entry.position_type,
                entry.position_id,
                entry.chain,
                entry.detail,
            )
        elif entry.diverged:
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
        elif entry.unverifiable and is_post:
            # POST-teardown an unreadable position (e.g. a burned LP NFT reading
            # back "not found") is a deliberate NO-OP for verification — it never
            # lowers a proposed CHAIN_VERIFIED (see
            # ``apply_post_teardown_to_verification_status`` and the TD-15
            # ``verify_closure_against_chain`` docstring). Still WARNING: it is
            # informative (closure could not be positively proven for this leg),
            # just not an anomaly to page on.
            logger.warning(
                "TD-08 post-teardown reconciliation UNVERIFIABLE: could not re-read %s %s (%s) on %s "
                "after teardown — %s. Deliberate no-op for verification (an unreadable post-teardown "
                "position, e.g. a burned LP NFT, never lowers the closure confidence).",
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
    _log_reconciliation_summary(report, is_post=is_post)
    return report


__all__ = [
    "PositionReconciliation",
    "ReconciliationPhase",
    "ReconciliationReport",
    "ReconciliationVerdict",
    "reconcile_known_positions_against_chain",
]
