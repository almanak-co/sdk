"""Pure state algebra for the capital-flow producer (VIB-5866 leg B, PR-B).

``PortfolioMetrics.deposits_usd`` / ``withdrawals_usd`` are *cumulative since
era start*. Keeping that cumulative honest across restarts, crashes and
partial scans is a state problem, not an RPC problem, so all of the decision
logic lives here as pure functions over one immutable record. The IO edge —
resolving web3 handles, scanning logs, persisting the record — lives in
``runner_state._populate_capital_flows``.

Two invariants drive the whole design:

- **Cursor and cumulative always travel together.** The record is read and
  written *wholesale*, never field by field. A cumulative that advanced
  without its cursor (or vice versa) would double-count or silently drop a
  deposit after a crash; because both mirrors carry the same object, recovery
  can simply take the mirror with the higher cursor.
- **Empty ≠ Zero.** ``None`` means "we could not measure this era" and is
  projected onto the metrics columns as ``None`` (the ``''`` storage sentinel
  from PR-C1). A measured zero stays ``Decimal("0")``. Nothing here ever
  substitutes one for the other.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from decimal import Decimal, InvalidOperation
from typing import Any

from almanak.framework.accounting.capital_flows import FlowClassification, TransferObservation

#: Bumped only when the persisted record shape changes incompatibly.
SCHEMA_VERSION = 1

STATUS_PENDING = "pending"
STATUS_MEASURED = "measured"
STATUS_UNMEASURED = "unmeasured"

# Reason taxonomy (VIB-5866). Each is permanent for the era: v1 never
# re-baselines a poisoned deployment, because a re-baseline would silently
# forgive whatever flow we failed to measure.
REASON_SHARED_WALLET = "shared_wallet_unattributable"
REASON_CHAIN_UNSCANNABLE = "chain_unscannable"
REASON_SCAN_GAP = "scan_gap_unmeasurable"
REASON_UNPRICEABLE_FLOW = "unpriceable_flow"
REASON_UNCLASSIFIED_MATERIAL = "unclassified_flows_material"
REASON_PENDING_OVERFLOW = "pending_overflow"

#: Upper bound on deferred unclassified transfers carried in the record.
#: Reached only by a wallet under a genuine transfer storm; overflowing is
#: poisoned rather than truncated, because dropping deferred entries would
#: silently forgive flows that were never explained.
MAX_PENDING_UNCLASSIFIED = 256

#: Non-transient detail stamped onto the snapshot copy of the record only
#: (never onto the durable cumulative), so an operator can tell "we skipped a
#: cycle" from "we poisoned the era".
DETAIL_SCAN_DEFERRED = "scan_deferred"
DETAIL_NO_GATEWAY = "no_gateway"

#: Materiality gate (VIB-5866). Unclassified flows below this magnitude are
#: accumulated forensically and do not poison the era; above it we cannot
#: claim the deposits/withdrawals split is right.
MATERIALITY_FLOOR_USD = Decimal("1")
MATERIALITY_NAV_FRACTION = Decimal("0.001")

#: Canonical local ``deployment_id`` shape (``resolve_deployment_id``,
#: blueprint 29 §2). Anything else — a bare strategy name, a hosted platform
#: id — cannot be proven to own the wallet it is about to attribute
#: transfers to (VIB-4927 boundary), so it is refused rather than guessed.
_CANONICAL_DEPLOYMENT_ID = re.compile(r"^deployment:[0-9a-f]{12}$")

_ZERO = Decimal("0")


def is_canonical_deployment_id(deployment_id: str | None) -> bool:
    """True when ``deployment_id`` is the canonical ``deployment:<12 hex>`` form."""
    return bool(deployment_id) and bool(_CANONICAL_DEPLOYMENT_ID.match(str(deployment_id)))


def materiality_threshold(nav_usd: Decimal) -> Decimal:
    """Largest unclassified magnitude an interval may carry and stay measured."""
    return max(MATERIALITY_FLOOR_USD, nav_usd * MATERIALITY_NAV_FRACTION)


@dataclass(frozen=True)
class PendingUnclassified:
    """One unclassified transfer awaiting a ledger recheck next cycle.

    A strategy transaction's ledger row carries a timestamp stamped at
    creation but is persisted on its own path, so a snapshot taken in the same
    instant can see the transfer on-chain while the row is still in flight.
    Gating on that first sighting would poison an era over the strategy's own
    trade; deferring one cycle removes the race entirely.
    """

    tx_hash: str
    chain: str
    token_address: str
    direction: str  # "IN" | "OUT"
    block: int
    value_usd: Decimal | None

    def to_record(self) -> dict[str, Any]:
        """JSON-safe form; ``value_usd`` stays ``None`` when unpriceable."""
        return {
            "tx_hash": self.tx_hash,
            "chain": self.chain,
            "token_address": self.token_address,
            "direction": self.direction,
            "block": self.block,
            "value_usd": None if self.value_usd is None else str(self.value_usd),
        }

    @classmethod
    def from_record(cls, raw: Any) -> PendingUnclassified | None:
        """Parse one persisted entry, or ``None`` when it is unreadable."""
        if not isinstance(raw, Mapping):
            return None
        try:
            return cls(
                tx_hash=str(raw["tx_hash"]),
                chain=str(raw.get("chain", "")),
                token_address=str(raw.get("token_address", "")),
                direction="IN" if str(raw.get("direction")) == "IN" else "OUT",
                block=int(raw.get("block", 0)),
                value_usd=_parse_optional_decimal(raw.get("value_usd")),
            )
        except (KeyError, TypeError, ValueError, InvalidOperation):
            return None


@dataclass(frozen=True)
class CapitalFlowRecord:
    """The whole capital-flow state for one deployment, persisted wholesale."""

    status: str = STATUS_PENDING
    cursors: dict[str, int] = field(default_factory=dict)
    era_start: dict[str, int] = field(default_factory=dict)
    deposits_usd: Decimal | None = _ZERO
    withdrawals_usd: Decimal | None = _ZERO
    unclassified_in_usd: Decimal = _ZERO
    unclassified_out_usd: Decimal = _ZERO
    unmeasured_reason: str | None = None
    #: Unclassified transfers deferred for a ledger recheck next cycle. Rides
    #: the record wholesale, so recovery semantics are unchanged.
    pending_unclassified: tuple[PendingUnclassified, ...] = ()

    @property
    def max_cursor(self) -> int:
        """Highest scanned block across chains; ``-1`` when no era exists yet."""
        return max(self.cursors.values(), default=-1)

    def to_record(self) -> dict[str, Any]:
        """JSON-safe wholesale form (str-decimals, int cursors)."""
        return {
            "schema_version": SCHEMA_VERSION,
            "cursors": dict(self.cursors),
            "era_start": dict(self.era_start),
            "deposits_usd": None if self.deposits_usd is None else str(self.deposits_usd),
            "withdrawals_usd": None if self.withdrawals_usd is None else str(self.withdrawals_usd),
            "unclassified_in_usd": str(self.unclassified_in_usd),
            "unclassified_out_usd": str(self.unclassified_out_usd),
            "status": self.status,
            "unmeasured_reason": self.unmeasured_reason,
            "pending_unclassified": [entry.to_record() for entry in self.pending_unclassified],
        }

    @classmethod
    def from_record(cls, raw: Any) -> CapitalFlowRecord | None:
        """Parse a persisted record, or ``None`` when it is absent / unreadable.

        Unreadable is deliberately *not* an error: a corrupt mirror must lose
        to the other mirror rather than crash the metrics build.
        """
        if not isinstance(raw, Mapping):
            return None
        try:
            if int(raw.get("schema_version", 0)) != SCHEMA_VERSION:
                return None
            return cls(
                status=str(raw.get("status") or STATUS_PENDING),
                cursors=_parse_block_map(raw.get("cursors")),
                era_start=_parse_block_map(raw.get("era_start")),
                deposits_usd=_parse_optional_decimal(raw.get("deposits_usd")),
                withdrawals_usd=_parse_optional_decimal(raw.get("withdrawals_usd")),
                unclassified_in_usd=_parse_optional_decimal(raw.get("unclassified_in_usd")) or _ZERO,
                unclassified_out_usd=_parse_optional_decimal(raw.get("unclassified_out_usd")) or _ZERO,
                unmeasured_reason=(str(raw["unmeasured_reason"]) if raw.get("unmeasured_reason") else None),
                pending_unclassified=_parse_pending(raw.get("pending_unclassified")),
            )
        except (TypeError, ValueError, InvalidOperation):
            return None


def _parse_pending(raw: Any) -> tuple[PendingUnclassified, ...]:
    """Parse deferred entries; unreadable ones are dropped, never guessed."""
    if not isinstance(raw, list | tuple):
        return ()
    parsed = (PendingUnclassified.from_record(entry) for entry in raw)
    return tuple(entry for entry in parsed if entry is not None)


def _parse_block_map(raw: Any) -> dict[str, int]:
    if not isinstance(raw, Mapping):
        return {}
    return {str(chain): int(block) for chain, block in raw.items()}


def _parse_optional_decimal(raw: Any) -> Decimal | None:
    # str() of a Decimal round-trips exactly, so one path serves both the
    # in-memory and the JSON-string shape (VIB-4062: no type bifurcation).
    if raw is None:
        return None
    text = str(raw)
    return None if text == "" else Decimal(text)


# --------------------------------------------------------------------------
# Recovery
# --------------------------------------------------------------------------


def recover_record(candidates: Iterable[Any]) -> CapitalFlowRecord | None:
    """Pick the freshest mirror *wholesale* — the one with the highest cursor.

    Never merges fields across mirrors. After a crash between the two writes,
    one mirror holds ``(cursor=N, cumulative=X)`` and the other
    ``(cursor=N+k, cumulative=X+d)``; both are internally consistent, so
    taking either whole is safe while mixing them would double-count ``d``.
    """
    best: CapitalFlowRecord | None = None
    for raw in candidates:
        parsed = raw if isinstance(raw, CapitalFlowRecord) else CapitalFlowRecord.from_record(raw)
        if parsed is None:
            continue
        if best is None or parsed.max_cursor > best.max_cursor:
            best = parsed
    return best


# --------------------------------------------------------------------------
# Transitions
# --------------------------------------------------------------------------


def pending_record() -> CapitalFlowRecord:
    """Era not started: the anchor transaction does not exist yet."""
    return CapitalFlowRecord(status=STATUS_PENDING)


def start_era(
    record: CapitalFlowRecord | None,
    *,
    cursors: Mapping[str, int],
    deposits_usd: Decimal,
    withdrawals_usd: Decimal,
) -> CapitalFlowRecord:
    """Open a measured era at ``cursors`` without booking anything historical.

    ``cursors`` doubles as ``era_start`` — the scan range is ``(cursor, head]``,
    so nothing at or before the anchor block can ever be booked.
    """
    base = record or pending_record()
    blocks = {str(chain): int(block) for chain, block in cursors.items()}
    return replace(
        base,
        status=STATUS_MEASURED,
        cursors=dict(blocks),
        era_start=dict(blocks),
        deposits_usd=deposits_usd,
        withdrawals_usd=withdrawals_usd,
        unmeasured_reason=None,
    )


def poison(record: CapitalFlowRecord | None, reason: str) -> CapitalFlowRecord:
    """Mark the era unmeasured; the columns project to ``None`` from here on."""
    base = record or pending_record()
    return replace(
        base,
        status=STATUS_UNMEASURED,
        unmeasured_reason=reason,
        deposits_usd=None,
        withdrawals_usd=None,
    )


def project_columns(record: CapitalFlowRecord) -> tuple[Decimal | None, Decimal | None]:
    """Metrics columns are a pure projection of the record — never a source."""
    if record.status != STATUS_MEASURED:
        return (None, None)
    return (record.deposits_usd, record.withdrawals_usd)


# --------------------------------------------------------------------------
# Interval algebra
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class IntervalSummary:
    """Valued totals for one scan interval, across every chain.

    Deposits and withdrawals are final — an EOA counterparty can never be a
    strategy transaction, so there is nothing to wait for. Unclassified
    transfers are *deferred* instead of gated (see :class:`PendingUnclassified`).
    """

    deposits_usd: Decimal = _ZERO
    withdrawals_usd: Decimal = _ZERO
    has_unpriceable_flow: bool = False
    pending: tuple[PendingUnclassified, ...] = ()


@dataclass(frozen=True)
class PendingTally:
    """Verdict material for pending entries that survived the ledger recheck."""

    in_usd: Decimal = _ZERO
    out_usd: Decimal = _ZERO
    has_unpriceable: bool = False
    tx_hashes: tuple[str, ...] = ()

    @property
    def magnitude(self) -> Decimal:
        """Σ|unclassified| — what the materiality gate is measured against."""
        return self.in_usd + self.out_usd

    def breaches(self, nav_usd: Decimal) -> bool:
        """True when this tally cannot be dismissed as immaterial noise."""
        return self.has_unpriceable or self.magnitude > materiality_threshold(nav_usd)


PriceLookup = Callable[[str, str], Decimal | None]


def _value_observation(obs: TransferObservation, price_of: PriceLookup) -> Decimal | None:
    """USD value of one transfer, or ``None`` when it cannot be valued."""
    if obs.amount is None:
        return None
    price = price_of(obs.chain, obs.token_address)
    return None if price is None else obs.amount * price


def summarize_interval(observations: Sequence[TransferObservation], price_of: PriceLookup) -> IntervalSummary:
    """Value classified transfers; a flow we can see but cannot value poisons.

    ``STRATEGY_TX`` observations are the strategy's own trades and are dropped
    outright. Everything else is valued at the *current* snapshot price; the
    error that introduces is bounded by one iteration of drift, whereas
    guessing a missing price would fabricate PnL.

    Unclassified observations are returned as ``pending`` rather than summed:
    at scan time we cannot yet distinguish "external contract flow" from "our
    own transaction whose ledger row is still in flight".
    """
    deposits = withdrawals = _ZERO
    unpriceable_flow = False
    pending: list[PendingUnclassified] = []

    for obs in observations:
        value = _value_observation(obs, price_of)

        if obs.classification is FlowClassification.DEPOSIT:
            unpriceable_flow |= value is None
            deposits += value or _ZERO
        elif obs.classification is FlowClassification.WITHDRAWAL:
            unpriceable_flow |= value is None
            withdrawals += value or _ZERO
        elif obs.classification in (FlowClassification.UNCLASSIFIED_IN, FlowClassification.UNCLASSIFIED_OUT):
            pending.append(
                PendingUnclassified(
                    tx_hash=obs.tx_hash,
                    chain=obs.chain,
                    token_address=obs.token_address,
                    direction=("IN" if obs.classification is FlowClassification.UNCLASSIFIED_IN else "OUT"),
                    block=obs.block_number,
                    value_usd=value,
                )
            )

    return IntervalSummary(
        deposits_usd=deposits,
        withdrawals_usd=withdrawals,
        has_unpriceable_flow=unpriceable_flow,
        pending=tuple(pending),
    )


def unmatched_pending(
    pending: Sequence[PendingUnclassified],
    ledger_tx_hashes: Iterable[str],
) -> tuple[PendingUnclassified, ...]:
    """Drop deferred entries whose tx is now visible in the ledger.

    This is the whole point of deferring: a strategy transaction's ledger row
    is stamped at creation but persisted separately, so a snapshot captured in
    the same instant can scan the tx on-chain while its row is still in
    flight. One cycle later the row is durably visible and the transfer is
    provably ours — silently dropped, never gated.
    """
    ledger = {str(h).strip().lower() for h in ledger_tx_hashes if h}
    return tuple(entry for entry in pending if entry.tx_hash.strip().lower() not in ledger)


def tally_pending(entries: Sequence[PendingUnclassified]) -> PendingTally:
    """Sum surviving entries into the material the materiality gate judges."""
    in_usd = out_usd = _ZERO
    unpriceable = False
    for entry in entries:
        if entry.value_usd is None:
            # An unclassified transfer we cannot even size could be
            # arbitrarily large, so it is treated as breaching the gate.
            unpriceable = True
        elif entry.direction == "IN":
            in_usd += abs(entry.value_usd)
        else:
            out_usd += abs(entry.value_usd)
    return PendingTally(
        in_usd=in_usd,
        out_usd=out_usd,
        has_unpriceable=unpriceable,
        tx_hashes=tuple(dict.fromkeys(entry.tx_hash for entry in entries)),
    )


def apply_interval(
    record: CapitalFlowRecord,
    summary: IntervalSummary,
    *,
    nav_usd: Decimal,
    cursors: Mapping[str, int],
    resolved: PendingTally,
) -> CapitalFlowRecord:
    """Fold one scanned interval into the record (cursor + cumulative together).

    ``resolved`` is the verdict on entries deferred by *earlier* cycles that
    the ledger still cannot explain; this interval's own unclassified
    transfers go into ``pending_unclassified`` and are judged next cycle.

    Poisoning precedence: an unpriceable deposit/withdrawal first (we *know*
    capital moved and cannot size it), then the materiality gate, then the
    deferral bound. All are permanent for the era. When the gate fires the
    forensic accumulators are credited with the poisoning sums, so the record
    always says how much unexplained flow caused it.
    """
    advanced = dict(record.cursors)
    advanced.update({str(chain): int(block) for chain, block in cursors.items()})

    forensic_in = record.unclassified_in_usd + resolved.in_usd
    forensic_out = record.unclassified_out_usd + resolved.out_usd

    if summary.has_unpriceable_flow:
        # Same forensics + pending contract as the materiality poison below:
        # the record must say what unexplained flow it saw, and a poisoned
        # (sticky) era never re-judges pending, so retaining entries would
        # only bloat both mirrors forever.
        return replace(
            poison(record, REASON_UNPRICEABLE_FLOW),
            cursors=advanced,
            unclassified_in_usd=forensic_in,
            unclassified_out_usd=forensic_out,
            pending_unclassified=(),
        )

    if resolved.breaches(nav_usd):
        return replace(
            poison(record, REASON_UNCLASSIFIED_MATERIAL),
            cursors=advanced,
            unclassified_in_usd=forensic_in,
            unclassified_out_usd=forensic_out,
            pending_unclassified=(),
        )

    # Every entry deferred by an earlier cycle has now been judged — matched
    # ones dropped as ours, unmatched ones folded into ``resolved`` above — so
    # only this interval's own deferrals carry forward. Retaining the old ones
    # would re-judge them every cycle and double-count their forensics.
    pending = summary.pending
    if len(pending) > MAX_PENDING_UNCLASSIFIED:
        # Refusing to grow the record without bound is honest; silently
        # dropping deferred entries would forgive flows we never explained.
        return replace(
            poison(record, REASON_PENDING_OVERFLOW),
            cursors=advanced,
            unclassified_in_usd=forensic_in,
            unclassified_out_usd=forensic_out,
            pending_unclassified=(),
        )

    return replace(
        record,
        cursors=advanced,
        deposits_usd=(record.deposits_usd or _ZERO) + summary.deposits_usd,
        withdrawals_usd=(record.withdrawals_usd or _ZERO) + summary.withdrawals_usd,
        unclassified_in_usd=forensic_in,
        unclassified_out_usd=forensic_out,
        pending_unclassified=pending,
    )
