"""Senior-Quant header aggregations for the strategy detail dashboard.

Pure read-side aggregations over the accounting tables that the SDK
already writes (transaction_ledger, accounting_events, position_events,
portfolio_snapshots, portfolio_metrics). No new schema, no writes —
every value here is derivable from data on disk.

The four blocks of the header card:

1. **Money trail** (G1, G4, G5) — initial deployed capital, current NAV,
   lifetime PnL %, annualised return, drawdown.
2. **Cost stack** (G2, G3, P3, P5) — life-to-date gas, protocol fees,
   slippage, fees earned, interest, funding, realized PnL, IL.
3. **Reconciliation (G6)** — wallet PnL ≡ Σ component PnL within ε.
4. **Audit-trail completeness** (G9, G12, G13) — fraction of rows with
   price_inputs / pre+post state / version stamps.

Plus an Accountant-Test posture rollup (which cells PASS / FAIL / XFAIL
on this strategy's primitive). The posture evaluator here is intentionally
*lighter-weight* than the full pytest harness in
``almanak.framework.accounting.accountant_test`` — it answers ``can the
data on disk *support* this cell``, not ``does the cell pass with epsilon
ε``. That's the distinction between an at-a-glance dashboard chip and a
pre-merge audit gate.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from almanak.framework.observability.ledger import LedgerQuantStats, lenient_ledger_decimal
from almanak.framework.portfolio.models import STRATEGY_REPORTED_VALUATION_SOURCE, ValueConfidence
from almanak.framework.valuation.net_debt import (
    net_debt_from_positions_json,
    net_debt_from_snapshot,
    parse_positions_payload,
    read_position_decimal,
    wallet_nav_usd,
)

logger = logging.getLogger(__name__)

# VIB-5339: a post-teardown residual worth a fraction of a dollar (a sub-floor
# dust leg, or a fully-closed position marked at ~0) is not an "open position" —
# displaying "1 open position(s)" for it is a display lie on an otherwise-flat
# wallet. The DISPLAY count excludes legs whose |value_usd| is unmeasured or
# at/below this dust floor. This is presentation-only: it changes neither the
# debt-netting money math (``net_debt_from_snapshot`` still owns ``debt_mark`` /
# ``net_cost``) nor the cash-vs-deployed classification.
#
# VIB-5738 update: the write side is now authoritative. The PortfolioValuer
# classifies a sub-floor, non-directional swap-inventory residual as wallet cash
# (``dust_residual``) and de-duplicates a holding that surfaces as both a
# discovered pseudo-position and a swap-inventory row, so a typed snapshot
# produced by the current valuer no longer carries a sub-floor swap-dust leg or a
# double-counted holding for this filter to remove. This threshold is therefore
# now a defensive BACKSTOP — retained (not removed) to keep protecting dict /
# legacy / historical snapshots (which never pass through the valuer fix) and any
# non-swap sub-floor leg — deliberately NOT the primary fix. It is intentionally
# coarser than and independent of the valuer's $5 classification floor: a genuine
# declared-``base_token`` holding worth <$1 stays a deployed position in the money
# math (``total_value_usd``) even while this badge rounds it out of the count.
_OPEN_POSITION_DUST_USD = Decimal("1")


def _count_open_positions(snapshot: Any) -> int | None:
    """Number of position legs worth more than dust on ``snapshot``.

    Returns ``None`` when the snapshot exposes no typed ``positions`` list (dict
    / legacy shapes), so the caller keeps the ``net_debt_from_snapshot`` count
    unchanged rather than guessing. A leg with an unmeasured (``None``)
    ``value_usd`` is NOT counted (Empty ≠ Zero — an unmeasured leg must not
    inflate the badge), and a leg at/below :data:`_OPEN_POSITION_DUST_USD` is
    dust, not an open position.
    """
    positions = getattr(snapshot, "positions", None)
    if positions is None or isinstance(snapshot, dict):
        return None
    count = 0
    for pos in positions:
        value = read_position_decimal(pos, "value_usd")
        if value is not None and abs(value) > _OPEN_POSITION_DUST_USD:
            count += 1
    return count


# ε threshold for G6 reconciliation. The Accountant Test uses ε = $0.50
# on real mainnet runs; the dashboard mirrors that so both surfaces
# agree on PASS/FAIL.
_G6_EPSILON_USD = Decimal("0.50")

# All Accountant Test cell IDs. Source: docs/internal/blueprints/27-accounting.md §14.
_GENERIC_CELLS = (
    "G1",
    "G2",
    "G3",
    "G4",
    "G5",
    "G6",
    "G7",
    "G8",
    "G9",
    "G10",
    "G11",
    "G12",
    "G13",
    "G14",
    "G15",
)
_LP_CELLS = ("LP1", "LP2", "LP3", "LP4", "LP5", "LP6")
_LENDING_CELLS = ("L1", "L2", "L3", "L4", "L5", "L6")
_PERP_CELLS = ("P1", "P2", "P3", "P4", "P5", "P6")

# Cells that require Track C (position_state_snapshots). Local SQLite has a
# runner caller, but hosted mode still short-circuits until the metrics-database
# table and hosted caller are available. The dashboard posture is a lightweight
# capability summary, so Track-C-dependent cells remain XFAIL when those rows
# are absent from the strategy's data.
_TRACK_C_DEPENDENT = frozenset(
    {
        "G14",
        "G15",
        "LP2",
        "LP4",
        "LP5",
        "LP6",
        "L1",
        "L2",
        "L3",
        "L5",
        "L6",
        "P2",
        "P4",
        "P5",
        "P6",
    }
)


def _to_decimal(value: Any, default: str = "0") -> Decimal:
    """Best-effort Decimal parse; never raises."""
    if value is None or value == "":
        return Decimal(default)
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)


def _safe_payload_loads(raw: Any) -> dict[str, Any]:
    """Decode an accounting event's ``payload_json`` defensively.

    The aggregator runs over arbitrary on-disk events; a single malformed
    payload must not crash the full posture/cost-stack evaluation.
    Returns an empty dict on any decode failure or non-dict result.
    """
    if not raw:
        return {}
    try:
        out = json.loads(raw)
    except (json.JSONDecodeError, TypeError, ValueError):
        return {}
    return out if isinstance(out, dict) else {}


def _payload_decimal(payload: dict[str, Any], *keys: str, default: str = "0") -> Decimal:
    """Pull the first non-empty value across `keys` from a payload dict.

    Lending/LP/perp payloads carry both legacy and spec-name keys for the
    same value (``principal_repaid_usd`` / ``principal_delta_usd``); this
    walks them in caller-provided priority order.
    """
    for key in keys:
        v = payload.get(key)
        if v is not None and v != "":
            return _to_decimal(v, default)
    return Decimal(default)


def _payload_decimal_or_none(payload: dict[str, Any], *keys: str) -> Decimal | None:
    """Presence-aware twin of :func:`_payload_decimal` (Empty≠Zero, VIB-5942).

    Returns the first present-and-**parseable** value across ``keys`` as a finite
    Decimal, or ``None`` when none of the keys yields one — so the caller can tell
    a MEASURED zero (``"0"`` on a key) from an UNMEASURED bucket (no key at all,
    e.g. a perp fee the receipt parser has not populated yet).

    A present-but-**malformed / non-finite** value (``"N/A"``, ``"NaN"``,
    ``"Infinity"``, a corrupt legacy row) is treated as UNMEASURED (``None``), NOT
    coerced to a measured ``Decimal("0")`` — VIB-5942 audit fix: the earlier
    ``_to_decimal`` fallback marked corrupt rows as a confident measured zero,
    exactly the fabrication this presence-aware path exists to prevent. A malformed
    value does not short-circuit the alternate-key walk, so a valid alternate still
    wins (legacy vs spec-name keys).
    """
    for key in keys:
        v = payload.get(key)
        if v is not None and v != "":
            parsed = _to_decimal_or_none(v)
            if parsed is not None:
                return parsed
    return None


# ---------------------------------------------------------------------------
# Cost stack + reconciliation components
# ---------------------------------------------------------------------------


@dataclass
class CostStack:
    """Life-to-date cost / yield decomposition over accounting events."""

    gas_usd: Decimal = Decimal("0")
    protocol_fees_usd: Decimal = Decimal("0")
    slippage_usd: Decimal = Decimal("0")
    # VIB-5942 (Empty≠Zero): the fee / slippage buckets aggregate to Decimal("0")
    # BOTH when a measured zero was recorded AND when NO contributing event carried
    # the term (e.g. a GMX perp whose receipt parser has not populated open/close
    # fees yet — the VIB-5941 payload gap). These flags carry that distinction so
    # the cost-stack tile can render "$0.00" (measured) vs "—" (unmeasured) instead
    # of a fabricated "$0.00" for an unmeasured cost. Gas is always measured (every
    # tx pays it), so it needs no flag.
    protocol_fees_measured: bool = False
    slippage_measured: bool = False
    fees_earned_usd: Decimal = Decimal("0")  # LP fees collected
    interest_paid_usd: Decimal = Decimal("0")
    interest_earned_usd: Decimal = Decimal("0")
    funding_paid_usd: Decimal = Decimal("0")
    funding_earned_usd: Decimal = Decimal("0")
    realized_pnl_usd: Decimal = Decimal("0")
    il_usd: Decimal = Decimal("0")  # diagnostic (not in net PnL)
    # VIB-6283 (Empty≠Zero): the LP earn / IL buckets have exactly the same
    # measured-vs-unmeasured ambiguity the fee / slippage buckets above got
    # flags for in VIB-5942, and for a sharper reason — ``lp_accounting``
    # already emits ``fees_total_usd = None`` / ``il_usd = None`` when the
    # quantity is unmeasurable (bundled-fee close it could not attribute, an
    # N-coin pool with no two-token HODL anchor), and the LP fold used to
    # flatten that ``None`` into a confident ``Decimal("0")`` via
    # ``_payload_decimal``'s "0" default. 18 of 66 LP_CLOSE rows in the
    # 2026-06/07 run corpus carry no ``fees_total_usd`` at all, so that
    # fabrication was reaching real dashboards. A bucket is measured only when
    # every applicable LP event contributed its term.
    fees_earned_measured: bool = False
    # Whether the bucket APPLIES at all (>= 1 LP close / collect), and whether
    # ANY applicable event actually contributed a term. See the three-state
    # comment in ``compute_cost_stack``: applicable drives the caveat,
    # any_measured drives whether a number is carried at all.
    fees_earned_applicable: bool = False
    il_applicable: bool = False
    fees_earned_any_measured: bool = False
    il_any_measured: bool = False
    il_measured: bool = False
    # VIB-4984: mark-to-market of held directional swap inventory (e.g. RSI
    # net-long WETH). None = unmeasured (Empty≠Zero), NOT Decimal("0").
    # Computed separately by compute_inventory_unrealized (needs token prices)
    # and stamped by the GetCostStack producer — NOT by compute_cost_stack.
    inventory_unrealized_usd: Decimal | None = None


@dataclass
class ReconciliationStatus:
    """G6: wallet PnL ≡ Σ component PnL within ε."""

    wallet_pnl_usd: Decimal = Decimal("0")
    component_pnl_usd: Decimal = Decimal("0")
    gap_usd: Decimal = Decimal("0")
    epsilon_usd: Decimal = _G6_EPSILON_USD
    passed: bool = False
    has_data: bool = False
    sum_swap: Decimal = Decimal("0")
    sum_lp: Decimal = Decimal("0")
    sum_perp: Decimal = Decimal("0")
    sum_fees: Decimal = Decimal("0")
    sum_funding: Decimal = Decimal("0")
    sum_interest: Decimal = Decimal("0")
    sum_gas: Decimal = Decimal("0")
    # Ambient inventory revaluation (blueprint 27 §11.5) — the same additive
    # component term the Accountant Test G6 cell folds in, so the dashboard G6
    # and the harness G6 stay byte-identical on the same DB.
    sum_inventory_reval: Decimal = Decimal("0")
    # True when the inventory-revaluation term was UNMEASURED (a held token with
    # no mark, or an open lot with no basis). Empty≠Zero: the term is left out
    # of ``component_pnl_usd`` and the surface degrades rather than silently
    # folding in a zero.
    has_unmeasured: bool = False


@dataclass
class AuditTrailStats:
    """Counts that drive the audit-trail completeness tile."""

    ledger_total: int = 0
    ledger_with_price_inputs: int = 0
    ledger_with_pre_post_state: int = 0
    ledger_with_gas_usd: int = 0
    events_total: int = 0
    events_with_versions: int = 0


@dataclass
class AccountantPosture:
    """Cell-by-cell PASS/FAIL/XFAIL count for the strategy's primitive."""

    primitive: str = "mixed"
    cells_total: int = 21
    cells_passed: int = 0
    cells_failed: int = 0
    cells_xfail: int = 0
    failing: list[str] = field(default_factory=list)
    xfail: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Wallet-anchored "Deployed" — single source of truth (VIB-3914)
#
# Pre-VIB-3914 the dashboard derived ``deployed_usd`` from
# ``portfolio_metrics.initial_value_usd``, which is seeded from the
# strategy config knob (``total_value_usd``). On a contaminated wallet
# (pre-existing ERC-20s from prior runs) or a RESUMED strategy the knob
# diverges from reality; the May 3 2026 mainnet AccountingQuantLP run
# showed +381% PnL on a 2-action strategy with this exact failure mode.
#
# The fix: anchor "Deployed" to the wallet snapshot the strategy itself
# captured (``transaction_ledger.pre_state_json`` × ``price_inputs_json``)
# at the moment of its first intent. Same definition as NAV (broker-
# statement style). PnL = wallet delta. Reconciles with G6 by construction.
# ---------------------------------------------------------------------------


def _wallet_value_at_first_action(ledger_entries: list[Any]) -> Decimal | None:
    """Total USD value of the wallet at the moment of the strategy's first intent.

    Reads ``pre_state_json.wallet_balances`` (token → balance) and
    ``price_inputs_json`` (token → {price_usd, …}) of the earliest ledger
    row by timestamp, regardless of ``success`` — pre-state is captured at
    attempt time and is the right anchor even for failed first attempts.

    Returns ``None`` when no ledger row carries both fields, signalling the
    caller to fall back to ``portfolio_metrics`` (e.g., a strategy that
    has not yet executed).
    """
    if not ledger_entries:
        return None

    def _ts(entry: Any) -> Any:
        ts = getattr(entry, "timestamp", None)
        if ts is None and isinstance(entry, dict):
            ts = entry.get("timestamp")
        return ts

    sorted_entries = sorted(ledger_entries, key=lambda e: _ts(e) or datetime.min.replace(tzinfo=UTC))

    for entry in sorted_entries:
        pre_raw = getattr(entry, "pre_state_json", None)
        prices_raw = getattr(entry, "price_inputs_json", None)
        if pre_raw is None and isinstance(entry, dict):
            pre_raw = entry.get("pre_state_json")
            prices_raw = entry.get("price_inputs_json")
        if not pre_raw or not prices_raw:
            continue
        try:
            pre = json.loads(pre_raw)
            prices = json.loads(prices_raw)
        except (json.JSONDecodeError, TypeError):
            continue
        balances = pre.get("wallet_balances") if isinstance(pre, dict) else None
        if not isinstance(balances, dict) or not isinstance(prices, dict):
            continue

        total = Decimal("0")
        for token, balance_raw in balances.items():
            balance = _to_decimal(balance_raw)
            if balance == 0:
                continue
            price_entry = prices.get(token)
            if not isinstance(price_entry, dict):
                continue
            price = _to_decimal(price_entry.get("price_usd"))
            if price == 0:
                continue
            total += balance * price
        if total > 0:
            return total
    return None


def ledger_quant_stats_from_entries(ledger_entries: list[Any]) -> LedgerQuantStats:
    """Python reference aggregation over full ledger rows (VIB-5059 Phase 1).

    Produces the same :class:`LedgerQuantStats` the state stores compute with
    targeted SQL aggregates, from an in-memory list of LedgerEntry-shaped
    objects or dicts. Field semantics mirror the legacy per-row loops this
    module ran before the SQL path existed (non-empty-column counts, the
    ``gas_usd`` sum, the VIB-3914 first-action anchor walk) — the equivalence
    suite pins both implementations against each other on the same data.

    Used by callers that already hold full rows (the legacy
    ``build_quant_header`` composer and its regression tests); the gateway
    hot path receives a store-computed ``LedgerQuantStats`` instead and never
    materialises the row list.
    """
    total = len(ledger_entries)
    with_tx_hash = 0
    with_cycle_id = 0
    with_price_inputs = 0
    with_pre_post_state = 0
    with_positive_gas_usd = 0
    gas_usd_sum = Decimal("0")

    for entry in ledger_entries:
        if isinstance(entry, dict):
            tx_hash = entry.get("tx_hash")
            cycle_id = entry.get("cycle_id")
            price_inputs = entry.get("price_inputs_json")
            pre_state = entry.get("pre_state_json")
            post_state = entry.get("post_state_json")
            gas_usd_raw = entry.get("gas_usd")
        else:
            tx_hash = getattr(entry, "tx_hash", None)
            cycle_id = getattr(entry, "cycle_id", None)
            price_inputs = getattr(entry, "price_inputs_json", None)
            pre_state = getattr(entry, "pre_state_json", None)
            post_state = getattr(entry, "post_state_json", None)
            gas_usd_raw = getattr(entry, "gas_usd", None)
        if tx_hash:
            with_tx_hash += 1
        if cycle_id:
            with_cycle_id += 1
        if price_inputs:
            with_price_inputs += 1
        if pre_state and post_state:
            with_pre_post_state += 1
        gas = lenient_ledger_decimal(gas_usd_raw)
        gas_usd_sum += gas
        if gas_usd_raw and gas > Decimal("0"):
            with_positive_gas_usd += 1

    return LedgerQuantStats(
        total=total,
        with_tx_hash=with_tx_hash,
        with_cycle_id=with_cycle_id,
        with_price_inputs=with_price_inputs,
        with_pre_post_state=with_pre_post_state,
        with_positive_gas_usd=with_positive_gas_usd,
        gas_usd_sum=gas_usd_sum,
        first_action_wallet_value_usd=_wallet_value_at_first_action(ledger_entries),
    )


def _ledger_stats(ledger: list[Any] | LedgerQuantStats) -> LedgerQuantStats:
    """Normalise the ledger input every aggregation consumes (VIB-5059).

    The gateway hot path passes a store-computed :class:`LedgerQuantStats`
    (O(1)-row SQL aggregates); legacy callers and the regression suites pass
    full row lists, which reduce through the reference aggregation above.
    Both shapes flow through ONE downstream code path, so list-path and
    SQL-path consumers cannot drift.
    """
    if isinstance(ledger, LedgerQuantStats):
        return ledger
    return ledger_quant_stats_from_entries(ledger)


def _open_position_cost_basis(accounting_events: list[dict[str, Any]]) -> Decimal:
    """Σ cost_basis_usd over positions whose OPEN event has no matching CLOSE.

    Matches LP_OPEN ↔ LP_CLOSE on ``position_key`` and
    SUPPLY ↔ WITHDRAW / PERP_OPEN ↔ PERP_CLOSE on ``position_key`` when
    available. Falls through to a permissive count when the writer has
    not stamped position_key (no matching possible — the open contributes).

    Used as a fallback for ``deployed_capital_usd`` when the snapshot
    writer has emitted ``0`` despite open positions in
    ``accounting_events`` — the live failure mode VIB-3883/VIB-3894 were
    meant to fix but did not in production.
    """
    open_basis: dict[str, Decimal] = {}
    closed_keys: set[str] = set()
    no_key_basis = Decimal("0")

    open_types = {"LP_OPEN", "SUPPLY", "PERP_OPEN"}
    close_types = {"LP_CLOSE", "WITHDRAW", "PERP_CLOSE"}

    for event in accounting_events:
        payload = _safe_payload_loads(event.get("payload_json") if isinstance(event, dict) else None)
        if not payload:
            continue
        event_type = (payload.get("event_type") or "").upper()
        position_key = payload.get("position_key") or ""

        if event_type in open_types:
            cost = _payload_decimal(payload, "cost_basis_usd")
            if cost == 0:
                continue
            if position_key:
                open_basis[position_key] = open_basis.get(position_key, Decimal("0")) + cost
            else:
                no_key_basis += cost
        elif event_type in close_types and position_key:
            closed_keys.add(position_key)

    still_open = sum(
        (basis for key, basis in open_basis.items() if key not in closed_keys),
        Decimal("0"),
    )
    return still_open + no_key_basis


# ---------------------------------------------------------------------------
# Public entries: compute_pnl_summary, compute_cost_stack (below),
# compute_reconciliation (below), compute_audit_trail (below),
# evaluate_posture (below), build_quant_header (composer; deprecated)
# ---------------------------------------------------------------------------


@dataclass
class PnLSummary:
    """5-second-eyeball card: wallet money trail + cash + primary-risk gauge.

    The decomposed slice of ``QuantHeader`` that powers
    ``GetPnLSummary`` and ``render_pnl_section`` (VIB-3969). Excludes
    cost-stack, reconciliation, audit-trail, and Accountant-Test
    posture — those have their own focused builders / RPCs so a PnL
    consumer never pays the cost of computing G6 + the 21-cell matrix.
    """

    # Money trail (G1, G4, G5)
    deployed_usd: Decimal = Decimal("0")
    nav_usd: Decimal = Decimal("0")
    # VIB-5866: ``None`` = SUPPRESSED because the capital flows behind the
    # ``deployed_usd`` baseline are unmeasured (Empty≠Zero, blueprint 27
    # §10.10) — never a measured zero. Same presence-aware "" wire encoding as
    # ``CostStack.inventory_unrealized_usd`` (VIB-4984).
    lifetime_pnl_usd: Decimal | None = Decimal("0")
    lifetime_pnl_pct: Decimal | None = Decimal("0")
    net_apr_pct: Decimal | None = Decimal("0")
    max_drawdown_pct: Decimal = Decimal("0")
    current_drawdown_pct: Decimal = Decimal("0")
    value_confidence: ValueConfidence | None = None
    age_days: int = 0
    # VIB-6283: fractional elapsed days — the annualisation denominator.
    # ``age_days`` above is the whole-day DISPLAY label.
    age_days_exact: Decimal = Decimal("0")
    # VIB-5866: True when ``deposits_usd`` / ``withdrawals_usd`` could not be
    # measured, so the three metrics above are suppressed. Diagnostic for local
    # renderers; the gateway wire signal is the empty string on those fields.
    capital_flows_unmeasured: bool = False
    # Position + cash
    deployed_capital_usd: Decimal = Decimal("0")
    available_cash_usd: Decimal = Decimal("0")
    open_position_count: int = 0

    # Primary-risk gauge — primitive-aware tile rendered next to the
    # money trail. Kept on PnLSummary so the operator console assembles
    # its full eyeball row from one fetch (LP range / lending HF /
    # perp leverage, depending on what's open).
    # VIB-3925 — honest empty-state copy. "Positions N/A" reads as broken;
    # "No active positions" reads as honest.
    primary_risk_label: str = "No active positions"
    primary_risk_value: str = ""
    primary_risk_color: str = "neutral"
    primary_risk_kind: str = "none"

    # VIB-6308: True when a leg counted into open-position NAV contributed NO
    # cost basis to the netted basis, so ``NAV - cost`` differences two sides
    # with mismatched coverage and books the unbacked mark as profit. ``False``
    # is the harmless default: it means "no known gap", which is also what a
    # pre-VIB-6308 snapshot (no coverage stamp) yields -- those keep today's
    # behaviour rather than being retro-labelled partial on no evidence.
    #
    # APPENDED, never inserted — same reason as the client-side twin in
    # ``dashboard/gateway_client.py``: this is a plain dataclass, so a
    # positional caller would have every later field silently rebound by an
    # inserted default. New fields go last.
    cost_basis_partial: bool = False

    def __post_init__(self) -> None:
        """Canonicalize direct/legacy construction to the typed vocabulary."""
        self.value_confidence = ValueConfidence.parse_optional(
            self.value_confidence,
            field_name="PnLSummary.value_confidence",
        )


@dataclass
class QuantHeader:
    """DEPRECATED (VIB-3969): legacy bundle of PnL + Cost + Audit slices.

    Kept as a server-side composer for one release while the operator
    console migrates to ``GetPnLSummary`` / ``GetCostStack`` /
    ``GetAuditPosture``. New consumers should depend on the focused
    sub-types directly.
    """

    deployed_usd: Decimal = Decimal("0")
    nav_usd: Decimal = Decimal("0")
    # VIB-5866: mirrors PnLSummary — ``None`` = suppressed (unmeasured flows).
    lifetime_pnl_usd: Decimal | None = Decimal("0")
    lifetime_pnl_pct: Decimal | None = Decimal("0")
    net_apr_pct: Decimal | None = Decimal("0")
    max_drawdown_pct: Decimal = Decimal("0")
    current_drawdown_pct: Decimal = Decimal("0")
    value_confidence: ValueConfidence | None = None
    age_days: int = 0
    # VIB-6283: fractional elapsed days — the annualisation denominator.
    # ``age_days`` above is the whole-day DISPLAY label.
    age_days_exact: Decimal = Decimal("0")
    capital_flows_unmeasured: bool = False
    deployed_capital_usd: Decimal = Decimal("0")
    available_cash_usd: Decimal = Decimal("0")
    open_position_count: int = 0
    primary_risk_label: str = "No active positions"
    primary_risk_value: str = ""
    primary_risk_color: str = "neutral"
    primary_risk_kind: str = "none"
    cost_stack: CostStack = field(default_factory=CostStack)
    reconciliation: ReconciliationStatus = field(default_factory=ReconciliationStatus)
    audit_trail: AuditTrailStats = field(default_factory=AuditTrailStats)
    posture: AccountantPosture = field(default_factory=AccountantPosture)

    def __post_init__(self) -> None:
        """Canonicalize direct/legacy construction to the typed vocabulary."""
        self.value_confidence = ValueConfidence.parse_optional(
            self.value_confidence,
            field_name="QuantHeader.value_confidence",
        )


def _accumulate_term(stack: CostStack, attr: str, meter: list[int], value: Decimal | None) -> None:
    """Accumulate one APPLICABLE cost term and record coverage (VIB-5942 audit fix).

    ``meter`` is ``[applicable, measured]``. Every call marks one applicable event
    for the bucket (``meter[0] += 1``); a MEASURED term (``value is not None``) is
    summed into ``stack.<attr>`` and counted (``meter[1] += 1``).

    The bucket is measured **only if every applicable event contributed its term**
    — the caller resolves ``meter[1] == meter[0] > 0``. A single applicable event
    missing its term (``value is None``) leaves ``meter[1] < meter[0]`` → the whole
    bucket is UNMEASURED. This kills the "first measured component flips the flag"
    fabrication: a measured ``PERP_OPEN.open_fee_usd`` followed by a ``PERP_CLOSE``
    with the close fee absent yields a partial total that must NOT be presented as
    a confident measured lifetime number (it renders "— unmeasured" instead).
    """
    meter[0] += 1
    if value is not None:
        setattr(stack, attr, getattr(stack, attr) + value)
        meter[1] += 1


def _row_ledger_id(event: Any) -> str:
    """The accounting_events row's ``ledger_entry_id`` column (the submission link)."""
    return str(event.get("ledger_entry_id") or "") if isinstance(event, dict) else ""


def _executed_settlements_by_link(accounting_events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Map submission ledger id → EXECUTED ``PERP_SETTLEMENT`` payload (VIB-3872 WI-4).

    The measured settlement event SUPERSEDES the ESTIMATED ``PERP_OPEN`` /
    ``PERP_CLOSE`` submission event for the same link **component by component** —
    for each economic field the settlement's MEASURED value wins, and a field the
    settlement left unmeasured (``None``) falls back to the estimate's value; folding
    both, or folding a fabricated zero for an unmeasured settlement component, would
    corrupt the number (Empty≠Zero). Only ``EXECUTED`` settlements carry measured
    fill economics; ``CANCELLED`` / ``FROZEN`` / ``NOT_FOUND_UNCORRELATED`` /
    ``UNMEASURED`` do NOT supersede (the submission event stays the estimate). The
    join key is the settlement payload's ``submission_ledger_entry_id`` (== the
    settlement row's ``ledger_entry_id`` column == the submission row's
    ``ledger_entry_id``). Returning the payload (not just the link) is what lets the
    estimate branch read the settlement's per-field measured values.
    """
    by_link: dict[str, dict[str, Any]] = {}
    for event in accounting_events:
        row_type = str(event.get("event_type") or "") if isinstance(event, dict) else ""
        payload = _safe_payload_loads(event.get("payload_json") if isinstance(event, dict) else None)
        event_type = (row_type or payload.get("event_type") or "").upper()
        if event_type != "PERP_SETTLEMENT" or payload.get("settlement_state") != "EXECUTED":
            continue
        link = payload.get("submission_ledger_entry_id") or _row_ledger_id(event)
        if link:
            by_link[str(link)] = payload
    return by_link


def _perp_estimate_links(accounting_events: list[dict[str, Any]]) -> set[str]:
    """Ledger ids of the ESTIMATED ``PERP_OPEN`` / ``PERP_CLOSE`` submission rows.

    A ``PERP_SETTLEMENT`` whose link is in this set has a submission estimate present
    — the estimate branch performs the per-component merge, so the settlement branch
    must NOT fold again (that is the no-double-count property). A settlement whose
    link is absent (an ORPHAN, e.g. the estimate row is outside the window) is folded
    on its own, measured-only.
    """
    links: set[str] = set()
    for event in accounting_events:
        row_type = str(event.get("event_type") or "") if isinstance(event, dict) else ""
        payload = _safe_payload_loads(event.get("payload_json") if isinstance(event, dict) else None)
        event_type = (row_type or payload.get("event_type") or "").upper()
        if event_type in ("PERP_OPEN", "PERP_CLOSE"):
            link = _row_ledger_id(event)
            if link:
                links.add(link)
    return links


def _settlement_fee_usd(payload: dict[str, Any]) -> Decimal | None:
    """Total measured perp trading fee for an EXECUTED settlement = position_fee +
    borrowing_fee. Empty≠Zero: ``None`` when either component is unmeasured."""
    position_fee = _payload_decimal_or_none(payload, "position_fee_usd")
    borrowing_fee = _payload_decimal_or_none(payload, "borrowing_fee_usd")
    if position_fee is None or borrowing_fee is None:
        return None
    return position_fee + borrowing_fee


def _perp_fee_term(settlement: dict[str, Any] | None, estimate: dict[str, Any], estimate_key: str) -> Decimal | None:
    """The leg's trading-fee term with measured-only, settlement-preferred precedence.

    Settlement total fee (position+borrowing) if MEASURED, else the estimate's
    ``estimate_key`` (open_fee_usd / close_fee_usd) if measured, else ``None``. Never
    fabricates a zero for an unmeasured component (Empty≠Zero): when the settlement's
    fee is partially unmeasured (e.g. borrowing ``None``) the estimate's measured
    total stands in rather than a settlement partial that silently drops a component.
    """
    settled = _settlement_fee_usd(settlement) if settlement else None
    if settled is not None:
        return settled
    return _payload_decimal_or_none(estimate, estimate_key)


def _perp_slippage_term(settlement: dict[str, Any] | None, estimate: dict[str, Any]) -> Decimal | None:
    """price_impact term: settlement measured value, else estimate's, else None."""
    settled = _payload_decimal_or_none(settlement, "price_impact_usd") if settlement else None
    if settled is not None:
        return settled
    return _payload_decimal_or_none(estimate, "price_impact_usd")


def _perp_realized_pnl(settlement: dict[str, Any] | None, estimate: dict[str, Any]) -> Decimal:
    """Realized PnL: settlement's MEASURED value if present, else the estimate's.

    The estimate fallback uses the legacy None→0 contract (a plain PERP_CLSE with no
    settlement behaves exactly as before). The point of FIX-1: an EXECUTED settlement
    whose ``realized_pnl_usd`` is ``None`` must NOT replace a measured estimate with a
    fabricated zero — it falls back to the estimate's value instead.
    """
    settled = _payload_decimal_or_none(settlement, "realized_pnl_usd") if settlement else None
    if settled is not None:
        return settled
    return _payload_decimal(estimate, "realized_pnl_usd")


def _apply_signed_funding(stack: CostStack, funding: Decimal) -> None:
    """Split one SIGNED funding fee into paid (≥0, cost) / earned (<0, received)."""
    if funding >= 0:
        stack.funding_paid_usd += funding
    else:
        stack.funding_earned_usd += -funding


def _fold_perp_event(
    stack: CostStack,
    event: Any,
    payload: dict[str, Any],
    event_type: str,
    settlement_by_link: dict[str, dict[str, Any]],
    estimate_links: set[str],
    fee_meter: list[int],
    slip_meter: list[int],
) -> bool:
    """Fold one PERP_OPEN / PERP_CLOSE / PERP_SETTLEMENT event into ``stack``.

    Returns ``True`` iff ``event_type`` was a perp event (handled). A measured
    EXECUTED PERP_SETTLEMENT supersedes the ESTIMATED PERP_OPEN/PERP_CLOSE fold for
    the same submission ledger link **per component** (VIB-3872 WI-4): for each field
    the settlement's measured value wins, an unmeasured settlement field falls back to
    the estimate (never a fabricated zero), and each field is folded from exactly ONE
    source (no double-count). Extracted from ``compute_cost_stack`` to keep it under
    the complexity gate.
    """
    if event_type == "PERP_OPEN":
        s = settlement_by_link.get(_row_ledger_id(event))
        _accumulate_term(stack, "protocol_fees_usd", fee_meter, _perp_fee_term(s, payload, "open_fee_usd"))
        _accumulate_term(stack, "slippage_usd", slip_meter, _perp_slippage_term(s, payload))
        return True
    if event_type == "PERP_CLOSE":
        s = settlement_by_link.get(_row_ledger_id(event))
        _accumulate_term(stack, "protocol_fees_usd", fee_meter, _perp_fee_term(s, payload, "close_fee_usd"))
        _accumulate_term(stack, "slippage_usd", slip_meter, _perp_slippage_term(s, payload))
        stack.realized_pnl_usd += _perp_realized_pnl(s, payload)
        s_funding = _payload_decimal_or_none(s, "funding_fee_usd") if s else None
        if s_funding is not None:
            _apply_signed_funding(stack, s_funding)
        else:
            stack.funding_paid_usd += _payload_decimal(payload, "funding_paid_usd")
            stack.funding_earned_usd += _payload_decimal(payload, "funding_received_usd")
        return True
    if event_type == "PERP_SETTLEMENT":
        # An EXECUTED settlement whose submission estimate is PRESENT was already
        # merged by the estimate branch (per component) — folding here double-counts.
        # Only an ORPHAN settlement (no PERP_OPEN/PERP_CLOSE row for its link) folds
        # here, and then measured-only (Empty≠Zero: an unmeasured field contributes
        # nothing, it is NOT zero).
        if payload.get("settlement_state") != "EXECUTED":
            return True
        link = str(payload.get("submission_ledger_entry_id") or _row_ledger_id(event))
        if link in estimate_links:
            return True
        _accumulate_term(stack, "protocol_fees_usd", fee_meter, _settlement_fee_usd(payload))
        _accumulate_term(stack, "slippage_usd", slip_meter, _payload_decimal_or_none(payload, "price_impact_usd"))
        realized = _payload_decimal_or_none(payload, "realized_pnl_usd")
        if realized is not None:
            stack.realized_pnl_usd += realized
        funding = _payload_decimal_or_none(payload, "funding_fee_usd")
        if funding is not None:
            _apply_signed_funding(stack, funding)
        return True
    return False


def compute_cost_stack(
    ledger_entries: list[Any] | LedgerQuantStats,
    accounting_events: list[dict[str, Any]],
) -> CostStack:
    """Aggregate life-to-date cost / yield buckets.

    ``ledger_entries`` is either a list of LedgerEntry-shaped objects (the
    backend dataclass or a dict) or a pre-aggregated
    :class:`LedgerQuantStats` (VIB-5059 — the gateway hot path pushes the
    gas SUM into the store's SQL so it never materialises the row list).
    ``accounting_events`` is the raw-row output of
    ``backend.get_accounting_events`` — each row's ``payload_json`` is
    decoded here.
    """
    stack = CostStack()
    # [applicable, measured] coverage meters (VIB-5942): a fee/slippage bucket is
    # measured only when EVERY applicable event contributed its term.
    fee_meter = [0, 0]
    slip_meter = [0, 0]
    # VIB-6283: same [applicable, measured] shape for the LP earn / IL buckets.
    earn_meter = [0, 0]
    il_meter = [0, 0]
    # VIB-3872 WI-4: per-component supersession. The estimate branch merges each
    # economic field (settlement-measured-else-estimate); the settlement branch folds
    # only ORPHAN settlements (no submission estimate present). Together: exactly one
    # source per field, no double-count, no fabricated zero (Empty≠Zero).
    settlement_by_link = _executed_settlements_by_link(accounting_events)
    estimate_links = _perp_estimate_links(accounting_events)

    stack.gas_usd += _ledger_stats(ledger_entries).gas_usd_sum

    for event in accounting_events:
        payload = _safe_payload_loads(event.get("payload_json") if isinstance(event, dict) else None)
        # Read ``event_type`` from the row first so events with a valid
        # column-level type but a payload that omits the key (or has
        # malformed JSON) still flow into the right cost-stack bucket.
        # The gateway join already treats ``event_type`` as a first-class
        # column on accounting_events; falling back to payload only when
        # the column is absent matches that contract.
        row_event_type = ""
        if isinstance(event, dict):
            row_event_type = str(event.get("event_type") or "")
        event_type = (row_event_type or payload.get("event_type") or "").upper()
        if not event_type:
            continue

        # SWAP — slippage is a cost; swap-gas already counted on ledger.
        if event_type == "SWAP":
            _accumulate_term(stack, "slippage_usd", slip_meter, _payload_decimal_or_none(payload, "slippage_usd"))
            # VIB-4905 (F1): prefer ``realized_pnl_usd_matched`` (matched-
            # portion PnL, populated on partial matches too) and fall back
            # to legacy ``realized_pnl_usd`` (null on partial matches under
            # the v1 contract).  Pre-v2 payloads on disk only carry the
            # legacy key — the precedence walk handles both.
            stack.realized_pnl_usd += _payload_decimal(payload, "realized_pnl_usd_matched", "realized_pnl_usd")
            _accumulate_term(
                stack, "protocol_fees_usd", fee_meter, _payload_decimal_or_none(payload, "protocol_fee_usd", "fee_usd")
            )
            continue

        # LENDING family
        if event_type in ("SUPPLY", "WITHDRAW"):
            stack.interest_earned_usd += _payload_decimal(payload, "interest_accrued_usd", "interest_delta_usd")
            continue
        if event_type in ("BORROW",):
            # No realized cost on open; accrual lives on REPAY.
            continue
        if event_type in ("REPAY", "DELEVERAGE"):
            stack.interest_paid_usd += _payload_decimal(payload, "interest_paid_usd", "interest_delta_usd")
            continue

        # LP family
        if event_type == "LP_OPEN":
            continue
        if event_type in ("LP_CLOSE", "LP_COLLECT_FEES"):
            # VIB-6283 — two fixes to one branch.
            #
            # (1) ``LP_COLLECT_FEES`` was never folded here. It is a
            #     first-class accounting event (FIFO basis replay at
            #     ``basis.py``'s ``_replay_lp``; emitted by every LP
            #     connector) and it CREDITS the wallet, so a strategy that
            #     harvests fees without closing had real USD income landing
            #     in wallet PnL but never in Σ component PnL — a G6 gap whose
            #     true cause was a missing fold. No double-count risk: a
            #     CLOSE's ``fees_total_usd`` covers only the fees collected in
            #     that close tx, mid-life harvests having already been paid
            #     out on their own ``LP_COLLECT_FEES`` rows.
            #
            # (2) ``_payload_decimal`` defaults a missing / empty key to
            #     ``Decimal("0")``, destroying the unmeasured signal
            #     ``lp_accounting`` deliberately preserves as ``None``. Use
            #     the presence-aware twin + meters, exactly as the SWAP branch
            #     above does for fees / slippage (VIB-5942).
            _accumulate_term(stack, "fees_earned_usd", earn_meter, _payload_decimal_or_none(payload, "fees_total_usd"))
            # realized PnL and IL are CLOSE-ONLY. A fee harvest disposes of no
            # principal, so neither is realised by it — and the producer's
            # number for a COLLECT is actively wrong to fold:
            # ``lp_handler._lp_close_realized_pnl`` runs for the whole
            # ``_LP_CLOSE_LIKE`` set and computes ``cost_basis_usd -
            # open_basis``, where a COLLECT's ``cost_basis_usd`` is only the USD
            # value of the fees collected. On a $1,000 position harvesting $10
            # that emits ``realized_pnl_usd ≈ -990`` — the still-deployed
            # principal booked as a realized loss. Widening this branch to
            # LP_COLLECT_FEES for the fee-income fix above must therefore NOT
            # widen the realized/IL folds with it. (The producer emitting a
            # meaningless realized_pnl_usd on a COLLECT at all is tracked
            # separately; the dashboard refusing to fold it is correct either
            # way.)
            if event_type == "LP_CLOSE":
                stack.realized_pnl_usd += _payload_decimal(payload, "realized_pnl_usd")
                _accumulate_term(stack, "il_usd", il_meter, _payload_decimal_or_none(payload, "il_usd"))
            continue

        # PERP family — a measured EXECUTED PERP_SETTLEMENT supersedes the ESTIMATED
        # PERP_OPEN/PERP_CLOSE submission fold for the same link (VIB-3872 WI-4).
        if _fold_perp_event(
            stack, event, payload, event_type, settlement_by_link, estimate_links, fee_meter, slip_meter
        ):
            continue

    # A bucket is MEASURED only if every applicable event contributed its term
    # (meter[1] == meter[0]) and at least one applicable event existed (> 0).
    # Any-missing → unmeasured ("" sentinel on the wire → "— unmeasured").
    stack.protocol_fees_measured = fee_meter[0] > 0 and fee_meter[1] == fee_meter[0]
    stack.slippage_measured = slip_meter[0] > 0 and slip_meter[1] == slip_meter[0]
    # VIB-6283 — same rule for the LP buckets. Note what "unmeasured" means for
    # an OPEN LP position: no LP_CLOSE / LP_COLLECT_FEES row exists yet, so
    # ``earn_meter[0] == 0`` and the tile renders "—". That is the honest
    # answer. Fees ARE accruing on-chain (a real fork with induced volume shows
    # ~$88 owed on a $1.26M position), but nothing in the accounting lane has
    # measured them until a collect or close lands — and "we have not measured
    # this" must never be rendered as "$0.00".
    stack.fees_earned_measured = earn_meter[0] > 0 and earn_meter[1] == earn_meter[0]
    stack.il_measured = il_meter[0] > 0 and il_meter[1] == il_meter[0]
    # Three states, not two — collapsing them to two is what cost the headline
    # its money in one direction and fabricated a $0 in the other:
    #
    #   any_measured False              -> NOTHING was measured. "" on the wire,
    #                                      "—" on the tile. A close that carried
    #                                      no ``fees_total_usd`` at all must not
    #                                      render "$0.00" (18 of 66 corpus rows).
    #   any_measured True, measured False-> PARTIAL. The sum is REAL — it is the
    #                                      fees we did measure — and must travel,
    #                                      flagged. Dropping it understated
    #                                      Strategy PnL by exactly that amount.
    #   measured True                   -> complete.
    #
    # ``applicable`` (>=1 LP event) is a fourth, separate fact: it distinguishes
    # "no LP leg at all" from "an LP leg we failed to measure", which is what
    # keeps the caveat off every perp / lending / swap strategy.
    stack.fees_earned_applicable = earn_meter[0] > 0
    stack.il_applicable = il_meter[0] > 0
    stack.fees_earned_any_measured = earn_meter[1] > 0
    stack.il_any_measured = il_meter[1] > 0
    return stack


def _inventory_price_for_token(prices: dict[str, Any], token: str) -> Decimal | None:
    """Look up a held swap-inventory token's mark price (VIB-4984).

    ``token`` is the FIFO lot's resolved symbol (e.g. ``"WETH"``).
    ``prices`` is the ``portfolio_snapshots.token_prices`` map. Two shapes
    are supported (mirrors ``pnl_attributor._price_for_token``):

    - flat ``{symbol: price}`` / ``{"chain:address": price}``
    - snapshot shape ``{"chain:0xaf88…": {"price_usd": "1.0", "symbol": "USDC"}}``

    Matching is case-insensitive on the symbol field and on the ``chain:``
    suffix. Returns ``None`` when no mark price is found (degrade — never
    fetch a live price; gateway boundary).
    """
    if not prices or not token:
        return None
    needle = str(token).lower()
    for key, val in prices.items():
        key_str = str(key).lower()
        if key_str == needle or key_str.endswith(":" + needle):
            if isinstance(val, dict):
                price = val.get("price_usd")
                parsed = _to_decimal_or_none(price)
                if parsed is not None:
                    return parsed
                continue
            parsed = _to_decimal_or_none(val)
            if parsed is not None:
                return parsed
        if isinstance(val, dict):
            symbol = val.get("symbol")
            if symbol and str(symbol).lower() == needle:
                parsed = _to_decimal_or_none(val.get("price_usd"))
                if parsed is not None:
                    return parsed
    return None


def _to_decimal_or_none(value: Any) -> Decimal | None:
    """Parse to a finite Decimal or return None (no zero-coercion)."""
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    return parsed if parsed.is_finite() else None


def compute_inventory_unrealized(
    accounting_events: list[dict[str, Any]],
    deployment_id: str,
    latest_token_prices: dict[str, Any],
    exclude_tokens: frozenset[str] = frozenset(),
) -> Decimal | None:
    """Mark-to-market the held directional swap inventory (VIB-4984).

    LEGACY-SNAPSHOT path only (VIB-5057): the snapshot writer now classifies
    open swap-inventory lots as deployed capital and stamps
    ``snapshot_metadata["swap_inventory"]["status"] == "applied"`` — for those
    snapshots the inventory MTM already flows through ``open_position_nav −
    deployed_capital_usd`` and the caller (``GetCostStack``) suppresses this
    additive term to avoid double-counting. This function keeps serving
    snapshots written by pre-classifier writers.

    A directional swap strategy's net-long ``token_out`` inventory (e.g. RSI
    net-long WETH) was valued by legacy snapshot writers as
    ``available_cash_usd``, so it never entered ``deployed_capital_usd`` and
    its mark cancelled out of the Strategy-PnL
    ``unrealized = open_position_nav − deployed_capital_usd``
    term. This function recovers that omitted mark-to-market by replaying the
    already-persisted SWAP accounting events into ``FIFOBasisStore``, summing
    ``remaining * mark_price − cost_usd_for_remaining`` over every open swap
    inventory lot. The result is an ADDITIVE delta (``mark − cost``), so it can
    only enter Strategy PnL once (the mark is already in NAV via cash, but NAV
    is not an input to the tile — see ``_detail_header._strategy_pnl_usd``).

    ``latest_token_prices`` is the ``portfolio_snapshots.token_prices`` map.
    The store-key reconstruction is per-``deployment_id`` so a shared wallet
    only marks this strategy's own inventory.

    ``exclude_tokens`` (VIB-6362) holds case-folded ``"<chain>:<token>"`` keys
    the snapshot writer already folded into a POSITION leg — stamped as
    ``snapshot_metadata["swap_inventory"]["folded_into_positions"]`` when a
    discovered ``PositionType.TOKEN`` row was backed with its swap-lot basis.
    Those lots' ``mark - cost`` now flows through ``open_position_nav -
    deployed_capital_usd``, so adding it here too would count it twice. The
    whole-snapshot ``status == "applied"`` gate in ``GetCostStack`` cannot cover
    this case: the fold happens precisely on the snapshots where the classifier
    produced NO row for that token (``capped_to_zero``), and a co-held token that
    IS still cash must keep its additive term.

    Chain-qualified, matching the writer's stamp: a bare symbol would drop the
    inventory term for that token on EVERY chain of a multi-chain deployment,
    including chains whose lots the writer left classified as cash. The chain is
    read off each lot's own ``swap:<chain>:<wallet>`` key — the same derivation
    the writer uses (``portfolio_valuer._chain_from_swap_position_key``), so the
    two sides cannot disagree about which lot a stamp names. A lot whose key has
    no parseable chain can never match an exclusion and therefore keeps its term
    — the safe direction, since dropping a term understates PnL silently.

    Returns ``None`` (unmeasured — Empty≠Zero) when:
      - ``deployment_id`` is missing/empty — we cannot scope events to this
        strategy, and summing a shared wallet's full event stream would leak a
        co-located strategy's inventory into this tile; fail closed,
      - there are NO open swap inventory lots left after ``exclude_tokens``,
      - any held lot has ``cost_usd_for_remaining is None`` (missing basis —
        do NOT read held inventory as pure profit),
      - a held token has no mark price in ``latest_token_prices`` (degrade —
        do NOT fetch a live price; gateway boundary).
    Otherwise returns the summed Decimal.
    """
    from almanak.framework.accounting.basis import FIFOBasisStore
    from almanak.framework.valuation.portfolio_valuer import _chain_from_swap_position_key

    # Shared-wallet isolation: replay ONLY this deployment's events so a
    # co-located strategy on the same wallet cannot leak inventory into this
    # tile. Without a deployment_id we cannot scope, so we FAIL CLOSED
    # (return None ⇒ tile renders "—") rather than mark over an unscoped,
    # potentially shared-wallet event stream — cross-strategy contamination
    # is worse than an unmeasured tile (CodeRabbit, VIB-4984).
    if not deployment_id:
        return None
    scoped_events = [
        ev for ev in accounting_events if isinstance(ev, dict) and ev.get("deployment_id") == deployment_id
    ]

    store = FIFOBasisStore()
    store.reconstruct_from_events(scoped_events)

    total = Decimal("0")
    saw_lot = False
    for position_key, token, remaining, cost_for_remaining in store.iter_open_swap_lots():
        # VIB-6362: this lot's mark already reached Strategy PnL through a
        # position leg. Skipped BEFORE saw_lot so a snapshot whose every lot was
        # folded returns None (nothing left in cash to recover) rather than a
        # measured 0 — and before the basis/price refusals, which describe lots
        # this term is still responsible for.
        lot_chain = _chain_from_swap_position_key(position_key)
        if lot_chain and f"{lot_chain}:{token.casefold()}" in exclude_tokens:
            continue
        saw_lot = True
        if cost_for_remaining is None:
            # Missing basis — refuse to mark held inventory as pure profit.
            return None
        mark = _inventory_price_for_token(latest_token_prices, token)
        if mark is None:
            # No persisted mark for this held token — degrade rather than
            # fetch a live price (gateway boundary).
            return None
        total += (remaining * mark) - cost_for_remaining

    if not saw_lot:
        return None
    return total


def _perp_reconciliation_terms(
    event: Any,
    payload: dict[str, Any],
    event_type: str,
    settlement_by_link: dict[str, dict[str, Any]],
    estimate_links: set[str],
) -> tuple[Decimal, Decimal]:
    """(Δ perp realized PnL, Δ net funding) for a perp-terminal event (VIB-3872 WI-4).

    Mirrors ``_fold_perp_event``'s per-component supersession for the G6
    reconciliation sums — kept in lockstep so the cost stack and reconciliation agree
    on the perp signal. ``sum_funding`` convention: paid funding REDUCES PnL, so a
    positive ``funding_fee_usd`` (paid) contributes ``-funding_fee_usd``. Empty≠Zero:
    an unmeasured settlement field falls back to the estimate (PERP_CLOSE) or
    contributes nothing (orphan settlement) — never a fabricated zero over a measured
    estimate.
    """
    if event_type == "PERP_CLOSE":
        s = settlement_by_link.get(_row_ledger_id(event))
        perp = _perp_realized_pnl(s, payload)
        s_funding = _payload_decimal_or_none(s, "funding_fee_usd") if s else None
        if s_funding is not None:
            funding = -s_funding
        else:
            funding = _payload_decimal(payload, "funding_received_usd") - _payload_decimal(payload, "funding_paid_usd")
        return perp, funding
    # PERP_SETTLEMENT: only an EXECUTED ORPHAN (no submission estimate) folds here;
    # a linked settlement was already merged by the PERP_CLOSE branch above.
    if payload.get("settlement_state") != "EXECUTED":
        return Decimal("0"), Decimal("0")
    link = str(payload.get("submission_ledger_entry_id") or _row_ledger_id(event))
    if link in estimate_links:
        return Decimal("0"), Decimal("0")
    realized = _payload_decimal_or_none(payload, "realized_pnl_usd")
    funding_fee = _payload_decimal_or_none(payload, "funding_fee_usd")
    return (
        realized if realized is not None else Decimal("0"),
        -funding_fee if funding_fee is not None else Decimal("0"),
    )


def compute_reconciliation(
    initial_value_usd: Decimal,
    nav_usd: Decimal,
    cost_stack: CostStack,
    accounting_events: list[dict[str, Any]],
    *,
    snapshot_initial: dict[str, Any] | None = None,
    snapshot_final: dict[str, Any] | None = None,
    deployment_id: str = "",
) -> ReconciliationStatus:
    """G6: wallet PnL ≡ Σ component PnL within ε.

    Mirrors the canonical decomposition in
    ``almanak.framework.accounting.accountant_test`` — kept here as a
    duplicate for the local (no-pytest) read path. If the formula
    upstream changes, both sites move together (small, audit-friendly).

    Ambient inventory revaluation (blueprint 27 §11.5): when the caller supplies
    the endpoint snapshots (``snapshot_initial`` / ``snapshot_final``) it folds
    the SAME ``compute_inventory_revaluation`` term the Accountant Test G6 cell
    adds, so the dashboard G6 and the harness G6 produce a byte-identical
    ``component_pnl_usd`` on the same DB. Callers that have not yet been wired to
    pass snapshots get the back-compat behaviour (term = 0); an UNMEASURED term
    (Empty≠Zero) leaves the component sum unchanged and flags ``has_unmeasured``
    so the surface can degrade rather than silently fold in a zero.
    """
    status = ReconciliationStatus()
    status.wallet_pnl_usd = nav_usd - initial_value_usd
    status.has_data = bool(accounting_events) or initial_value_usd > Decimal("0")

    sum_swap = Decimal("0")
    sum_lp = Decimal("0")
    sum_perp = Decimal("0")
    sum_fees = Decimal("0")
    sum_funding = Decimal("0")
    sum_interest = Decimal("0")
    # VIB-3872 WI-4: per-component supersession (see _perp_reconciliation_terms).
    settlement_by_link = _executed_settlements_by_link(accounting_events)
    estimate_links = _perp_estimate_links(accounting_events)

    for event in accounting_events:
        payload = _safe_payload_loads(event.get("payload_json") if isinstance(event, dict) else None)
        # Same row-vs-payload precedence as compute_cost_stack: a row
        # with a column-level event_type but a degenerate payload is
        # still valid evidence and must contribute to the G6 buckets.
        row_event_type = ""
        if isinstance(event, dict):
            row_event_type = str(event.get("event_type") or "")
        event_type = (row_event_type or payload.get("event_type") or "").upper()
        if not event_type:
            continue
        if event_type == "SWAP":
            # VIB-4905 (F1): same matched-priority precedence as
            # compute_cost_stack — keep both sites in lockstep so the
            # reconciliation buckets agree on the SWAP signal.
            sum_swap += _payload_decimal(payload, "realized_pnl_usd_matched", "realized_pnl_usd")
        elif event_type in ("LP_CLOSE", "LP_COLLECT_FEES"):
            # VIB-6283: mirrors the compute_cost_stack LP branch — keep both
            # folds in lockstep (the same lockstep note the SWAP branch above
            # makes). A mid-life LP_COLLECT_FEES credits the wallet, so
            # omitting it here left its income in wallet PnL but absent from
            # Σ component PnL, i.e. a G6 gap reported as "unexplained" when the
            # real cause was this missing fold. Lockstep also means the
            # realized-PnL half stays CLOSE-only here for the same reason it
            # does there: a COLLECT's producer-side ``realized_pnl_usd`` is
            # ``fees_value - full_open_basis``, so folding it would book the
            # still-deployed principal as a realized loss and blow up the very
            # G6 gap this branch was widened to close.
            if event_type == "LP_CLOSE":
                sum_lp += _payload_decimal(payload, "realized_pnl_usd")
            sum_fees += _payload_decimal(payload, "fees_total_usd")
        elif event_type in ("PERP_CLOSE", "PERP_SETTLEMENT"):
            # WI-4: per-component supersession (settlement-measured-else-estimate),
            # folded from exactly one source per field — no double-count, no
            # fabricated zero. Mirrors _fold_perp_event.
            delta_perp, delta_funding = _perp_reconciliation_terms(
                event, payload, event_type, settlement_by_link, estimate_links
            )
            sum_perp += delta_perp
            sum_funding += delta_funding
        elif event_type in ("WITHDRAW",):
            sum_interest += _payload_decimal(payload, "interest_accrued_usd", "interest_delta_usd")
        elif event_type in ("REPAY", "DELEVERAGE"):
            sum_interest -= _payload_decimal(payload, "interest_paid_usd", "interest_delta_usd")

    sum_gas = -cost_stack.gas_usd  # gas is a cost (negative contribution)

    # Ambient inventory revaluation (blueprint 27 §11.5). Folds in ONLY when the
    # caller supplied the endpoint snapshots — same lane, same marks, same number
    # as the Accountant Test G6 cell. Empty≠Zero: an unmeasured term is left out
    # of the sum and flagged on ``has_unmeasured`` (never coerced to zero).
    sum_inventory_reval = Decimal("0")
    if snapshot_initial is not None or snapshot_final is not None:
        from almanak.framework.accounting.inventory_revaluation import (
            compute_inventory_revaluation,
        )

        inv = compute_inventory_revaluation(
            snapshot_initial=snapshot_initial,
            snapshot_final=snapshot_final,
            accounting_events=accounting_events,
            deployment_id=deployment_id,
        )
        if inv.total_usd is None:
            status.has_unmeasured = True
        else:
            sum_inventory_reval = inv.total_usd

    component_pnl = sum_swap + sum_lp + sum_perp + sum_fees + sum_funding + sum_interest + sum_gas
    component_pnl += sum_inventory_reval

    status.component_pnl_usd = component_pnl
    status.gap_usd = (status.wallet_pnl_usd - component_pnl).copy_abs()
    status.passed = status.has_data and status.gap_usd <= status.epsilon_usd
    status.sum_swap = sum_swap
    status.sum_lp = sum_lp
    status.sum_perp = sum_perp
    status.sum_fees = sum_fees
    status.sum_funding = sum_funding
    status.sum_interest = sum_interest
    status.sum_gas = sum_gas
    status.sum_inventory_reval = sum_inventory_reval

    return status


def compute_audit_trail(
    ledger_entries: list[Any] | LedgerQuantStats,
    accounting_events: list[dict[str, Any]],
) -> AuditTrailStats:
    """G9 / G12 / G13 dashboard rollup — counts of populated columns.

    Specifically tracks the fields whose absence drives the May 2 mainnet
    Accountant Test gaps: ``price_inputs_json`` (G12),
    ``pre_state_json`` + ``post_state_json`` (G6 + L4 unblock),
    ``gas_usd`` (G2), and the version-stamp triple (G13). The ledger-side
    counts come from :class:`LedgerQuantStats` (SQL ``COUNT`` on the gateway
    hot path, the Python reference aggregation for list callers — VIB-5059).
    """
    stats = AuditTrailStats()
    ledger_stats = _ledger_stats(ledger_entries)
    stats.ledger_total = ledger_stats.total
    stats.ledger_with_price_inputs = ledger_stats.with_price_inputs
    stats.ledger_with_pre_post_state = ledger_stats.with_pre_post_state
    stats.ledger_with_gas_usd = ledger_stats.with_positive_gas_usd

    stats.events_total = len(accounting_events)
    for event in accounting_events:
        payload = _safe_payload_loads(event.get("payload_json") if isinstance(event, dict) else None)
        if not payload:
            continue
        if all(payload.get(k) for k in ("schema_version", "formula_version", "matching_policy_version")):
            stats.events_with_versions += 1

    return stats


# ---------------------------------------------------------------------------
# Lightweight Accountant posture evaluator
# ---------------------------------------------------------------------------


def _detect_primitive(accounting_events: list[dict[str, Any]]) -> str:
    """Pick the dominant primitive from event types.

    Simple plurality: lending if any SUPPLY/BORROW/REPAY/WITHDRAW;
    LP if any LP_OPEN/LP_CLOSE; perp if any PERP_OPEN/PERP_CLOSE;
    swap-only if only SWAPs; mixed if multiple non-swap families.

    Falls back to ``payload_json["event_type"]`` when the row-level
    column is missing — same defensive read pattern as
    ``compute_cost_stack`` / ``compute_reconciliation``.
    """
    has_lending = False
    has_lp = False
    has_perp = False
    has_swap = False
    for event in accounting_events:
        et = ""
        if isinstance(event, dict):
            payload = _safe_payload_loads(event.get("payload_json"))
            et = str(event.get("event_type") or payload.get("event_type") or "").upper()
        if et in ("SUPPLY", "WITHDRAW", "BORROW", "REPAY", "DELEVERAGE"):
            has_lending = True
        elif et in ("LP_OPEN", "LP_CLOSE"):
            has_lp = True
        elif et in ("PERP_OPEN", "PERP_CLOSE"):
            has_perp = True
        elif et == "SWAP":
            has_swap = True

    families = sum([has_lending, has_lp, has_perp])
    if families > 1:
        return "mixed"
    if has_lp:
        return "lp"
    if has_lending:
        return "lending"
    if has_perp:
        return "perp"
    if has_swap:
        return "swap"
    return "mixed"


# crap-allowlist: this PR's diff against ``evaluate_posture`` is pure
# docstring-content cleanup (Track C wording refinement); zero branches added,
# function was already over the CRAP threshold on main (cc=55, cov=86%) and
# carries an existing C901 exemption for the same reason. Mirror of PR #2163's
# treatment of ``runner_state.emit_iteration_summary``. Refactor of
# ``evaluate_posture`` should be tracked under its own ticket and is out of
# scope for this misc cleanup PR.
def evaluate_posture(  # noqa: C901
    primitive: str,
    ledger_entries: list[Any] | LedgerQuantStats,
    accounting_events: list[dict[str, Any]],
    snapshots: list[Any],
    audit: AuditTrailStats,
    reconciliation: ReconciliationStatus,
    portfolio_metrics: Any,
) -> AccountantPosture:
    """Lightweight posture: which cells PASS / FAIL / XFAIL given the
    data on disk.

    XFAIL = the lightweight dashboard posture cannot currently evaluate the
    Track C table. Local SQLite may contain ``position_state_snapshots`` rows,
    but this aggregation surface does not load them; hosted Track C also remains
    gated. The full Accountant Test/reporting query is the authoritative path
    for Track-C PASS/FAIL.
    """
    posture = AccountantPosture(primitive=primitive)

    cells = list(_GENERIC_CELLS)
    if primitive == "lp":
        cells.extend(_LP_CELLS)
    elif primitive == "lending":
        cells.extend(_LENDING_CELLS)
    elif primitive == "perp":
        cells.extend(_PERP_CELLS)
    else:
        # mixed/swap: just the 15 generic cells
        pass
    posture.cells_total = len(cells)

    ledger_stats = _ledger_stats(ledger_entries)
    have_ledger = ledger_stats.total > 0
    have_events = len(accounting_events) > 0

    def _ev_status(cell: str, passed: bool, *, structurally_xfail: bool = False) -> None:
        if structurally_xfail or cell in _TRACK_C_DEPENDENT:
            posture.cells_xfail += 1
            posture.xfail.append(cell)
            return
        if passed:
            posture.cells_passed += 1
        else:
            posture.cells_failed += 1
            posture.failing.append(cell)

    # --- Generic cells ---------------------------------------------------
    # G1/G7: "every ledger row carries X" ⇔ the non-empty-column count equals
    # the row count (the legacy ``all(...)`` loops expressed as SQL COUNTs).
    _ev_status(
        "G1",
        have_ledger and ledger_stats.with_tx_hash == ledger_stats.total,
    )
    _ev_status(
        "G2",
        have_ledger and audit.ledger_with_gas_usd == audit.ledger_total,
    )
    _ev_status("G3", have_events)  # any yield-bearing event present
    # ``portfolio_metrics`` can be ``None`` when the gateway RPC fails or the
    # strategy hasn't written its first portfolio snapshot yet — guard the
    # attribute access so the posture aggregator stays evaluable on an empty
    # / fresh DB.
    if portfolio_metrics is not None:
        deployed_capital = _to_decimal(getattr(portfolio_metrics, "initial_value_usd", "0"))
        nav_now = _to_decimal(getattr(portfolio_metrics, "total_value_usd", "0"))
    else:
        deployed_capital = Decimal("0")
        nav_now = Decimal("0")
    _ev_status("G4", deployed_capital > Decimal("0") or nav_now > Decimal("0"))
    _ev_status("G5", deployed_capital > Decimal("0") and nav_now > Decimal("0"))
    _ev_status("G6", reconciliation.has_data and reconciliation.passed)
    _ev_status(
        "G7",
        have_ledger and ledger_stats.with_cycle_id == ledger_stats.total,
    )
    _ev_status("G8", len(snapshots) >= 2)
    _ev_status(
        "G9",
        have_events
        and all(
            (e.get("confidence") if isinstance(e, dict) else getattr(e, "confidence", "")) for e in accounting_events
        ),
    )
    _ev_status("G10", have_ledger)  # 1:1 by ledger schema construction
    _ev_status("G11", True)  # no failed intent contract — vacuously OK
    _ev_status(
        "G12",
        audit.ledger_total > 0 and audit.ledger_with_price_inputs == audit.ledger_total,
    )
    _ev_status(
        "G13",
        audit.events_total > 0 and audit.events_with_versions == audit.events_total,
    )
    _ev_status("G14", False)  # XFAIL via _TRACK_C_DEPENDENT
    _ev_status("G15", False)  # XFAIL via _TRACK_C_DEPENDENT

    # --- Primitive cells -------------------------------------------------
    if primitive == "lp":
        _ev_status(
            "LP1",
            any(
                (e.get("event_type") or "").upper() in ("LP_OPEN", "LP_CLOSE", "SNAPSHOT")
                for e in accounting_events
                if isinstance(e, dict)
            ),
        )
        for c in ("LP2", "LP4", "LP5", "LP6"):
            _ev_status(c, False)  # XFAIL via _TRACK_C_DEPENDENT
        _ev_status(
            "LP3",
            any(
                _payload_decimal(_safe_payload_loads(e.get("payload_json")), "fees_total_usd") > 0
                for e in accounting_events
                if isinstance(e, dict) and (e.get("event_type") or "").upper() == "LP_CLOSE"
            )
            if have_events
            else False,
        )
    elif primitive == "lending":
        for c in ("L1", "L2", "L3", "L5", "L6"):
            _ev_status(c, False)  # XFAIL via _TRACK_C_DEPENDENT
        # L4 — REPAY rows must carry principal_repaid_usd ≠ NULL
        repay_ok = False
        for e in accounting_events:
            if not isinstance(e, dict):
                continue
            if (e.get("event_type") or "").upper() not in ("REPAY", "DELEVERAGE"):
                continue
            p = _safe_payload_loads(e.get("payload_json"))
            if p.get("principal_repaid_usd") and p.get("interest_paid_usd"):
                repay_ok = True
                break
        # If no REPAY ever happened, vacuous XFAIL — but if REPAY exists with NULLs, FAIL.
        has_repay = any(
            (e.get("event_type") or "").upper() in ("REPAY", "DELEVERAGE")
            for e in accounting_events
            if isinstance(e, dict)
        )
        if not has_repay:
            _ev_status("L4", False, structurally_xfail=True)
        else:
            _ev_status("L4", repay_ok)
    elif primitive == "perp":
        for c in ("P2", "P4", "P5", "P6"):
            _ev_status(c, False)  # XFAIL via _TRACK_C_DEPENDENT
        _ev_status(
            "P1",
            any(
                (e.get("event_type") or "").upper() in ("PERP_OPEN", "PERP_CLOSE")
                for e in accounting_events
                if isinstance(e, dict)
            ),
        )
        # P3: open/close fee separability
        p3_ok = (
            any(
                isinstance(e, dict)
                and (e.get("event_type") or "").upper() in ("PERP_OPEN", "PERP_CLOSE")
                and (
                    _payload_decimal(_safe_payload_loads(e.get("payload_json")), "open_fee_usd") > 0
                    or _payload_decimal(_safe_payload_loads(e.get("payload_json")), "close_fee_usd") > 0
                )
                for e in accounting_events
            )
            if have_events
            else False
        )
        _ev_status("P3", p3_ok)

    return posture


# ---------------------------------------------------------------------------
# Drawdown / APR helpers
# ---------------------------------------------------------------------------


def _snapshot_value_confidence(snapshot: Any, *, field_name: str) -> ValueConfidence | None:
    """Read and type one snapshot confidence at an aggregation boundary."""
    raw = (
        snapshot.get("value_confidence") if isinstance(snapshot, dict) else getattr(snapshot, "value_confidence", None)
    )
    return ValueConfidence.parse_optional(raw, field_name=field_name)


def _drawdowns(snapshots: list[Any]) -> tuple[Decimal, Decimal]:
    """Return (max_drawdown_pct, current_drawdown_pct).

    Both expressed as positive percentages (0–100). Empty / single-snapshot
    histories return (0, 0).

    VIB-3884: drawdowns measure wallet NAV decline, so the per-snapshot
    value is ``total_value_usd + available_cash_usd`` (positions + cash),
    matching the "NAV now" header tile. Pre-VIB-3884 this read the
    deployed-only column, which produced spurious "drawdown 100%"
    readings whenever the strategy was fully un-deployed.

    VIB-4983 follow-up: net the BORROW debt leg per-snapshot so the series
    matches the debt-netted "NAV now" tile (``compute_pnl_summary``). The
    recent-window snapshots carry ``positions_json`` (``get_recent_snapshots``
    selects it), so the un-netted phantom — wallet NAV spiking up by the borrow
    at open and collapsing at teardown, manufacturing a large lifecycle
    drawdown on a flat equity loop — is removed here. Non-leveraged snapshots
    subtract ``Decimal("0")`` (no negative leg) and stay byte-identical.

    VIB-5408 / ALM-3086: skip snapshots whose confidence is absent or
    ``UNAVAILABLE`` (== ``not PortfolioSnapshot.is_valid``). Their NAV cannot be
    trusted as a measured sample; folding one would manufacture a drawdown or move
    the high-watermark. ``ESTIMATED`` / ``STALE`` remain valued because they are
    priced, merely imprecise or late. Unknown strings fail at this named boundary.

    VIB-6283: and skip snapshots carrying a ``strategy_reported`` mark, by the
    same rule and the same predicate the lifetime path uses. This is NOT a
    duplicate gate — it is the SECOND drawdown entrypoint.
    :func:`compute_pnl_summary` falls back here whenever the lifetime scan is
    unavailable (no state manager, empty series, or a transient
    ``get_nav_series`` failure — ``dashboard_service._get_lifetime_drawdown``
    returns ``None``), so gating only the lifetime reader left the documented
    graceful-degrade path folding exactly the phantom this fix exists to remove.
    Both PR #3532 reviewers found this independently; the fault was fixing one
    entrypoint of a two-entrypoint rule.
    """
    values: list[Decimal] = []
    for snap in snapshots:
        if _snapshot_has_strategy_reported_mark(snap):
            # Checked BEFORE the confidence gate: such a row is typically
            # stamped ESTIMATED, which that gate deliberately keeps.
            continue
        confidence = _snapshot_value_confidence(snap, field_name="drawdown snapshot.value_confidence")
        if confidence is None or confidence == ValueConfidence.UNAVAILABLE:
            continue
        v = getattr(snap, "total_value_usd", None)
        if v is None and isinstance(snap, dict):
            v = snap.get("total_value_usd")
        cash = getattr(snap, "available_cash_usd", None)
        if cash is None and isinstance(snap, dict):
            cash = snap.get("available_cash_usd")
        _count, debt_mark, _debt_cost, _net_cost = net_debt_from_snapshot(snap)
        # VIB-5942: one shared wallet-NAV definition (total − debt + cash) — the
        # same helper the NAV-history chart, the drawdown text reader, and the
        # NAV-now tile use, so the recent-window drawdown cannot drift from them.
        wallet_nav = wallet_nav_usd(_to_decimal(v), debt_mark, _to_decimal(cash))
        if wallet_nav > Decimal("0"):
            values.append(wallet_nav)
    return _drawdown_stats(values)


@dataclass(frozen=True)
class DrawdownState:
    """Resumable running-peak drawdown fold over a wallet-NAV series (VIB-5134).

    Represents the drawdown recurrence after folding some chronological prefix of
    a wallet-NAV series. Folding the remaining suffix via :func:`fold_drawdowns`
    is **byte-identical** to a single :func:`_drawdown_stats` recompute over the
    whole series — the recurrence is associative in the running peak — so the
    gateway can keep current-drawdown live by folding only the snapshots newer
    than a cursor instead of re-scanning the full history every render.

    Fields hold raw recurrence state; ``max_drawdown`` is a **fraction** in
    ``[0, 1)`` (converted to a percentage only at the property boundary, mirroring
    :func:`_drawdown_stats`'s final ``* 100``). ``running_peak`` / ``latest_nav``
    are ``None`` only before any positive sample has been folded.
    """

    running_peak: Decimal | None = None
    max_drawdown: Decimal = Decimal("0")  # fraction in [0, 1)
    latest_nav: Decimal | None = None

    @property
    def max_drawdown_pct(self) -> Decimal:
        return self.max_drawdown * Decimal("100")

    @property
    def current_drawdown_pct(self) -> Decimal:
        if self.running_peak is None or self.running_peak <= Decimal("0") or self.latest_nav is None:
            return Decimal("0")
        return (self.running_peak - self.latest_nav) / self.running_peak * Decimal("100")

    def as_pcts(self) -> tuple[Decimal, Decimal]:
        """``(max_drawdown_pct, current_drawdown_pct)`` — the dashboard tuple."""
        return self.max_drawdown_pct, self.current_drawdown_pct


_EMPTY_DRAWDOWN_STATE = DrawdownState()


def fold_drawdowns(state: DrawdownState, navs: Iterable[Decimal]) -> DrawdownState:
    """Fold additional positive wallet-NAVs (chronological) into ``state`` (VIB-5134).

    ``navs`` MUST already be filtered to ``> 0`` (the Empty≠Zero decision lives in
    the caller / :func:`fold_nav_text`). The running-peak recurrence is resumable:
    ``fold_drawdowns(fold_drawdowns(EMPTY, A), B)`` equals
    ``fold_drawdowns(EMPTY, A + B)`` for any split — which is what lets the gateway
    advance a checkpoint by only the newest snapshots and still report the lifetime
    figure a full recompute would.
    """
    running_peak = state.running_peak
    max_dd = state.max_drawdown
    latest = state.latest_nav
    for nav in navs:
        if running_peak is None or nav > running_peak:
            running_peak = nav
        # running_peak >= nav > 0 here, so the division is always well-defined.
        dd = (running_peak - nav) / running_peak
        if dd > max_dd:
            max_dd = dd
        latest = nav
    return DrawdownState(running_peak=running_peak, max_drawdown=max_dd, latest_nav=latest)


def _snapshot_positions(snap: Any) -> list[Any]:
    """Positions of a snapshot object OR dict row, typed-first (VIB-6283).

    **Precedence mirrors :func:`net_debt_from_snapshot` deliberately, line for
    line.** ``StateManager.get_recent_snapshots`` returns typed
    ``PortfolioSnapshot`` objects on both backends, and a real
    ``PortfolioSnapshot`` has **no** ``positions_json`` attribute — reading it
    yields ``None``, the caller silently no-ops, and the feature is inert in
    production while every dict-shaped unit test passes. That is VIB-5170,
    which `net_debt.py` documents eight lines into the sibling helper this
    module already imports. The first version of this accessor read
    ``positions_json`` only and reproduced VIB-5170 exactly: `_drawdowns`
    returned a 74% max-drawdown on the very phantom the gate was added to
    refuse. Do not "simplify" this back to a single attribute read.
    """
    positions = getattr(snap, "positions", None)
    if positions is not None and not isinstance(snap, dict):
        return list(positions)
    if isinstance(snap, dict):
        payload = snap.get("positions_json")
        if payload is None:
            payload = snap.get("positions")
    else:
        payload = getattr(snap, "positions_json", None)
    return parse_positions_payload(payload)


def _position_is_strategy_reported(position: Any) -> bool:
    """Is this ONE position valued from the strategy's self-report? (VIB-6283)

    Shape-tolerant on purpose: the lifetime path parses JSON text into ``dict``
    rows while the recent-window path holds typed ``PositionValue`` dataclasses.
    A dict-only check made the second path inert — the same half of the VIB-5170
    trap as the accessor above, and the reason this predicate is a named
    function rather than an inline expression at two call sites.
    """
    details = getattr(position, "details", None)
    if details is None and isinstance(position, dict):
        details = position.get("details")
    return isinstance(details, dict) and details.get("valuation_source") == STRATEGY_REPORTED_VALUATION_SOURCE


def _snapshot_has_strategy_reported_mark(snap: Any) -> bool:
    """Provenance gate for a snapshot OBJECT (the recent-window drawdown path)."""
    return any(_position_is_strategy_reported(p) for p in _snapshot_positions(snap))


def _nav_basis_partial(snapshot: Any) -> bool:
    """Did a leg counted into open-position NAV contribute no cost basis? (VIB-6308)

    Reads the writer's ``nav_basis_coverage`` stamp -- see
    ``portfolio_valuer._nav_basis_coverage`` for why only the writer can measure
    this. Three states:

    * stamp present, ``legs_missing_basis > 0`` -> **partial** (return ``True``),
    * stamp present, ``legs_missing_basis == 0`` -> fully backed (``False``),
    * **stamp absent** -> coverage UNKNOWN (a fully-backed snapshot also omits
      it, by the metadata convention). Returns ``False``: a snapshot we cannot
      assess must not be labelled partial on no evidence, which would flip every
      historical row's tile to a warning.

    Tolerates the typed ``PortfolioSnapshot`` (``snapshot_metadata``) and the
    dict / envelope shapes the legacy readers hand in, because both reach this
    function in practice -- a real snapshot has no ``positions_json`` attribute
    (the VIB-5170 inert-feature class), so keying on one shape alone silently
    no-ops in production.
    """
    meta: Any = None
    if snapshot is not None:
        meta = (
            snapshot.get("snapshot_metadata")
            if isinstance(snapshot, dict)
            else getattr(snapshot, "snapshot_metadata", None)
        )
    if not isinstance(meta, dict):
        payload = (
            snapshot.get("positions_json") if isinstance(snapshot, dict) else getattr(snapshot, "positions_json", None)
        )
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (json.JSONDecodeError, TypeError):
                payload = None
        meta = payload.get("metadata") if isinstance(payload, dict) else None
    if not isinstance(meta, dict):
        return False
    coverage = meta.get("nav_basis_coverage")
    if not isinstance(coverage, dict):
        return False
    try:
        return int(coverage.get("legs_missing_basis") or 0) > 0
    except (TypeError, ValueError):
        return False


def _has_strategy_reported_mark(positions_json: Any) -> bool:
    """Provenance gate for a raw ``positions_json`` PAYLOAD (the lifetime path).

    ``True`` when ANY position in the payload was valued from the strategy's own
    self-report rather than a measurement — one such position is enough to make
    the row's ``total_value_usd`` untrustworthy as a NAV sample, exactly as one
    unmeasured position makes an ``UNAVAILABLE`` row's NAV deflated.

    Reuses :func:`parse_positions_payload` so the text/dict/list/envelope
    unwrapping has one owner — hand-rolling ``json.loads`` here would silently
    return ``[]`` for the already-deserialized payloads hosted Postgres hands
    back (the VIB-5170 inert-feature bug), and the gate would never fire in
    hosted mode.
    """
    return any(_position_is_strategy_reported(p) for p in parse_positions_payload(positions_json))


def _wallet_navs_from_nav_text(rows: Iterable[tuple[Any, ...]]) -> list[Decimal]:
    """Positive, debt-netted wallet-NAVs from ``get_nav_series`` rows (Empty≠Zero).

    Reads ``total_value_usd`` / ``available_cash_usd`` by position (row[1], row[2])
    so it is tolerant of the row arity — the reader appends ``id`` (row[3],
    VIB-5134), ``positions_json`` text (row[4], VIB-5170), and ``value_confidence``
    text (row[5], VIB-5408); legacy 3-/4-/5-tuples still extract the NAV columns
    identically and simply skip the debt netting / confidence gate.

    Each wallet-NAV is ``total − debt_mark + cash`` — the BORROW leg
    (Σ|negative value_usd| from ``positions_json``) is netted per row so the
    lifetime drawdown matches the debt-netted "NAV now" tile and
    :func:`_drawdowns`. Without it the lifetime series (preferred over the recent
    window on the main PnL surface) phantom-spikes at open and collapses at
    teardown for a flat leverage loop. Filtered to ``> 0`` so ``""`` / ``None`` /
    garbage (unmeasured) drop out; a row with no ``positions_json`` (short tuple)
    nets ``Decimal("0")`` debt — byte-identical to the pre-VIB-5170 behaviour.

    **VIB-5408 / ALM-3086 confidence gate (display-correctness, not fund-safety).**
    A row whose ``value_confidence`` (row[5]) is absent or ``UNAVAILABLE`` is
    SKIPPED: such a snapshot's ``total_value_usd`` may exclude an unmeasured
    position (e.g. a held PT that could not be priced —
    ``portfolio_valuer.py`` ``_pt_unmeasured_row`` — or VIB-5406's drain-barrier
    degrade), so the NAV is *deflated*. Folding it would manufacture a phantom
    drawdown dip and corrupt the displayed high-watermark / max-drawdown tiles.
    These tiles feed dashboard display + an agent-tools report dict only — no risk
    breaker or auto-teardown consumes them — so this is a display-correctness fix.

    The gate is aligned with ``PortfolioSnapshot.is_valid``: missing confidence and
    ``UNAVAILABLE`` are unmeasured; ``ESTIMATED`` (CEX / API estimates) and ``STALE``
    (old-but-real) snapshots ARE valued — the position is priced, just imprecisely
    or late — so they are *not* deflated and must NOT be skipped: dropping them
    would remove legitimate NAV samples and could MASK a real drawdown (a strategy
    legitimately ``ESTIMATED`` for its whole life would otherwise show 0% max-DD
    forever). For a drawdown metric a masked-dip false-negative is worse than an
    imprecise-but-real point.

    Skip-carry-forward is the chosen policy: dropping the ``UNAVAILABLE`` sample
    leaves the running peak untouched, so the fold behaves as if the last measured
    point persisted — no interpolation (which would invent a value) and no hard gap
    (lifetime drawdown is a default header metric that must degrade gracefully). A
    row with no confidence element, ``""``, or ``None`` is unmeasured and skipped;
    an unknown non-empty value is surfaced as an error instead of being trusted.

    **VIB-6283 strategy-reported gate.** The paragraph above is right that an
    ``ESTIMATED`` snapshot is normally "priced, just imprecisely" — but one
    ``ESTIMATED`` producer is not priced at all: when no on-chain path can value a
    position, the valuer may carry the STRATEGY'S OWN reported number, which for
    an LP is typically its *requested* config amounts, frozen at entry. That mark
    is stamped ``valuation_source="strategy_reported"``
    (:data:`STRATEGY_REPORTED_VALUATION_SOURCE`) and is skipped here for exactly
    the reason an unmeasured confidence is: it does not track the position, so
    folding it manufactures a drawdown out of the *mark's* movement rather than
    the portfolio's.

    Field-proven: on mainnet a V4 leg carried a frozen $4.4434 mark for its whole
    hold, then went to $0 at close. The fold read that evaporation as a real
    drawdown and rendered **-23.1% max DD** on a position that lost **$0.0078**;
    re-folding the same series against the measured value gives **0.35%**. And
    because max-drawdown is a running EXTREME it *latches* — the badge stayed
    wrong permanently even after the mark itself corrected at close. Demoting the
    mark's confidence alone cannot fix this: ``ESTIMATED`` is deliberately still
    valued by the gate above, which is why provenance is a separate axis from
    confidence.
    """
    navs: list[Decimal] = []
    for row in rows:
        if len(row) > 4 and _has_strategy_reported_mark(row[4]):
            # Not a measurement — see the VIB-6283 paragraph above. Same
            # skip-carry-forward policy: the running peak is left untouched rather
            # than being set by a number nobody measured. Checked BEFORE the
            # confidence gate because such a row is typically stamped ESTIMATED,
            # which that gate deliberately keeps.
            continue
        raw_confidence = row[5] if len(row) > 5 else None
        confidence = ValueConfidence.parse_optional(
            raw_confidence,
            field_name="NAV series value_confidence",
        )
        if confidence is None or confidence == ValueConfidence.UNAVAILABLE:
            # Unmeasured confidence ⇒ skip-carry-forward so this sample never moves
            # the running peak/drawdown. ESTIMATED / STALE remain valued.
            continue
        debt_mark = Decimal("0")
        if len(row) > 4:
            _count, debt_mark, _debt_cost = net_debt_from_positions_json(row[4])
        # VIB-5942: one wallet-NAV definition (total − debt + cash) shared with the
        # NAV-now tile and the NAV-history chart series. Byte-identical to the prior
        # inline expression; routed through the helper so the three cannot drift.
        wallet_nav = wallet_nav_usd(_to_decimal(row[1]), debt_mark, _to_decimal(row[2]))
        if wallet_nav > Decimal("0"):
            navs.append(wallet_nav)
    return navs


def fold_nav_text(state: DrawdownState, rows: Iterable[tuple[Any, ...]]) -> DrawdownState:
    """Fold ``get_nav_series`` text rows into ``state`` (VIB-5134).

    Thin bridge between the raw-text reader rows and the numeric
    :func:`fold_drawdowns`: applies the Empty≠Zero filter
    (:func:`_wallet_navs_from_nav_text`) then folds. Used both for the full-history
    seed (``state=_EMPTY_DRAWDOWN_STATE``) and the gateway's incremental advance.
    """
    return fold_drawdowns(state, _wallet_navs_from_nav_text(rows))


def _drawdown_stats(values: list[Decimal]) -> tuple[Decimal, Decimal]:
    """Running-peak drawdown recurrence over an ordered wallet-NAV series.

    ``values`` are positive wallet-NAVs (``total_value_usd + available_cash_usd``,
    VIB-3884) in chronological order — already filtered to ``> 0``. Returns
    ``(max_drawdown_pct, current_drawdown_pct)`` as positive percentages.

    Extracted from :func:`_drawdowns` (VIB-5118) so the recent-window path and
    the lifetime full-series path (:func:`lifetime_drawdowns_from_nav_text`) run
    the **identical** recurrence. Since VIB-5134 the recurrence itself lives in
    :func:`fold_drawdowns`, so the windowed path, the lifetime path, and the
    gateway's incremental fold are provably one algorithm. The ``< 2`` guard
    preserves the original ``(0, 0)`` for empty / single-sample histories.
    """
    # Presentational guard only: fold_drawdowns independently yields (0, 0) for an
    # empty or single-sample series (current = (peak - peak) / peak = 0), so this is
    # the original VIB-5118 contract made explicit, not a second source of truth.
    if len(values) < 2:
        return Decimal("0"), Decimal("0")
    return fold_drawdowns(_EMPTY_DRAWDOWN_STATE, values).as_pcts()


def lifetime_drawdowns_from_nav_text(
    rows: list[tuple[Any, ...]],
) -> tuple[Decimal, Decimal]:
    """Lifetime ``(max_drawdown_pct, current_drawdown_pct)`` from a full NAV series.

    ``rows`` are ``(timestamp, total_value_usd_text, available_cash_usd_text, id,
    positions_json_text, value_confidence_text)`` oldest-first, as returned by
    ``StateManager.get_nav_series`` — the **whole** snapshot history rather than the
    recent 168-row window that :func:`_drawdowns` sees via the dashboard loader.
    Fixes VIB-5118, where a lifetime peak/drawdown older than ~14h was silently
    understated because the running peak only saw the recent window.

    The NAV columns arrive as raw text so the Empty≠Zero decision (including the
    VIB-5408 ``UNAVAILABLE``-confidence skip) lives in
    :func:`_wallet_navs_from_nav_text`; the filtered series is then fed through
    the shared :func:`fold_drawdowns` recurrence. Equivalent to
    ``fold_nav_text(EMPTY, rows).as_pcts()``; kept as a named entry point for the
    full-scan callers and the VIB-5118 regression suite.
    """
    return fold_nav_text(_EMPTY_DRAWDOWN_STATE, rows).as_pcts()


# VIB-6283 — see ``pages/_detail_header._MIN_ANNUALISATION_DAYS``. Kept in
# lockstep with that constant: the two annualisation sites must agree on when a
# window is too short to annualise, or they reproduce the very disagreement
# (one "—", one "0.00%") this change removes.
_MIN_ANNUALISATION_DAYS = Decimal("1") / Decimal("24")


def _annualised_return(initial_value: Decimal, current_value: Decimal, age_days: Decimal | int) -> Decimal | None:
    """Naïve annualised return = (NAV/Deployed - 1) × 365 / age_days × 100.

    VIB-6283 — two fixes.

    ``age_days`` is now FRACTIONAL (see :func:`strategy_age_days_exact`). It
    used to be integer floor days, so every run shorter than 24 h annualised
    against a denominator of ``0`` and fell into the guard below.

    The guard used to ``return Decimal("0")`` — a FABRICATED ZERO that read as
    "this strategy returned 0 %" when the truth was "not computable yet". It
    now returns ``None`` (unmeasured), matching ``_strategy_apr_pct``, which
    returned ``None`` on the identical condition. Two computations of
    annualised return with opposite Empty≠Zero behaviour was itself a bug.
    """
    age = Decimal(str(age_days))
    if initial_value <= Decimal("0") or age < _MIN_ANNUALISATION_DAYS:
        return None
    raw = (current_value - initial_value) / initial_value
    return raw * Decimal("365") / age * Decimal("100")


def _strategy_age_seconds(portfolio_metrics: Any) -> int:
    """Elapsed seconds since the strategy's first measured value, or 0.

    VIB-6283 — the shared, unquantised age primitive. Both the DISPLAY age
    (whole days, "3d age") and the ANNUALISATION denominator (fractional days)
    derive from this, so they can never disagree about how old a run is.
    """
    initial_ts = getattr(portfolio_metrics, "initial_timestamp", None)
    if not initial_ts:
        return 0
    try:
        if isinstance(initial_ts, str):
            initial_dt = datetime.fromisoformat(initial_ts)
        else:
            initial_dt = initial_ts
        if initial_dt.tzinfo is None:
            initial_dt = initial_dt.replace(tzinfo=UTC)
    except (ValueError, TypeError):
        return 0
    delta: timedelta = datetime.now(tz=UTC) - initial_dt
    return max(int(delta.total_seconds()), 0)


def _strategy_age_days(portfolio_metrics: Any) -> int:
    """Whole elapsed days — the DISPLAY value ("0d age" on a 6 h run).

    Correct as a label; it was never correct as an annualisation denominator.
    Use :func:`strategy_age_days_exact` for that.
    """
    return _strategy_age_seconds(portfolio_metrics) // 86400


def strategy_age_days_exact(portfolio_metrics: Any) -> Decimal:
    """Fractional elapsed days — the ANNUALISATION denominator (VIB-6283).

    ``_strategy_age_days`` floors to whole days, so a 6 h run yielded ``0`` and
    every annualised figure on a sub-24 h run was suppressed or fabricated.
    A 6 h run is ``Decimal("0.25")`` here.
    """
    return Decimal(_strategy_age_seconds(portfolio_metrics)) / Decimal("86400")


# ---------------------------------------------------------------------------
# Debt-netting for leveraged-lending NAV (VIB-4983).
#
# ``portfolio_snapshots.total_value_usd`` is positive-position-scoped (VIB-3614):
# it sums only positions whose ``value_usd > 0`` (Aave SUPPLY collateral counted;
# the BORROW debt leg — same ``positions_json``, *negative* ``value_usd`` — dropped),
# so ``nav_usd = total_value_usd + cash`` overstates an open leverage loop by the
# un-netted debt. The netting that recovers ``collateral − debt`` is the canonical
# ``valuation/net_debt.py::compute_net_debt_projection`` (the PortfolioValuer
# projection contract, blueprint 27 §7.11). VIB-5225 (US-016) deleted this module's
# duplicate ``_net_from_position_items`` / ``_snapshot_net_debt`` /
# ``_open_positions_and_net_debt`` wrappers and routes the read paths above
# (``_drawdowns``, ``_wallet_navs_from_nav_text``, ``compute_pnl_summary``) directly
# through ``net_debt.net_debt_from_snapshot`` / ``net_debt_from_positions_json`` — the
# single home for both the netting math and its typed accessors.
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Top-level builder
# ---------------------------------------------------------------------------


def compute_pnl_summary(
    *,
    portfolio_metrics: Any,
    snapshots: list[Any],
    ledger_entries: list[Any] | LedgerQuantStats,
    accounting_events: list[dict[str, Any]],
    position_summary: Any | None = None,
    lifetime_drawdown: tuple[Decimal, Decimal] | None = None,
) -> PnLSummary:
    """Wallet-level money trail + cash buffer + primary-risk gauge.

    All inputs are already-fetched objects from the StateManager — this
    function does no I/O. Empty inputs collapse gracefully to a summary
    with unmeasured (``None``) confidence and zero-valued tiles, never an
    exception. Confidence remains ``None`` until a measured snapshot supplies it.

    Decomposed from ``build_quant_header`` (VIB-3969) so a PnL consumer
    never pays the cost of computing G6 reconciliation + 21-cell
    Accountant Test posture. Backs ``GetPnLSummary``.
    """
    pnl = PnLSummary()

    # ── Latest snapshot first: needed to compute wallet NAV (VIB-3884) ───
    # Raw ``total_value_usd`` + debt kept separately so the "NAV now" tile below
    # computes wallet NAV through the ONE shared ``wallet_nav_usd`` helper with
    # honest (total, debt, cash) args (VIB-5942) — same definition as the NAV
    # chart + drawdown series. Hoisted so they are bound when there is no snapshot.
    _raw_total_value_usd = Decimal("0")
    _debt_to_net = Decimal("0")
    _debt_cost_to_net = Decimal("0")
    # VIB-5339/VIB-5738: the dust-aware DISPLAY count for a typed snapshot, or
    # None for dict/legacy shapes (and when there is no snapshot). Hoisted to
    # function scope so the accounting-events fallback below can tell "we already
    # resolved a typed count" from "no typed count available".
    _display_count: int | None = None
    if snapshots:
        latest = snapshots[-1]
        pnl.available_cash_usd = _to_decimal(getattr(latest, "available_cash_usd", "0"))
        # Empty != zero: preserve absent confidence as None. Unknown boundary
        # strings fail rather than being normalized or silently trusted.
        pnl.value_confidence = _snapshot_value_confidence(
            latest,
            field_name="PnL snapshot.value_confidence",
        )
        pnl.deployed_capital_usd = _to_decimal(getattr(latest, "deployed_capital_usd", "0"))
        _raw_total_value_usd = _to_decimal(getattr(latest, "total_value_usd", "0"))
        # Open position count + leverage debt-netting from positions_json.
        # VIB-4983: net the BORROW debt leg (negative value_usd) that
        # total_value_usd (positive-position-scoped, VIB-3614) dropped, so an
        # open leverage loop's NAV reads collateral − debt + cash instead of
        # collateral + cash (which manufactures a phantom −debt loss). A
        # position set with no negative leg subtracts Decimal("0") — non-
        # leveraged strategies are byte-identical to before.
        # Read the TYPED positions list (PortfolioSnapshot.positions) — a real
        # snapshot has no ``positions_json`` attribute, so the prior
        # getattr(..., "positions_json") returned None and silently no-op'd the
        # netting in production (the inert-feature bug behind the persisted
        # leverage phantom). net_debt_from_snapshot prefers .positions, falling
        # back to positions_json for dict / legacy callers.
        pnl.open_position_count, _debt_to_net, _debt_cost_to_net, _net_cost = net_debt_from_snapshot(latest)
        # VIB-5339 / VIB-5738: the debt-netting count is len(positions) — it
        # counts sub-floor dust / fully-closed (~0) legs as open positions, so a
        # torn-down wallet reads "1 open position(s)". Override with the dust-
        # aware DISPLAY count (money math above is untouched). Only when the
        # snapshot exposes typed positions; dict/legacy shapes keep the len count.
        _display_count = _count_open_positions(latest)
        if _display_count is not None:
            pnl.open_position_count = _display_count

        # VIB-6308: Strategy PnL differences a fully-measured NAV against a cost
        # basis that SKIPS any leg whose ``cost_basis_usd`` did not survive
        # serialization, so an unbacked leg's mark books as pure profit. The
        # writer stamps which NAV legs lacked a basis (it is the only layer that
        # knows the VIB-4909 wallet-overlap partition); read it here rather than
        # re-deriving that partition a second time.
        pnl.cost_basis_partial = _nav_basis_partial(latest)

    # VIB-3914: Anchor "Deployed" to the wallet snapshot the strategy
    # itself captured at first intent, not the ``portfolio_metrics``
    # row (which is seeded from the config knob and unaware of pre-existing
    # wallet contents). Falls back to portfolio_metrics only when no
    # ledger row carries pre_state — e.g., strategy hasn't acted yet.
    # VIB-5059: the anchor arrives precomputed on LedgerQuantStats (the
    # gateway's bounded anchor walk); list callers reduce through the
    # reference walk inside _ledger_stats.
    wallet_anchored = _ledger_stats(ledger_entries).first_action_wallet_value_usd

    # VIB-5866: read the flows PRESENCE-AWARE (``_to_decimal_or_none``). The
    # old ``_to_decimal`` coerced an unmeasured flow to ``Decimal("0")`` — a
    # read-side zero fabrication that books an external deposit as profit in
    # the lifetime-PnL headline (nav − deployed). ``None`` here means the
    # producer could not measure the cumulative flows: the honest response is a
    # suppressed headline, not a confident wrong number. A legacy row storing
    # ``'0'`` (measured zero) still parses to ``Decimal("0")`` and is
    # byte-identical to before; an explicit ``None``, ``''`` (the PR-C1
    # unmeasured sentinel), and unparseable text all read as unmeasured. A SQL
    # NULL column never reaches here as unmeasured — both storage read seams
    # normalise it to the legacy ``Decimal("0")`` (c65fa094c), since a NULL
    # predates the sentinel and is a legacy row, not an unmeasured claim.
    deposits: Decimal | None = Decimal("0")
    withdrawals: Decimal | None = Decimal("0")
    if portfolio_metrics is not None:
        deposits = _to_decimal_or_none(getattr(portfolio_metrics, "deposits_usd", "0"))
        withdrawals = _to_decimal_or_none(getattr(portfolio_metrics, "withdrawals_usd", "0"))
        pnl.age_days = _strategy_age_days(portfolio_metrics)
        pnl.age_days_exact = strategy_age_days_exact(portfolio_metrics)

    if deposits is None or withdrawals is None:
        # No flow adjustment at all — adding a half-measured flow would be as
        # dishonest as adding zero. ``deployed_usd`` stays the wallet anchor /
        # initial baseline and remains VISIBLE (it is measured); only the three
        # metrics derived from the flow-adjusted baseline are suppressed below.
        pnl.capital_flows_unmeasured = True
        net_flow = Decimal("0")
    else:
        net_flow = deposits - withdrawals

    if wallet_anchored is not None:
        pnl.deployed_usd = wallet_anchored + net_flow
    elif portfolio_metrics is not None:
        initial = _to_decimal(getattr(portfolio_metrics, "initial_value_usd", "0"))
        pnl.deployed_usd = initial + net_flow

    # VIB-3884 (Codex F1): the snapshot's ``total_value_usd`` column is
    # *positive position values only* (per VIB-3614 / portfolio_valuer.py
    # 241-247) — undeployed wallet capital lives in ``available_cash_usd``.
    # The Senior-Quant audience reads "NAV now" as wallet net asset value
    # — what the strategy would mark to market right now if you had to
    # report to an LP. That's ``total_value_usd − debt + available_cash_usd``,
    # computed through the ONE shared helper so this tile, the NAV-history chart,
    # and the drawdown series are provably one definition (VIB-5942).
    wallet_nav = wallet_nav_usd(_raw_total_value_usd, _debt_to_net, pnl.available_cash_usd)
    pnl.nav_usd = wallet_nav
    if pnl.capital_flows_unmeasured:
        # VIB-5866: lifetime PnL is ``nav − (baseline + deposits − withdrawals)``.
        # With the flows unmeasured the baseline is unknown, so all three
        # flow-derived metrics are SUPPRESSED (None) rather than computed off a
        # fabricated zero flow — that reads every external deposit as profit.
        # ``deployed_usd`` / ``nav_usd`` / positions / cash stay visible: they
        # are measured, and hiding them would degrade a working card.
        pnl.lifetime_pnl_usd = None
        pnl.lifetime_pnl_pct = None
        pnl.net_apr_pct = None
    else:
        pnl.lifetime_pnl_usd = wallet_nav - pnl.deployed_usd
        if pnl.deployed_usd > Decimal("0"):
            pnl.lifetime_pnl_pct = (pnl.lifetime_pnl_usd / pnl.deployed_usd) * Decimal("100")
        pnl.net_apr_pct = _annualised_return(pnl.deployed_usd, wallet_nav, pnl.age_days_exact)

    # VIB-3914: Open exposure must read from accounting_events when the
    # snapshot writer has not summed open-position cost basis (the
    # production failure mode VIB-3883/VIB-3894 were meant to fix). The
    # snapshot value wins when populated; otherwise we reconstruct from
    # the same accounting events the cost stack is computed from.
    if pnl.deployed_capital_usd <= Decimal("0"):
        reconstructed = _open_position_cost_basis(accounting_events)
        if reconstructed > Decimal("0"):
            pnl.deployed_capital_usd = reconstructed
            # VIB-6308: the writer's coverage stamp measures the POSITION LEGS.
            # It cannot know that this branch just supplied a basis from a
            # different source — the accounting events. Once it has, the cost
            # side is no longer missing, so suppressing Strategy PnL would hide
            # a number the reader can actually compute.
            #
            # Measured on the 20260801 real-money batch: this branch fires on
            # 63/63 traderjoe-lp-avax snapshots (leg basis absent, accounting
            # basis $2.48, tile rendered -$0.07) and on only 2/64
            # pcs-aave-carry-bsc snapshots (leg basis present at $3.82) — so
            # clearing here restores the honest small number and leaves the
            # +42% phantom on the carry suppressed, which is the whole point.
            #
            # Deliberately narrow: this does NOT claim the reconstruction covers
            # exactly the legs that reached NAV.
            #
            # KNOWN RESIDUAL, stated in BOTH directions (VIB-6351). On a
            # MULTI-LEG snapshot this treats "a reconstruction exists" as "every
            # NAV leg is covered", and it does not. Two unbacked legs ($2.41 +
            # $5.00) with one LP_OPEN carrying $2.48 clears the flag and renders
            # +$4.93 where leaving it set renders "-" -- the same unbacked-mark-
            # as-profit shape VIB-6308 exists to kill. It is NOT a regression
            # (pre-VIB-6308 rendered that too, and the carry stays suppressed for
            # two independent reasons: its column is only <= 0 on post-teardown
            # zero-NAV rows, and _open_position_cost_basis returns 0 for it), but
            # it is a hole this clearing re-opens rather than merely a loss of
            # information. Gating on the stamp's ``legs_in_nav == 1`` would fence
            # it; the general fix is provenance-aware coverage in VIB-6351.
            pnl.cost_basis_partial = False
            # VIB-5339/VIB-5738: only synthesize a count when no dust-aware
            # DISPLAY count was available (dict/legacy snapshot). If a typed
            # snapshot already resolved 0 (torn-down / dust-only wallet), a
            # lingering historical cost basis must NOT flip the badge back to
            # "1 open position(s)" — that is the exact display lie the dust-aware
            # count fixes.
            if pnl.open_position_count == 0 and _display_count is None:
                pnl.open_position_count = 1

    # VIB-4983 follow-up: net the BORROW cost basis out of the Open-cost-basis
    # tile, the sibling of the NAV netting above. The writer sums
    # ``deployed_capital_usd`` as Σ abs(cost_basis_usd) (portfolio_valuer.py:702-705)
    # — counting the BORROW cost as a *positive* asset — while the accounting-events
    # reconstruction fallback above sums collateral cost only. Rather than guess
    # which gross convention produced the number and subtract 1x or 2x, replace it
    # with the net equity cost computed DIRECTLY from the position legs
    # (collateral cost − borrow cost == ``_net_cost``). Without this the Strategy-PnL
    # tile (open NAV − cost basis, _detail_header.py:_strategy_pnl_usd) reads
    # netted-NAV minus gross-cost = a phantom −debt loss on a flat position. Only
    # applied when a debt leg exists so non-leveraged snapshots keep the writer's
    # column byte-identically. Clamped at 0 so corrupt data can never surface a
    # negative basis (which would invert APR / Strategy-PnL sign).
    if _debt_cost_to_net > Decimal("0"):
        pnl.deployed_capital_usd = max(Decimal("0"), _net_cost)

    # VIB-5118: prefer the lifetime drawdown (computed by the gateway over the
    # FULL snapshot history via ``get_nav_series``) when supplied. ``snapshots``
    # here is the recent 168-row window (VIB-5026), so ``_drawdowns(snapshots)``
    # only sees ~14h of history — a lifetime peak/drawdown older than that is
    # understated. When the full series is unavailable (no I/O caller, backend
    # failure), fall back to the recent-window drawdown so behaviour is the
    # documented graceful degrade, never an exception. List/legacy callers that
    # pass no ``lifetime_drawdown`` are byte-for-byte unchanged.
    if lifetime_drawdown is not None:
        pnl.max_drawdown_pct, pnl.current_drawdown_pct = lifetime_drawdown
    else:
        pnl.max_drawdown_pct, pnl.current_drawdown_pct = _drawdowns(snapshots)

    _apply_primary_risk_gauge(pnl, position_summary, accounting_events)

    return pnl


def _apply_primary_risk_gauge(
    pnl: PnLSummary,
    position_summary: Any | None,
    accounting_events: list[dict[str, Any]],
) -> None:
    """Populate the ``primary_risk_*`` tile on ``pnl`` in place.

    Extracted from :func:`compute_pnl_summary` (VIB-4983) — behaviour is
    byte-identical; the block writes only the ``primary_risk_*`` fields and the
    extraction keeps the top-level builder's complexity within the CRAP budget.

    Primary risk gauge — pull from PositionSummary if provided. Contract: never
    paper over a missing field (Senior-Quant audit). When the underlying value
    is None we surface "unknown" with a neutral colour rather than defaulting to
    a red/green that misleads the operator into a money decision based on a
    bool() coercion.
    """
    if position_summary is not None:
        if getattr(position_summary, "lp_positions", None) and len(position_summary.lp_positions) > 0:
            in_range = position_summary.lp_positions[0].in_range
            pnl.primary_risk_kind = "lp"
            pnl.primary_risk_label = "Range"
            if in_range is None:
                pnl.primary_risk_value = "in-range pending"
                pnl.primary_risk_color = "neutral"
            else:
                pnl.primary_risk_value = "in-range YES" if in_range else "in-range NO"
                pnl.primary_risk_color = "green" if in_range else "red"
        elif getattr(position_summary, "health_factor", None) is not None:
            hf = position_summary.health_factor
            pnl.primary_risk_kind = "lending"
            pnl.primary_risk_label = "Health Factor"
            pnl.primary_risk_value = f"{hf:.2f}" if hf > 0 else "no debt"
            # VIB-3924 — colour ladder for lending health factor. A neutral
            # HF tile lets an operator drift toward liquidation without
            # warning; pre-VIB-3924 the dashboard rendered HF=1.05 in the
            # same colour as HF=3.00. Thresholds use the protocol-blind
            # default ladder; absolute values vary per protocol/asset LTV
            # so the tooltip carries the "thresholds vary by protocol"
            # caveat (VIB-3926). Lending tile lives behind the BETA-BADGE
            # banner per VIB-3929 — operators see "beta accounting" before
            # they read the HF tile.
            if hf <= 0:
                pnl.primary_risk_color = "neutral"  # no debt
            elif hf >= Decimal("1.5"):
                pnl.primary_risk_color = "green"
            elif hf >= Decimal("1.2"):
                pnl.primary_risk_color = "yellow"
            else:
                pnl.primary_risk_color = "red"
        elif getattr(position_summary, "leverage", None) is not None:
            lev = position_summary.leverage
            pnl.primary_risk_kind = "perp"
            pnl.primary_risk_label = "Leverage"
            pnl.primary_risk_value = f"{lev:.2f}×"
            # Same protocol-blindness argument: leverage thresholds depend on
            # market liquidation params. Surface raw leverage; let Tape /
            # primary-risk-detail tile carry the protocol context.
            pnl.primary_risk_color = "neutral"

    # VIB-3914: Fallback when ``position_summary`` is empty (snapshot's
    # positions_json never populated by the writer) but ``accounting_events``
    # show open positions. Prevents the screen from rendering "Positions
    # N/A" in defiance of an open LP / SUPPLY / PERP event on disk.
    if pnl.primary_risk_kind == "none" and pnl.deployed_capital_usd > Decimal("0"):
        primitive_now = _detect_primitive(accounting_events)
        if primitive_now == "lp":
            pnl.primary_risk_kind = "lp"
            pnl.primary_risk_label = "Range"
            pnl.primary_risk_value = "in-range pending"
            pnl.primary_risk_color = "neutral"
        elif primitive_now == "lending":
            pnl.primary_risk_kind = "lending"
            pnl.primary_risk_label = "Health Factor"
            pnl.primary_risk_value = "unknown"
            pnl.primary_risk_color = "neutral"
        elif primitive_now == "perp":
            pnl.primary_risk_kind = "perp"
            pnl.primary_risk_label = "Leverage"
            pnl.primary_risk_value = "unknown"
            pnl.primary_risk_color = "neutral"


def build_quant_header(
    *,
    portfolio_metrics: Any,
    snapshots: list[Any],
    ledger_entries: list[Any] | LedgerQuantStats,
    accounting_events: list[dict[str, Any]],
    position_summary: Any | None = None,
) -> QuantHeader:
    """DEPRECATED (VIB-3969): legacy bundle composer.

    The gateway-side ``GetQuantHeader`` RPC is gone — the operator
    console reads through the focused trio
    (``GetPnLSummary`` / ``GetCostStack`` / ``GetAuditPosture``).
    This Python composer is retained only for the existing
    ``tests/unit/dashboard/test_quant_header_nav_vib3884.py`` /
    ``test_quant_aggregations.py`` regression suites that pin the
    composed shape; new consumers should call ``compute_pnl_summary`` /
    ``compute_cost_stack`` / ``compute_audit_trail`` /
    ``compute_reconciliation`` / ``evaluate_posture`` directly.
    """
    pnl = compute_pnl_summary(
        portfolio_metrics=portfolio_metrics,
        snapshots=snapshots,
        ledger_entries=ledger_entries,
        accounting_events=accounting_events,
        position_summary=position_summary,
    )
    cost_stack = compute_cost_stack(ledger_entries, accounting_events)
    audit_trail = compute_audit_trail(ledger_entries, accounting_events)
    reconciliation = compute_reconciliation(
        initial_value_usd=pnl.deployed_usd,
        nav_usd=pnl.nav_usd,
        cost_stack=cost_stack,
        accounting_events=accounting_events,
    )
    primitive = _detect_primitive(accounting_events)
    posture = evaluate_posture(
        primitive=primitive,
        ledger_entries=ledger_entries,
        accounting_events=accounting_events,
        snapshots=snapshots,
        audit=audit_trail,
        reconciliation=reconciliation,
        portfolio_metrics=portfolio_metrics,
    )
    return QuantHeader(
        deployed_usd=pnl.deployed_usd,
        nav_usd=pnl.nav_usd,
        lifetime_pnl_usd=pnl.lifetime_pnl_usd,
        lifetime_pnl_pct=pnl.lifetime_pnl_pct,
        net_apr_pct=pnl.net_apr_pct,
        max_drawdown_pct=pnl.max_drawdown_pct,
        current_drawdown_pct=pnl.current_drawdown_pct,
        value_confidence=pnl.value_confidence,
        age_days=pnl.age_days,
        age_days_exact=pnl.age_days_exact,
        capital_flows_unmeasured=pnl.capital_flows_unmeasured,
        deployed_capital_usd=pnl.deployed_capital_usd,
        available_cash_usd=pnl.available_cash_usd,
        open_position_count=pnl.open_position_count,
        primary_risk_label=pnl.primary_risk_label,
        primary_risk_value=pnl.primary_risk_value,
        primary_risk_color=pnl.primary_risk_color,
        primary_risk_kind=pnl.primary_risk_kind,
        cost_stack=cost_stack,
        reconciliation=reconciliation,
        audit_trail=audit_trail,
        posture=posture,
    )
