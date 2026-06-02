"""CLI command for strategy PnL breakdown (VIB-3206, VIB-3427).

Provides ``almanak strat pnl -s <deployment_id>`` — a strategy-class-aware PnL
view that reads persisted accounting data (PortfolioMetrics, LedgerEntry rows,
PositionEvent rows, typed AccountingEvents, PortfolioSnapshot) from the local
SQLite state store and prints a breakdown.

Detects strategy class from event store and renders only relevant sections
(LP economics, lending carry, Pendle yield). Includes a Data Quality section
surfacing any UNAVAILABLE confidence records.

No gateway call is made — this is a read-only report against the state DB
written by the strategy runner.

Usage:
    almanak strat pnl -s <deployment_id>
    almanak strat pnl -s <deployment_id> --json
    almanak strat pnl -s <deployment_id> --db ./custom_state.db
"""

from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import click

from almanak.framework.accounting.lending_nav import compute_lending_nav
from almanak.framework.accounting.reporting import (
    build_data_quality,
    build_lending_report,
    build_lp_report,
    build_pendle_report,
    load_accounting_data,
)
from almanak.framework.accounting.reporting.leveraged_lending import (
    LeveragedLendingVerdict,
    detect_leveraged_lending,
)
from almanak.framework.accounting.reporting.render_json import (
    data_quality_to_dict,
    lending_section_to_dict,
    lp_section_to_dict,
    pendle_section_to_dict,
)
from almanak.framework.accounting.reporting.render_text import (
    render_data_quality_section,
    render_lending_section,
    render_lp_section,
    render_pendle_section,
)
from almanak.framework.accounting.reporting.swap_class_fallback import (
    detect_stale_post_teardown_snapshot,
)

# Stablecoin symbols used to convert amount_in/amount_out -> USD notional
# when explicit USD fields are absent from LedgerEntry (pre-VIB-3204).
_STABLE_SYMBOLS: frozenset[str] = frozenset(
    {
        "USDC",
        "USDC.E",
        "USDBC",
        "USDT",
        "USDT.E",
        "DAI",
        "DAI.E",
        "FRAX",
        "LUSD",
        "USDE",
        "USDS",
        "SUSDE",
        "USDD",
        "TUSD",
    }
)

_MISSING = "—"  # em dash: signals "not yet available"


# ---------------------------------------------------------------------------
# Data-loading helpers
# ---------------------------------------------------------------------------


def _default_db_path() -> str:
    """Resolve the canonical local DB path (VIB-3761).

    Hosted mode (``ALMANAK_IS_HOSTED`` set) has no local DB; ``almanak strat pnl``
    is a local-only command, so callers in hosted mode should not be
    invoking this.
    """
    from almanak.framework.local_paths import LocalPathError, local_db_path

    try:
        return str(local_db_path())
    except LocalPathError:
        # Caller can pass --db explicitly; we return a sentinel that
        # fails fast if accidentally used in hosted mode.
        return ":hosted-mode-no-sqlite-path:"


# ---------------------------------------------------------------------------
# Computation
# ---------------------------------------------------------------------------


def _dec(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    """Safe-convert a value to Decimal, returning ``default`` on failure/empty."""
    if value is None or value == "":
        return default
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return default


_BASE58_FORBIDDEN = frozenset({"0", "O", "I", "l"})


def _looks_like_address(s: str) -> bool:
    """Whether a stripped token field looks like an address, not a symbol.

    Covers EVM (``0x`` + 40 hex) and Solana (32-44 char base58) — the two
    chain families whose addresses can appear in ``LedgerEntry.token_in``.
    """
    if not s:
        return False
    if s.startswith("0x") and len(s) == 42:
        return True
    # Solana base58 alphabet excludes 0, O, I, l. CodeRabbit audit fix:
    # check the ENTIRE string against the forbidden set rather than only
    # the first character (prior version's ``"0" not in s[:1]`` was a
    # false-positive magnet — e.g. it let ``"So11111…"`` pass trivially
    # but also accepted symbols like ``"USDC.E"`` that happen to fit the
    # length bounds). ``_resolve_symbol`` still falls back on resolver
    # failure, so a loose-positive here is recoverable, but the tighter
    # check reduces unnecessary resolver calls on obvious symbols.
    if 32 <= len(s) <= 44 and not s.isdigit() and not (set(s) & _BASE58_FORBIDDEN):
        return True
    return False


def _resolve_symbol(token: str, chain: str = "") -> str:
    """Resolve a LedgerEntry token field to an uppercase symbol.

    Handles both shapes that the runner may have persisted (VIB-3206 comment):

    - **Symbol** (``"USDC"``, ``"ETH"``): upper-cased and returned as-is.
    - **Address** (``"0xaf88..."``, ``"So11..."``): resolved through the
      unified ``get_token_resolver()`` so that stablecoin detection works
      even when the runner serialised addresses instead of symbols.

    Chain context is required to resolve an address; without it, or when
    the resolver raises (unknown token / resolver unavailable / offline),
    the raw upper-cased string is returned so the caller can still apply
    the stablecoin heuristic when the input happens to be a recognisable
    symbol. This is a best-effort helper — callers MUST be safe against
    an unresolved result (``_amount_in_usd`` already is; it returns
    ``None`` when neither leg matches ``_STABLE_SYMBOLS``).

    Any ``chain:addr`` style prefix (``"arbitrum:0x..."``) is stripped
    before resolution so downstream code doesn't need to care.
    """
    if not token:
        return ""
    s = token.strip().rsplit(":", 1)[-1]
    if not s:
        return ""
    upper = s.upper()

    if not _looks_like_address(s):
        return upper

    if not chain:
        return upper  # no chain context, caller gets the raw form

    try:
        from almanak.framework.data.tokens import get_token_resolver

        resolved = get_token_resolver().resolve(s, chain, log_errors=False, skip_gateway=True)
    except Exception:
        # Resolver unavailable, token unknown, or address invalid — fall back
        # to the raw upper form. Stablecoin heuristic will miss; that's the
        # same outcome as before this fix, so no regression.
        return upper

    sym = getattr(resolved, "symbol", "") or ""
    return sym.strip().upper() or upper


# Intent types whose ``transaction_ledger.amount_in`` / ``amount_out`` columns
# carry **raw on-chain integer** amounts (smallest unit). The ledger writer for
# these intents does not scale by decimals (see ``observability/ledger.py``
# ``_extract_from_lp_open``); read-side consumers MUST scale via the token's
# decimals before treating the value as human-form. Adding a new intent type
# whose writer emits raw-wei requires an entry here — otherwise the read side
# will treat the raw integer as a human number and overstate by 10^decimals.
#
# Intent types NOT listed here (SWAP, LP_CLOSE, LP_COLLECT_FEES, REPAY,
# WITHDRAW, ...) flow through ``SwapAmounts``/intent-attribute fallbacks that
# store amounts as the human Decimal (string form). Those values do not need
# decimals scaling.
_RAW_WEI_INTENT_TYPES: frozenset[str] = frozenset({"LP_OPEN"})


def _human_amount(
    amount_str: Any,
    token: str,
    chain: str,
    intent_type: str,
) -> Decimal | None:
    """Convert a ledger ``amount_in`` / ``amount_out`` string to a human Decimal.

    Honors Empty != Zero (``AGENTS.md`` §Accounting):

    - ``None`` / ``""`` → ``None`` (unmeasured; caller must not substitute zero).
    - ``"0"`` → ``Decimal("0")`` (measured zero; counts as a real value).

    For intent types whose writer emits raw-wei (``LP_OPEN`` today; see
    ``_RAW_WEI_INTENT_TYPES``), the integer string is scaled to human form via
    the token's decimals — resolved through :func:`get_token_resolver`. The
    resolver NEVER defaults to 18 decimals; an unresolvable token returns
    ``None`` so the caller skips the row rather than silently writing an 18-
    order-of-magnitude error.

    For all other intent types the value is already human-form (per the
    ``SwapAmounts`` writer in ``observability/ledger.py``) and is returned
    unchanged.

    Defensive disambiguation for ``LP_OPEN`` (two writer paths exist in
    ``observability/ledger.py:_extract_from_lp_open``):

    1. **Primary** — ``result.extracted_data["lp_open_data"]`` carries the
       on-chain ``amount0`` / ``amount1`` as raw integers (smallest unit).
       These need decimals scaling (the W1-3 bug case).
    2. **Fallback** — when the receipt parser did NOT populate ``lp_open_data``,
       the writer stores ``intent.amount0`` / ``intent.amount1`` instead, which
       are user-supplied human-form ``Decimal`` values stringified as e.g.
       ``"100"`` for a whole-number deposit. These must NOT be scaled.

    We therefore disambiguate using three checks (any one means "human form"):

    - String contains ``.`` or ``e`` (already-decimal notation).
    - Value is a small integer (``< 10^6``): a raw stablecoin amount of even
      0.000001 unit equals at least 1 raw unit, so any meaningful raw stablecoin
      value is >= 10^6. WETH/ETH and other 18-dp tokens are >= 10^15. Human
      LP-deposit intent fallbacks (``Decimal("100")``, ``Decimal("1000")``,
      etc.) sit well below this threshold and stay un-scaled.
    - The token is unresolvable: skip the row (no 18-decimal default).

    The 10^6 threshold matches the smallest decimals of any ERC20 we ship
    (USDC / USDT = 6 dp). Tokens with smaller decimals would need this
    threshold lowered, but no production token has fewer than 6.
    """
    # Empty != Zero gate (AGENTS.md §Accounting).
    if amount_str is None:
        return None
    s = str(amount_str).strip()
    if s == "":
        return None

    try:
        amount = Decimal(s)
    except (InvalidOperation, ValueError, TypeError):
        return None

    # Non-raw-wei intents: amount is already human-form (Decimal-string).
    if intent_type not in _RAW_WEI_INTENT_TYPES:
        return amount

    # LP_OPEN (and any future raw-wei intent): treat as human-form when the
    # string carries a decimal point / scientific notation already.
    if "." in s or "e" in s.lower():
        return amount

    # Magnitude-based human-fallback disambiguation (see docstring): a small
    # integer LP_OPEN amount is the writer's intent-amount fallback path,
    # not the raw-wei primary path. Treat it as human-form so a 100 USDC
    # intent-fallback reports as $100, not $0.0001.
    if abs(amount) < Decimal(10**6):
        return amount

    if not token or not chain:
        # No way to resolve decimals safely — fail-fast, don't substitute.
        return None

    # Strip any ``chain:address`` prefix the writer may have persisted so
    # the resolver receives a canonical token form (matches the strip that
    # ``_resolve_symbol`` does for the stablecoin-detection path).
    resolver_token = token.strip().rsplit(":", 1)[-1]

    try:
        from almanak.framework.data.tokens import get_token_resolver

        resolved = get_token_resolver().resolve(resolver_token, chain, log_errors=False, skip_gateway=True)
    except Exception:
        # Token unresolvable: refuse to guess decimals (no 18 default).
        return None

    decimals = getattr(resolved, "decimals", None)
    if decimals is None or decimals < 0:
        return None

    return amount / Decimal(10**decimals)


def _amount_in_usd(entry: Any) -> Decimal | None:
    """Best-effort USD notional for a SWAP / LP_OPEN / LP_CLOSE ledger entry.

    Routes both legs through :func:`_human_amount` (decimals-aware) so the
    stablecoin-side heuristic works uniformly across:

    - **SWAP**: amounts already human-form. Stable side amount = USD.
    - **LP_OPEN**: amounts are raw on-chain integers (smallest unit). Scaled
      via ``get_token_resolver()`` decimals before applying the stable-side
      heuristic — fixes the W1-3 bug where raw 6-dp USDC (``1585552``) was
      read as ``$1,585,552`` and inflated ``avg_trade_size_usd`` to ~$1.26M.
    - **LP_CLOSE** / **LP_COLLECT_FEES**: amounts flow through the receipt
      parser's ``SwapAmounts`` (human-form). Pass through ``_human_amount``
      unchanged but covered for defensive symmetry — any future regression
      that puts LP_CLOSE on a raw-wei writer path stays self-correcting.

    For non-trade intent types (``SUPPLY``, ``BORROW``, ``REPAY``,
    ``WITHDRAW``, ``PERP_*``, etc.) the notional concept is meaningful in
    different units (supplied collateral, borrowed principal, position
    notional) and does not belong in the swap-style avg-trade-size metric;
    we return ``None`` so the caller skips the row.

    Stable-side heuristic: if either leg resolves to a known stablecoin
    (symbol- or address-form, via :class:`TokenResolver`), that leg's human
    amount IS the USD notional (within the stablecoin's USD peg). Returns
    ``None`` when neither leg is a stablecoin — both-sides-volatile pools
    (WETH/WBTC, etc.) cannot be valued without a price oracle, and
    ``strat pnl`` is a read-only local CLI with no network egress.

    Empty != Zero (``AGENTS.md`` §Accounting): an unmeasured ``amount_in =
    None`` / ``""`` propagates as ``None`` (caller skips), while a measured
    ``"0"`` returns ``Decimal("0")`` (counts as a real value).

    VIB-3204 will add an explicit ``amount_in_usd`` column — this heuristic
    is the interim path until the writer emits the canonical USD value.
    """
    intent_type = (getattr(entry, "intent_type", "") or "").upper()

    # Notional only applies to trade-style intents. Lending / perp / bridge
    # rows carry amounts whose USD interpretation belongs in those
    # category-specific reports, not the generic avg-trade-size metric.
    if intent_type not in ("SWAP", "LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"):
        return None

    chain = getattr(entry, "chain", "") or ""
    raw_t_in = getattr(entry, "token_in", "") or ""
    raw_t_out = getattr(entry, "token_out", "") or ""
    t_in = _resolve_symbol(raw_t_in, chain)
    t_out = _resolve_symbol(raw_t_out, chain)
    a_in = _human_amount(getattr(entry, "amount_in", None), raw_t_in, chain, intent_type)
    a_out = _human_amount(getattr(entry, "amount_out", None), raw_t_out, chain, intent_type)

    # Stable-side heuristic, Empty != Zero aware: only the leg whose human
    # amount is a measured value (not None) AND > 0 contributes. A measured
    # zero (Decimal("0")) on the stable side adds no notional and falls
    # through to the next leg / None.
    if t_in in _STABLE_SYMBOLS and a_in is not None and a_in > 0:
        return a_in
    if t_out in _STABLE_SYMBOLS and a_out is not None and a_out > 0:
        return a_out
    return None


def _aggregate_protocol_fees(position_events: list) -> Decimal | None:
    """Sum measured ``protocol_fees_usd`` across position events (VIB-4846 T6).

    Honors Empty≠Zero (``AGENTS.md`` §Accounting): a ``""``/missing field is
    *unmeasured* and contributes nothing; ``"0"`` is a *measured zero* and
    counts. Returns ``None`` when NO event carries a measured value — so the
    top line reads "—" (unmeasured) rather than a misleading ``$0.00``. This
    mirrors ``pnl_attributor._protocol_fee_or_none`` (the per-event Empty≠Zero
    parse) and the ``lp_report.py`` aggregation (``+= _dec0(...)``), but at the
    strategy level and preserving the unmeasured/zero distinction at the total.
    """
    total = Decimal("0")
    any_measured = False
    for event in position_events:
        raw = event.get("protocol_fees_usd")
        if raw is None or raw == "":
            continue  # unmeasured — do not substitute zero
        try:
            total += Decimal(str(raw))
        except (InvalidOperation, ValueError, TypeError):
            # Malformed → treat as unmeasured (matches pnl_attributor).
            continue
        any_measured = True
    return total if any_measured else None


def _pnl_from_attribution(event: dict) -> Decimal | None:
    """Parse realized PnL from a PositionEvent's ``attribution_json`` field.

    Returns ``None`` when the event has no attribution (i.e. hasn't been
    through PnLAttributor yet) so callers can distinguish "zero PnL" from
    "unknown PnL".
    """
    raw = event.get("attribution_json") or ""
    if not raw or raw == "{}":
        return None
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(parsed, dict):
        return None
    if "net_pnl_usd" not in parsed:
        return None
    try:
        return Decimal(str(parsed["net_pnl_usd"]))
    except (InvalidOperation, ValueError, TypeError):
        return None


@dataclass
class PnLBreakdown:
    """Computed PnL breakdown suitable for rendering to text or JSON.

    ``None`` values are surfaced as ``_MISSING`` placeholders in text output
    and as JSON ``null`` in machine-readable output. This distinguishes
    "upstream feature not wired yet" (protocol fees, IL) from "really zero".
    """

    deployment_id: str
    name: str = ""
    chain: str = ""
    wallet: str = ""

    # Core PnL fields
    gross_pnl_usd: Decimal | None = None
    gas_usd: Decimal | None = None
    # VIB-4846 (T6): rolled up from position_events.protocol_fees_usd — see
    # _aggregate_protocol_fees. None = no measured fee on any event (Empty≠Zero).
    protocol_fees_usd: Decimal | None = None
    slippage_usd: Decimal | None = None
    impermanent_loss_usd: Decimal | None = None  # VIB-3205 — placeholder for now
    net_pnl_usd: Decimal | None = None

    # VIB-4788: net strategy NAV — the live net asset value of the strategy's
    # positions, netting lending liabilities against collateral.  Defined as
    # ``snapshot.total_value_usd − gross_debt_value_usd`` (positive positions
    # minus borrowed liabilities).  For strategies with no BORROW positions
    # (LP / TA / swap) this equals ``total_value_usd`` exactly; for leveraged
    # lending (Looping) it subtracts the debt that ``total_value_usd``
    # (positive-position-scoped, VIB-3614) does not.  Additive: does NOT
    # change ``total_value_usd`` semantics.  None when no snapshot is
    # available (unmeasured, Empty≠Zero).
    net_strategy_nav_usd: Decimal | None = None

    # VIB-4846 (T5): gas-efficiency ratios. Derived, read-path only.
    #   avg_gas_per_trade_usd       = gas_usd / trade_count
    #   gas_as_pct_of_avg_trade_bps = (avg_gas_per_trade / avg_trade_size) * 10000
    #   total_friction_usd          = gas + slippage + protocol_fees
    # All None-safe: a None input leaves the derived field None (no zero
    # substitution, no div-by-zero).
    avg_gas_per_trade_usd: Decimal | None = None
    gas_as_pct_of_avg_trade_bps: Decimal | None = None
    total_friction_usd: Decimal | None = None

    # Trade stats
    win_rate: Decimal | None = None
    wins: int = 0
    closed_positions: int = 0
    # Closed positions that have PnL attribution — the true win_rate denominator.
    # ``closed_positions`` is the count of ALL closes (including those without
    # attribution); using it as the denominator when attribution is partial
    # would under-report the win rate displayed next to a percent computed
    # from a smaller set. Keep both so callers can see the gap.
    scored_closes: int = 0
    avg_trade_size_usd: Decimal | None = None
    trade_count: int = 0
    open_positions: int = 0
    # VIB-4846 (Codex review): True when unattributed closes exceed 25% of all
    # closes — the headline win rate is then too unreliable to present as a
    # single number. Surfaced in BOTH text (loud WARNING line) and JSON (this
    # flag) so machine consumers see the same caveat human readers do.
    high_unattributed_win_rate: bool = False

    # VIB-3493: LP rebalance attribution under the continuous strategy-level
    # model. When the strategy has no LP events these stay None / 0; otherwise
    # they surface the strategy-total LP gas (rebalance gas included) plus a
    # rebalance-cycle count so multi-rebalance strategies don't look
    # artificially cheap when only per-lifecycle attribution is shown.
    lp_total_gas_usd: Decimal | None = None
    lp_open_gas_usd: Decimal | None = None
    lp_close_gas_usd: Decimal | None = None
    lp_open_count: int = 0
    lp_close_count: int = 0
    lp_close_open_pairs: int = 0

    # Diagnostics
    warnings: list[str] = field(default_factory=list)

    # VIB-4907 / F4: headline suppression.  Set by the SWAP-class fallback
    # detector when the post-teardown snapshot is byte-identical to the
    # pre-teardown one but a successful SWAP ran between them — the
    # cache-staleness fingerprint that makes the gross/net/NAV numbers
    # misleading.  Renderers use ``headline_suppressed`` to decide whether
    # to display ``Headline PnL: unavailable`` in place of the gross/net
    # block.  ``headline_suppression_reason`` is a single-line plain-English
    # explanation suitable for the operator-facing render and the JSON
    # payload.  When ``headline_suppressed`` is ``False`` the reason is
    # ``None`` — Empty≠Zero (no detection ran or it returned negative).
    headline_suppressed: bool = False
    headline_suppression_reason: str | None = None

    # VIB-4975 (B-open): leveraged-lending headline override.  When the strategy
    # holds a LIVE leveraged-lending position (SUPPLY + BORROW legs in the
    # latest snapshot), the gross/net headline taken verbatim from
    # ``PortfolioMetrics`` over-reports because ``total_value_usd`` is
    # positive-position-scoped (VIB-3614): it counts the re-supplied *borrowed*
    # collateral but never nets the borrowed-principal liability.  When this
    # flag is set, ``gross_pnl_usd`` / ``net_pnl_usd`` have been re-derived from
    # the debt-netted lending NAV (``net_strategy_nav_usd`` − initial − flows)
    # instead, so leverage is not booked as profit.  The closed-state artifact
    # (collateral back in the wallet → NAV $0) is NOT solved here — that needs a
    # scoped wallet/cash baseline (VIB-4976) and falls under
    # ``headline_suppressed`` above.  ``headline_leverage_note`` is the
    # single-line operator-facing explanation; ``None`` when no override ran
    # (Empty≠Zero).
    headline_leverage_adjusted: bool = False
    headline_leverage_note: str | None = None

    def to_json_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-friendly dict (Decimal -> str, None preserved)."""

        def _maybe(v: Decimal | None) -> str | None:
            return None if v is None else str(v)

        return {
            "deployment_id": self.deployment_id,
            "name": self.name,
            "chain": self.chain,
            "wallet": self.wallet,
            "gross_pnl_usd": _maybe(self.gross_pnl_usd),
            "gas_usd": _maybe(self.gas_usd),
            "protocol_fees_usd": _maybe(self.protocol_fees_usd),
            "slippage_usd": _maybe(self.slippage_usd),
            "impermanent_loss_usd": _maybe(self.impermanent_loss_usd),
            "net_pnl_usd": _maybe(self.net_pnl_usd),
            "net_strategy_nav_usd": _maybe(self.net_strategy_nav_usd),
            "avg_gas_per_trade_usd": _maybe(self.avg_gas_per_trade_usd),
            "gas_as_pct_of_avg_trade_bps": _maybe(self.gas_as_pct_of_avg_trade_bps),
            "total_friction_usd": _maybe(self.total_friction_usd),
            "win_rate": _maybe(self.win_rate),
            "wins": self.wins,
            "closed_positions": self.closed_positions,
            "scored_closes": self.scored_closes,
            "avg_trade_size_usd": _maybe(self.avg_trade_size_usd),
            "trade_count": self.trade_count,
            "open_positions": self.open_positions,
            "high_unattributed_win_rate": self.high_unattributed_win_rate,
            "lp_total_gas_usd": _maybe(self.lp_total_gas_usd),
            "lp_open_gas_usd": _maybe(self.lp_open_gas_usd),
            "lp_close_gas_usd": _maybe(self.lp_close_gas_usd),
            "lp_open_count": self.lp_open_count,
            "lp_close_count": self.lp_close_count,
            "lp_close_open_pairs": self.lp_close_open_pairs,
            "warnings": list(self.warnings),
            "headline_suppressed": self.headline_suppressed,
            "headline_suppression_reason": self.headline_suppression_reason,
            "headline_leverage_adjusted": self.headline_leverage_adjusted,
            "headline_leverage_note": self.headline_leverage_note,
        }


# ---------------------------------------------------------------------------
# compute_pnl_breakdown — per-source aggregation steps
#
# `compute_pnl_breakdown` is the read-side PnL aggregator (blueprint 27 §4 /
# §3.6): it reads the canonical sources (``portfolio_metrics``,
# ``transaction_ledger``, ``position_events``) and writes a derived
# ``PnLBreakdown``. Each step below owns ONE source-block of the original
# procedure; behaviour (operations, ordering, Empty≠Zero semantics) is
# preserved verbatim — these are mechanical extractions, not redesigns.
# ---------------------------------------------------------------------------


def _populate_identity(
    breakdown: PnLBreakdown,
    deployment_id: str,
    snapshot: Any,
    ledger_entries: list,
) -> None:
    """Name / chain / wallet.

    Prefer the deployment_id carried on metrics, fall back to the
    caller-supplied value. Chain comes from the snapshot or ledger; wallet is
    not currently persisted in PortfolioMetrics so we leave it blank for now.
    """
    breakdown.name = deployment_id
    if snapshot is not None:
        breakdown.chain = getattr(snapshot, "chain", "") or ""
    if not breakdown.chain and ledger_entries:
        # Pick the most recent non-empty chain from ledger entries
        for entry in ledger_entries:
            chain_val = getattr(entry, "chain", "") or ""
            if chain_val:
                breakdown.chain = chain_val
                break


def _populate_strategy_nav(breakdown: PnLBreakdown, snapshot: Any) -> None:
    """Net strategy NAV (VIB-4788): positive position value minus lending debt.

    ``snapshot.total_value_usd`` is strategy-scoped to *positive* position
    values only (VIB-3614), so BORROW liabilities are excluded rather than
    netted.  Subtract the gross debt — ``compute_lending_nav`` sums the BORROW
    legs' ``abs(value_usd)`` — to recover the true net asset value.  A strategy
    with no BORROW positions has zero debt, so the NAV equals
    ``total_value_usd`` exactly (additive: ``total_value_usd`` is never
    mutated).  Leaves the field None (unmeasured, Empty≠Zero) when no snapshot
    is available or ``total_value_usd`` is missing.
    """
    if snapshot is None:
        return
    total = getattr(snapshot, "total_value_usd", None)
    if total is None:
        return
    try:
        nav = compute_lending_nav(snapshot)
    except Exception:  # pragma: no cover - defensive; compute_lending_nav is shape-tolerant
        return
    breakdown.net_strategy_nav_usd = _dec(total) - nav.gross_debt_value_usd


def _populate_gross_net_pnl(breakdown: PnLBreakdown, metrics: Any) -> None:
    """Gross / net PnL from PortfolioMetrics."""
    if metrics is not None:
        breakdown.gross_pnl_usd = _dec(metrics.pnl_before_gas)
        breakdown.net_pnl_usd = _dec(metrics.pnl_after_gas)
    else:
        breakdown.warnings.append(
            "No PortfolioMetrics row found — gross/net PnL unavailable. "
            "Run the strategy at least once so metrics are persisted."
        )


def _apply_open_leveraged_headline(breakdown: PnLBreakdown, metrics: Any, verdict: LeveragedLendingVerdict) -> None:
    """Re-derive the gross/net headline from the debt-netted lending NAV (B-open).

    For a LIVE leveraged-lending position the headline taken verbatim from
    ``PortfolioMetrics`` over-reports: ``pnl_before_gas`` uses
    ``total_value_usd``, which under VIB-3614 is positive-position-scoped — it
    counts the re-supplied *borrowed* collateral but never nets the
    borrowed-principal liability, so taking on leverage manufactures a phantom
    gain.  We swap ``total_value_usd`` for the **debt-netted lending NAV**
    (``net_lending_nav_usd`` = Σ SUPPLY − Σ |BORROW|) while keeping the SAME
    initial/flow baseline ``PortfolioMetrics`` uses:

        gross = net_lending_nav − initial_value_usd − deposits + withdrawals
        net   = gross − gas_spent_usd

    NB (Codex): the headline is PnL carried through the baseline, NOT the NAV
    itself.  On the ``insp4`` shape NAV $3.63 is the position *value*; the PnL
    is that value minus the $4 initial (≈ −$0.37 before gas), not +$1.19.

    No-ops (leaving the verbatim ``PortfolioMetrics`` headline) when metrics or
    the netted NAV are unmeasured — Empty≠Zero, never substitute a guess.
    Likewise when ``initial_value_usd`` is ``None`` (unmeasured baseline):
    deriving ``NAV − 0 − flows`` off a zero-defaulted baseline would emit a
    confident wrong PnL, so we skip the derivation rather than guess (Gemini
    review).  ``deposits`` / ``withdrawals`` / ``gas`` keep the measured-zero
    default — an absent flow legitimately means "no capital moved", and the
    verbatim ``PortfolioMetrics.pnl_before_gas`` it mirrors treats them the
    same way.
    """
    if metrics is None or verdict.net_lending_nav_usd is None:
        return
    # Empty≠Zero: an unmeasured initial baseline must NOT default to 0 — that
    # would book the entire NAV as PnL.  Leave the verbatim headline untouched.
    initial_raw = getattr(metrics, "initial_value_usd", None)
    if initial_raw is None:
        return
    net_nav = _dec(verdict.net_lending_nav_usd)
    initial = _dec(initial_raw)
    deposits = _dec(getattr(metrics, "deposits_usd", None))
    withdrawals = _dec(getattr(metrics, "withdrawals_usd", None))
    gas_spent = _dec(getattr(metrics, "gas_spent_usd", None))
    gross = net_nav - initial - deposits + withdrawals
    breakdown.gross_pnl_usd = gross
    breakdown.net_pnl_usd = gross - gas_spent
    breakdown.headline_leverage_adjusted = True
    breakdown.headline_leverage_note = (
        "leveraged-lending: gross/net derived from debt-netted lending NAV "
        f"({_fmt_money(net_nav)} = SUPPLY − BORROW) instead of the "
        "positive-position-scoped total_value_usd (VIB-3614), so re-supplied "
        "borrowed collateral is not booked as profit (VIB-4975). NB: this "
        "headline is lending-NAV-scoped (Σ SUPPLY − Σ BORROW); the 'Net "
        "strategy NAV' line is total-position-scoped (total_value_usd − "
        "BORROW) and the two diverge if the loop also holds a non-lending "
        "leg (e.g. LP)."
    )


def _apply_leveraged_lending_headline(
    breakdown: PnLBreakdown,
    metrics: Any,
    snapshot: Any,
    ledger_entries: list,
) -> None:
    """Apply the VIB-4975 leveraged-lending headline verdict.

    Non-leveraged strategies (no live BORROW position, no historical BORROW
    ledger entry) are left untouched — the verbatim ``PortfolioMetrics``
    headline stands (regression guard).

    * **open** — re-derive the headline from the debt-netted lending NAV
      (B-open) so leverage is not booked as profit.
    * **closed** — suppress the headline (B3): the collateral has returned to
      the wallet, the positive-position-scoped ``total_value_usd`` collapses to
      ~0, and the verbatim headline would read ≈ −initial (a false −100%).
      Recognising wallet-held value needs a scoped wallet/cash baseline
      (VIB-4976, design); until then an honest "unavailable" beats a confident
      wrong number.
    """
    verdict = detect_leveraged_lending(snapshot, ledger_entries)
    if not verdict.is_leveraged_lending:
        return
    if verdict.state == "open":
        _apply_open_leveraged_headline(breakdown, metrics, verdict)
    elif verdict.state == "closed":
        breakdown.headline_suppressed = True
        breakdown.headline_suppression_reason = verdict.reason


def _compute_gas(breakdown: PnLBreakdown, metrics: Any, ledger_entries: list) -> int:
    """Gas (sum across ledger). Returns ``measured_gas_count``.

    Track the count of rows that carry a MEASURED gas value (Empty≠Zero):
    this is the correct denominator for ``avg_gas_per_trade_usd`` so that
    rows whose gas was never measured do not dilute the average. ``""`` /
    missing = unmeasured (skipped); any present value counts.
    """
    gas_total = Decimal("0")
    measured_gas_count = 0
    for entry in ledger_entries:
        gas_val = getattr(entry, "gas_usd", "")
        if gas_val:
            gas_total += _dec(gas_val)
            measured_gas_count += 1
    if measured_gas_count > 0:
        breakdown.gas_usd = gas_total
    elif metrics is not None:
        # Fall back to the rolling gas_spent_usd on metrics (authoritative).
        # This is a strategy-total, not a per-row sum, so there is no
        # measured-row count to average over — leave it 0 so the per-trade
        # average stays unmeasured (None) rather than dividing the total by
        # trade_count (which includes rows with unmeasured gas).
        breakdown.gas_usd = _dec(metrics.gas_spent_usd)
    return measured_gas_count


def _compute_slippage(breakdown: PnLBreakdown, ledger_entries: list) -> None:
    """Slippage (sum over swap ledger entries)."""
    slippage_total = Decimal("0")
    any_slippage = False
    for entry in ledger_entries:
        if (getattr(entry, "intent_type", "") or "").upper() != "SWAP":
            continue
        slippage_bps = getattr(entry, "slippage_bps", None)
        if slippage_bps is None:
            continue
        notional = _amount_in_usd(entry)
        if notional is None or notional <= 0:
            continue
        slippage_total += (Decimal(str(slippage_bps)) / Decimal("10000")) * notional
        any_slippage = True
    if any_slippage:
        breakdown.slippage_usd = slippage_total


def _compute_trade_stats(breakdown: PnLBreakdown, ledger_entries: list) -> None:
    """Trade count / avg trade size.

    Empty != Zero (``AGENTS.md`` §Accounting): rows whose ``_amount_in_usd``
    returns ``None`` (unmeasured — unresolvable token, non-stable both legs,
    or non-trade intent type) are skipped without substituting zero. Rows
    whose notional is a measured ``Decimal("0")`` are also skipped from the
    average because a zero-notional row would drag the mean toward zero and
    misrepresent the size of actual trades. The denominator is therefore
    the count of POSITIVE-notional rows, mirroring the pre-W1-3 behaviour.
    """
    breakdown.trade_count = len(ledger_entries)
    if ledger_entries:
        notionals: list[Decimal] = []
        for entry in ledger_entries:
            notional = _amount_in_usd(entry)
            if notional is not None and notional > 0:
                notionals.append(notional)
        if notionals:
            breakdown.avg_trade_size_usd = sum(notionals, Decimal("0")) / Decimal(len(notionals))


def _latest_lifecycle_by_position(position_events: list) -> dict[str, dict[str, Any]]:
    """Map ``position_id`` -> ``{"closed", "pnl"}`` for its latest lifecycle event.

    `SQLiteStore.get_position_events()` returns rows newest-first. For each
    ``position_id`` we only care about the *latest* lifecycle event:

      - OPEN (first-ever) / OPEN (reopen after CLOSE) -> position is OPEN
      - CLOSE                                         -> position is CLOSED
      - UPDATE / COLLECT_FEES / ...                   -> preserves prior state

    The old implementation latched ``closed = True`` on any historical
    ``CLOSE`` regardless of later events, so a position re-opened under the
    same ``position_id`` (e.g. resumed strategy on a restart) would be
    miscounted as closed and its stale attribution would leak into the
    win-rate stat. We now take the first event we see per ``pid`` (= the
    newest, since rows are newest-first) and derive state from that.
    Only OPEN / CLOSE are lifecycle-defining. Everything else (SNAPSHOT,
    COLLECT_FEES, UPDATE, ...) preserves the prior state — so we walk
    past them to find the first lifecycle event per ``pid``.
    """
    _LIFECYCLE_TYPES = {"OPEN", "CLOSE"}

    positions_by_id: dict[str, dict[str, Any]] = {}
    for event in position_events:
        pid = event.get("position_id") or ""
        if not pid or pid in positions_by_id:
            continue
        event_type = (event.get("event_type") or "").upper()
        # CodeRabbit audit fix: non-lifecycle events (SNAPSHOT, COLLECT_FEES,
        # UPDATE, ...) must NOT flip the open/closed verdict. Skip them and
        # keep walking until we find an OPEN or CLOSE.
        if event_type not in _LIFECYCLE_TYPES:
            continue
        closed = event_type == "CLOSE"
        # Only attach PnL when the latest lifecycle event is a CLOSE — an
        # OPEN (reopen) supersedes any prior attribution.
        pnl = _pnl_from_attribution(event) if closed else None
        positions_by_id[pid] = {"closed": closed, "pnl": pnl}
    return positions_by_id


def _compute_lp_gas(breakdown: PnLBreakdown, position_events: list) -> None:
    """Strategy-level LP gas (VIB-3493, continuous-model attribution).

    Per-lifecycle ``attribute_lp`` only shows one OPEN+CLOSE pair per
    position; multi-rebalance strategies need the strategy-level total
    so the gas spent on every rebalance cycle is visible in one place.
    """
    from ..observability.pnl_attributor import attribute_lp_strategy

    lp_summary = attribute_lp_strategy(position_events)
    if lp_summary["open_count"] > 0 or lp_summary["close_count"] > 0:
        breakdown.lp_total_gas_usd = _dec(lp_summary["total_gas_usd"])
        breakdown.lp_open_gas_usd = _dec(lp_summary["open_gas_usd"])
        breakdown.lp_close_gas_usd = _dec(lp_summary["close_gas_usd"])
        breakdown.lp_open_count = lp_summary["open_count"]
        breakdown.lp_close_count = lp_summary["close_count"]
        breakdown.lp_close_open_pairs = lp_summary["close_open_pairs"]


def _compute_position_stats(breakdown: PnLBreakdown, position_events: list) -> None:
    """Closed / open / win rate from PositionEvent rows + LP gas + caveats."""
    positions_by_id = _latest_lifecycle_by_position(position_events)

    closed_count = sum(1 for p in positions_by_id.values() if p["closed"])
    open_count = sum(1 for p in positions_by_id.values() if not p["closed"])
    breakdown.closed_positions = closed_count
    breakdown.open_positions = open_count

    _compute_lp_gas(breakdown, position_events)

    wins = sum(1 for p in positions_by_id.values() if p["closed"] and p["pnl"] is not None and p["pnl"] > 0)
    # Only compute win rate if at least one closed position has attribution;
    # otherwise the number is meaningless.
    scored_closes = sum(1 for p in positions_by_id.values() if p["closed"] and p["pnl"] is not None)
    breakdown.scored_closes = scored_closes
    if scored_closes > 0:
        breakdown.wins = wins
        breakdown.win_rate = (Decimal(wins) / Decimal(scored_closes)) * Decimal("100")
    elif closed_count > 0:
        breakdown.warnings.append(
            "Closed positions exist but none have attribution_json — "
            "win rate unavailable. PnLAttributor may not have run yet."
        )

    # VIB-4846 (Codex review): flag when unattributed closes dominate (>25% of
    # all closes) so the win rate is too unreliable to trust as a single
    # number. Computed here (not only in render_text) so the JSON output
    # carries the same caveat for machine consumers.
    unattributed_closes = max(closed_count - scored_closes, 0)
    if closed_count > 0 and unattributed_closes > 0:
        breakdown.high_unattributed_win_rate = (Decimal(unattributed_closes) / Decimal(closed_count)) > Decimal("0.25")


def _compute_friction(breakdown: PnLBreakdown, position_events: list, measured_gas_count: int) -> None:
    """Protocol fees + gas-efficiency ratios + total friction (VIB-4846 T5/T6).

    All None-safe: each derived field stays None unless its inputs are
    measured. No zero substitution; no div-by-zero.
    """
    # --- Protocol fees roll-up (VIB-4846 T6) ------------------------------
    # Already-persisted column; mirror lp_report.py's aggregation but keep the
    # Empty≠Zero distinction at the strategy total (None when nothing measured).
    breakdown.protocol_fees_usd = _aggregate_protocol_fees(position_events)

    # Codex review (VIB-4846): average gas ONLY over rows that carry a
    # measured gas value — dividing the total by ``trade_count`` (which
    # includes rows with unmeasured gas) would dilute the per-trade average
    # downward (Empty≠Zero). ``measured_gas_count`` is the count of ledger
    # rows that contributed to ``gas_total``; it is 0 when ``gas_usd`` came
    # from the metrics fallback (a strategy-total with no per-row breakdown),
    # in which case the per-trade average stays unmeasured (None).
    if breakdown.gas_usd is not None and measured_gas_count > 0:
        breakdown.avg_gas_per_trade_usd = breakdown.gas_usd / Decimal(measured_gas_count)
    if (
        breakdown.avg_gas_per_trade_usd is not None
        and breakdown.avg_trade_size_usd is not None
        and breakdown.avg_trade_size_usd > 0
    ):
        breakdown.gas_as_pct_of_avg_trade_bps = (
            breakdown.avg_gas_per_trade_usd / breakdown.avg_trade_size_usd
        ) * Decimal("10000")
    # total_friction = gas + slippage + protocol_fees, summing only measured
    # components. None when every component is unmeasured (Empty≠Zero); a
    # measured-zero component (e.g. slippage_usd == 0) still counts.
    friction_parts = [
        p for p in (breakdown.gas_usd, breakdown.slippage_usd, breakdown.protocol_fees_usd) if p is not None
    ]
    if friction_parts:
        breakdown.total_friction_usd = sum(friction_parts, Decimal("0"))


def compute_pnl_breakdown(
    deployment_id: str,
    metrics: Any,
    ledger_entries: list,
    position_events: list,
    snapshot: Any,
) -> PnLBreakdown:
    """Compute a PnLBreakdown from already-loaded persisted rows.

    Split from the CLI command so tests can drive it with in-memory fixtures.
    Each step reads one canonical source and writes its slice of the
    breakdown (read-side aggregator, blueprint 27 §4); ordering is preserved
    because ``_compute_friction`` consumes values written by earlier steps
    (gas, slippage, avg trade size).
    """
    breakdown = PnLBreakdown(deployment_id=deployment_id)
    _populate_identity(breakdown, deployment_id, snapshot, ledger_entries)
    _populate_strategy_nav(breakdown, snapshot)
    _populate_gross_net_pnl(breakdown, metrics)
    # VIB-4975: leveraged-lending strategies need the headline scoped to the
    # debt-netted NAV (open) or suppressed (closed) — runs AFTER the verbatim
    # PortfolioMetrics headline so it can override / suppress it in place.
    _apply_leveraged_lending_headline(breakdown, metrics, snapshot, ledger_entries)
    measured_gas_count = _compute_gas(breakdown, metrics, ledger_entries)
    _compute_slippage(breakdown, ledger_entries)
    _compute_trade_stats(breakdown, ledger_entries)
    _compute_position_stats(breakdown, position_events)
    _compute_friction(breakdown, position_events, measured_gas_count)
    return breakdown


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _fmt_money(value: Decimal | None, width: int = 8) -> str:
    """Format a Decimal as ``$  12.34`` / ``-$  0.80`` / ``—`` placeholder."""
    if value is None:
        return _MISSING
    # Sign-aware formatting with a leading $ and right-justified number so
    # columns align under the default width.
    sign = "-" if value < 0 else " "
    body = f"${abs(value):>{width},.2f}"
    return f"{sign}{body}"


def _fmt_percent(value: Decimal | None) -> str:
    if value is None:
        return _MISSING
    return f"{int(value.quantize(Decimal('1')))}%"


def _fmt_bps(value: Decimal | None) -> str:
    """Format a basis-points value as ``12.5 bps (0.13%)`` / ``—`` placeholder."""
    if value is None:
        return _MISSING
    pct = value / Decimal("100")
    return f"{value.quantize(Decimal('0.1'))} bps ({pct.quantize(Decimal('0.01'))}%)"


def _append_nav_line(lines: list[str], breakdown: PnLBreakdown) -> None:
    """Append the VIB-4788 net-strategy-NAV line when it was measured.

    Rendered only when a snapshot was available (``None`` = unmeasured,
    Empty≠Zero).  For leveraged lending this is the headline that
    ``total_value_usd`` alone over-reports.
    """
    if breakdown.net_strategy_nav_usd is not None:
        lines.append(f"Net strategy NAV: {_fmt_money(breakdown.net_strategy_nav_usd)}")


def _append_headline_pnl(lines: list[str], breakdown: PnLBreakdown) -> None:
    """Append the Gross PnL line, OR the VIB-4907 suppression notice.

    When ``headline_suppressed`` is set (the SWAP-class fallback fired),
    the gross number cascades off a stale snapshot and is replaced by
    ``Headline PnL: unavailable`` plus the detection reason.  Friction
    components rendered below remain accurate and are kept either way.

    Parallel to :func:`_append_nav_line` — single concern, single conditional.
    """
    if breakdown.headline_suppressed:
        lines.append("Headline PnL:     unavailable")
        if breakdown.headline_suppression_reason:
            lines.append(f"  Reason: {breakdown.headline_suppression_reason}")
        return
    lines.append(f"Gross PnL:        {_fmt_money(breakdown.gross_pnl_usd)}")
    # VIB-4975 (B-open): when the headline was re-derived from the debt-netted
    # lending NAV, say so loudly so the operator knows the number is NOT the
    # verbatim PortfolioMetrics figure.
    if breakdown.headline_leverage_adjusted and breakdown.headline_leverage_note:
        lines.append(f"  Note: {breakdown.headline_leverage_note}")


def _append_net_pnl_and_nav(lines: list[str], breakdown: PnLBreakdown) -> None:
    """Append Net PnL + net-strategy-NAV unless headline is suppressed.

    Net PnL (``pnl_after_gas`` from ``PortfolioMetrics``) and NAV both
    cascade off the same snapshot-derived inputs as the gross headline,
    so the VIB-4907 suppression hides them together.  Friction components
    above stay visible regardless because they come from the ledger /
    position events, not from the snapshot.
    """
    if breakdown.headline_suppressed:
        return
    lines.append(f"Net PnL:          {_fmt_money(breakdown.net_pnl_usd)}")
    _append_nav_line(lines, breakdown)


def render_text(breakdown: PnLBreakdown) -> str:
    """Render a human-readable PnL breakdown."""
    lines: list[str] = []
    lines.append(f"Strategy: {breakdown.name or breakdown.deployment_id}")
    lines.append(f"Chain: {breakdown.chain or _MISSING}")
    lines.append(f"Wallet: {breakdown.wallet or _MISSING}")
    lines.append("")
    lines.append("PnL Breakdown")
    lines.append("-------------")
    # VIB-4907 / F4: under SWAP-class fallback the gross / net / NAV numbers
    # cascade off a stale post-teardown snapshot.  The friction components
    # below (gas, slippage, fees, IL) are read from the ledger and position
    # events and remain accurate, so we still surface them — only the
    # snapshot-derived headline is hidden behind ``unavailable``.
    _append_headline_pnl(lines, breakdown)
    lines.append(f"Gas costs:        {_fmt_money(-breakdown.gas_usd) if breakdown.gas_usd is not None else _MISSING}")
    if breakdown.protocol_fees_usd is None:
        # Codex review (VIB-4846): the protocol-fee roll-up IS implemented
        # (see _aggregate_protocol_fees). A None total means no event carried
        # a measured value (Empty≠Zero), so render it as "unmeasured" — NOT
        # "(not yet implemented)", which is stale and misleading.
        lines.append(f"Protocol fees:    {_MISSING}        (unmeasured)")
    else:
        lines.append(f"Protocol fees:    {_fmt_money(-breakdown.protocol_fees_usd)}")
    if breakdown.slippage_usd is None:
        lines.append(f"Slippage:         {_MISSING}")
    else:
        lines.append(f"Slippage:         {_fmt_money(-breakdown.slippage_usd)}")
    if breakdown.impermanent_loss_usd is None:
        lines.append(f"Impermanent loss: {_MISSING}        (not yet implemented)")
    else:
        lines.append(f"Impermanent loss: {_fmt_money(-breakdown.impermanent_loss_usd)}")
    _append_net_pnl_and_nav(lines, breakdown)
    lines.append("")

    # Win-rate row: the displayed ratio MUST use the same denominator that
    # produced the percent (``scored_closes`` — closed positions with PnL
    # attribution). Using ``closed_positions`` here when attribution is
    # partial would show e.g. ``66% (2/5 closed positions)`` even though
    # 2/5 = 40%. If there are unattributed closes, append a small note so
    # the gap is visible instead of silently hidden.
    scored = breakdown.scored_closes
    total_closed = breakdown.closed_positions
    unattributed = max(total_closed - scored, 0)
    # VIB-4846 (T7): when some closes lack attribution, the headline
    # ``scored_wins/scored_closes`` percent flatters the true rate (its
    # denominator excludes the unattributed closes). Show a conservative range
    # against the FULL closed set so the reader sees the worst/best bounds:
    #   low  = scored_wins / total_closed   (assume every unattributed lost)
    #   high = (scored_wins + unattributed) / total_closed  (assume all won)
    suffix = ""
    if unattributed > 0 and total_closed > 0:
        low = (Decimal(breakdown.wins) / Decimal(total_closed)) * Decimal("100")
        high = (Decimal(breakdown.wins + unattributed) / Decimal(total_closed)) * Decimal("100")
        suffix = f" [{unattributed} unattributed — conservative range {_fmt_percent(low)}–{_fmt_percent(high)}]"
    elif unattributed > 0:
        suffix = f" [{unattributed} unattributed]"
    if breakdown.win_rate is not None:
        lines.append(
            f"Win rate:         {_fmt_percent(breakdown.win_rate)} ({breakdown.wins}/{scored} scored closes){suffix}"
        )
    else:
        lines.append(f"Win rate:         {_MISSING} ({breakdown.wins}/{scored} scored closes){suffix}")
    # When unattributed closes dominate (>25% of all closes), the headline
    # percent is too unreliable to present as a single number — flag it loudly.
    # The threshold verdict is computed once in compute_pnl_breakdown
    # (``high_unattributed_win_rate``) so text and JSON stay in lockstep.
    if breakdown.high_unattributed_win_rate:
        lines.append(
            f"  WARNING: {unattributed}/{total_closed} closes are unattributed "
            f"(>25%); the headline win rate is unreliable — rely on the range above."
        )
    if breakdown.avg_trade_size_usd is not None:
        lines.append(f"Avg trade size:   {_fmt_money(breakdown.avg_trade_size_usd)}")
    else:
        lines.append(f"Avg trade size:   {_MISSING}")
    lines.append(
        f"Trade count:      {breakdown.trade_count} total "
        f"({breakdown.closed_positions} closed, {breakdown.open_positions} open)"
    )

    # VIB-4846 (T5): gas-efficiency ratios + total friction. None inputs render
    # as the em-dash placeholder so unmeasured stays distinct from measured.
    lines.append("")
    lines.append("Gas efficiency")
    lines.append("--------------")
    lines.append(
        f"Avg gas / trade:  {_fmt_money(breakdown.avg_gas_per_trade_usd) if breakdown.avg_gas_per_trade_usd is not None else _MISSING}"
    )
    if breakdown.gas_as_pct_of_avg_trade_bps is None:
        lines.append(f"Gas % of trade:   {_MISSING}")
    else:
        lines.append(f"Gas % of trade:   {_fmt_bps(breakdown.gas_as_pct_of_avg_trade_bps)}")
    lines.append(
        f"Total friction:   {_fmt_money(-breakdown.total_friction_usd) if breakdown.total_friction_usd is not None else _MISSING}"
    )

    # VIB-3493: LP rebalance attribution. Only render when the strategy
    # actually has LP events; non-LP strategies stay clean.
    if breakdown.lp_open_count > 0 or breakdown.lp_close_count > 0:
        lines.append("")
        lines.append("LP rebalance attribution (continuous strategy-level)")
        lines.append("-----------------------------------------------------")
        lines.append(
            f"LP gas total:     {_fmt_money(-breakdown.lp_total_gas_usd) if breakdown.lp_total_gas_usd is not None else _MISSING}"
        )
        lines.append(
            f"  open gas:       {_fmt_money(-breakdown.lp_open_gas_usd) if breakdown.lp_open_gas_usd is not None else _MISSING}"
        )
        lines.append(
            f"  close gas:      {_fmt_money(-breakdown.lp_close_gas_usd) if breakdown.lp_close_gas_usd is not None else _MISSING}"
        )
        lines.append(
            f"LP cycles:        {breakdown.lp_open_count} OPENs / {breakdown.lp_close_count} CLOSEs / "
            f"{breakdown.lp_close_open_pairs} close→open pairs"
        )

    if breakdown.warnings:
        lines.append("")
        lines.append("Notes:")
        for warning in breakdown.warnings:
            lines.append(f"  - {warning}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Click command
# ---------------------------------------------------------------------------


# VIB-4793: version stamp on the `strat pnl --json` document.  Operators and
# the hosted parity tooling pin to this; bump only on a breaking shape change
# (removed/renamed field or changed type).  Additive fields (new keys) do NOT
# require a bump.
_PNL_JSON_SCHEMA_VERSION = 1


def _emit_json_output(
    breakdown: PnLBreakdown,
    acct_data: Any,
    lp_section: Any,
    lending_section: Any,
    pendle_section: Any,
    dq_section: Any,
) -> None:
    """Serialise the strat-pnl payload to JSON and ``click.echo`` it.

    Extracted from ``strat_pnl`` to keep that function under the CRAP
    threshold after W1-2 / W1-3 / W1-6 read-side additions.  Each
    ``if not section.is_empty`` branch lives here rather than inflating
    the CLI entry point's cyclomatic complexity.

    The payload is prefixed with ``schema_version`` (VIB-4793) so machine
    consumers can pin to a stable shape.
    """
    out: dict[str, Any] = {"schema_version": _PNL_JSON_SCHEMA_VERSION}
    out.update(breakdown.to_json_dict())
    out["strategy_classes"] = sorted(str(c) for c in acct_data.strategy_classes)
    if not lp_section.is_empty:
        out["lp"] = lp_section_to_dict(lp_section)
    if not lending_section.is_empty:
        out["lending"] = lending_section_to_dict(lending_section)
    if not pendle_section.is_empty:
        out["pendle"] = pendle_section_to_dict(pendle_section)
    if not dq_section.is_empty:
        out["data_quality"] = data_quality_to_dict(dq_section)
    click.echo(json.dumps(out, indent=2))


# crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
@click.command("pnl")
@click.option(
    "--deployment-id",
    "-s",
    required=True,
    help="Deployment ID of the strategy (as printed by `almanak strat list`).",
)
@click.option(
    "--db",
    "db_path",
    default=None,
    help="SQLite state DB path (default: $ALMANAK_STATE_DB or ./almanak_state.db).",
)
@click.option(
    "--ledger-limit",
    default=10000,
    type=int,
    show_default=True,
    help="Max ledger rows to scan for gas/slippage aggregation.",
)
@click.option(
    "--position-limit",
    default=10000,
    type=int,
    show_default=True,
    help=(
        "Max position events to load for win-rate / lifecycle walk. "
        "Long-running strategies with >10k OPEN/CLOSE events should raise this; "
        "older events get truncated and can skew the win-rate denominator."
    ),
)
@click.option("--json", "-j", "as_json", is_flag=True, help="Emit JSON instead of text.")
def strat_pnl(  # noqa: C901
    deployment_id: str,
    db_path: str | None,
    ledger_limit: int,
    position_limit: int,
    as_json: bool,
) -> None:
    """Per-strategy PnL breakdown from persisted accounting data (VIB-3206/3427).

    Reads ``PortfolioMetrics``, ``LedgerEntry``, ``PositionEvent``,
    ``AccountingEvent``, and the latest ``PortfolioSnapshot`` from the local
    SQLite state DB. Detects strategy class (LP / lending / Pendle / swap) and
    renders only relevant sections. Does not call the gateway.

    Examples:

    \b
        almanak strat pnl -s uniswap_rsi:ab12cd34ef56
        almanak strat pnl -s uniswap_rsi:ab12cd34ef56 --json
        almanak strat pnl -s uniswap_rsi:ab12cd34ef56 --db ./state.db
    """
    if ledger_limit <= 0:
        click.secho("--ledger-limit must be a positive integer.", fg="red", err=True)
        sys.exit(1)
    if position_limit <= 0:
        click.secho("--position-limit must be a positive integer.", fg="red", err=True)
        sys.exit(1)

    resolved_db = db_path or _default_db_path()
    if not Path(resolved_db).exists():
        click.secho(
            f"State DB not found at {resolved_db}. Run the strategy at least once (or pass --db).",
            fg="red",
            err=True,
        )
        sys.exit(1)

    try:
        # Probe with one extra row (`limit + 1`) on every truncatable axis so
        # we can distinguish "exactly N rows, no truncation" from "more than
        # N, older rows dropped". Without the probe row the only signal is
        # `len == limit`, which has a false-positive on the equality boundary.
        acct_data = asyncio.run(
            load_accounting_data(
                resolved_db,
                deployment_id,
                ledger_limit=ledger_limit + 1,
                position_limit=position_limit + 1,
            )
        )
    except Exception as exc:
        click.secho(f"Failed to read state DB: {exc}", fg="red", err=True)
        sys.exit(1)

    # Honour the user's caps downstream: trim each truncated list to the
    # requested window so reports / counts agree with what the flags asked
    # for. The truncation flags drive a single warning emitted later.
    position_events_truncated = len(acct_data.position_events) > position_limit
    if position_events_truncated:
        acct_data.position_events = acct_data.position_events[:position_limit]
    ledger_entries_truncated = len(acct_data.ledger_entries) > ledger_limit
    if ledger_entries_truncated:
        acct_data.ledger_entries = acct_data.ledger_entries[:ledger_limit]

    metrics = acct_data.metrics
    ledger_entries = acct_data.ledger_entries
    position_events = acct_data.position_events
    snapshot = acct_data.snapshot

    # A strategy with no data at all = not found.
    if (
        metrics is None
        and not ledger_entries
        and not position_events
        and snapshot is None
        and not acct_data.lending_events
        and not acct_data.pendle_events
    ):
        click.secho(
            f"No persisted data found for strategy '{deployment_id}' in {resolved_db}.",
            fg="red",
            err=True,
        )
        sys.exit(1)

    # Generic portfolio summary (unchanged from VIB-3206)
    breakdown = compute_pnl_breakdown(
        deployment_id=deployment_id,
        metrics=metrics,
        ledger_entries=ledger_entries,
        position_events=position_events,
        snapshot=snapshot,
    )

    # VIB-4907 / F4: stamp the breakdown when the SWAP-class fallback pattern
    # fires.  The renderer hides the gross/net/NAV lines and shows
    # ``Headline PnL: unavailable`` + the detection reason instead.  Friction
    # components (gas, slippage, fees, IL) are computed from the ledger /
    # position events and remain accurate, so we still surface them.
    fallback_verdict = detect_stale_post_teardown_snapshot(
        acct_data.recent_snapshots,
        ledger_entries,
    )
    if fallback_verdict.suppressed:
        breakdown.headline_suppressed = True
        breakdown.headline_suppression_reason = fallback_verdict.reason

    # Surface truncation on either axis. The probe-row pattern above means
    # these fire only when older rows were actually dropped — no false-
    # positive at the equality boundary. Wording is class-agnostic so the
    # message stays accurate for LP, perp, lending, and Pendle strategies
    # (all consume `position_events` for lifecycle stats; `ledger_entries`
    # back gas + slippage aggregation regardless of strategy class).
    if position_events_truncated:
        breakdown.warnings.append(
            f"Truncated to --position-limit ({position_limit}) position "
            f"events; the strategy has emitted more. Position-derived "
            f"stats (lifecycle counts, win rate, strategy-level LP gas, "
            f"perp lifecycle PnL) are partial — re-run with a higher "
            f"--position-limit to see the full window."
        )
    if ledger_entries_truncated:
        breakdown.warnings.append(
            f"Truncated to --ledger-limit ({ledger_limit}) ledger entries; "
            f"the strategy has emitted more. Ledger-derived stats (gas "
            f"total, slippage, trade count, avg trade size) are partial "
            f"— re-run with a higher --ledger-limit to see the full window."
        )

    # Strategy-class-specific sections
    lp_section = build_lp_report(acct_data)
    lending_section = build_lending_report(acct_data)
    pendle_section = build_pendle_report(acct_data)
    dq_section = build_data_quality(acct_data)

    if as_json:
        _emit_json_output(breakdown, acct_data, lp_section, lending_section, pendle_section, dq_section)
        return

    # Text output
    classes_label = ", ".join(sorted(str(c) for c in acct_data.strategy_classes))
    click.echo(render_text(breakdown))
    if classes_label and classes_label != "unknown":
        click.echo(f"\nStrategy class: {classes_label}")

    extra = "".join(
        filter(
            None,
            [
                render_lp_section(lp_section),
                render_lending_section(lending_section, snapshot=acct_data.snapshot),
                render_pendle_section(pendle_section),
                render_data_quality_section(dq_section),
            ],
        )
    )
    if extra:
        click.echo(extra)
