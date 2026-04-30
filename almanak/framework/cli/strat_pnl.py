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

from almanak.framework.accounting.reporting import (
    build_data_quality,
    build_lending_report,
    build_lp_report,
    build_pendle_report,
    load_accounting_data,
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

    Hosted mode (``AGENT_ID`` set) has no local DB; ``almanak strat pnl``
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


def _amount_in_usd(entry: Any) -> Decimal | None:
    """Best-effort USD notional for a swap ledger entry.

    Heuristic: if either leg resolves to a known stablecoin (symbol-form or
    address-form, via ``TokenResolver``), use that leg's amount as the USD
    notional. Returns ``None`` when neither leg is a stablecoin (the
    slippage contribution is then skipped, as per the ticket spec).

    VIB-3204 will add an explicit ``amount_in_usd`` column — this heuristic
    is the interim path.
    """
    chain = getattr(entry, "chain", "") or ""
    t_in = _resolve_symbol(getattr(entry, "token_in", "") or "", chain)
    t_out = _resolve_symbol(getattr(entry, "token_out", "") or "", chain)
    a_in = _dec(getattr(entry, "amount_in", ""), default=Decimal("0"))
    a_out = _dec(getattr(entry, "amount_out", ""), default=Decimal("0"))

    if t_in in _STABLE_SYMBOLS and a_in > 0:
        return a_in
    if t_out in _STABLE_SYMBOLS and a_out > 0:
        return a_out
    return None


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
    protocol_fees_usd: Decimal | None = None  # VIB-3204 — placeholder for now
    slippage_usd: Decimal | None = None
    impermanent_loss_usd: Decimal | None = None  # VIB-3205 — placeholder for now
    net_pnl_usd: Decimal | None = None

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

    # Diagnostics
    warnings: list[str] = field(default_factory=list)

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
            "win_rate": _maybe(self.win_rate),
            "wins": self.wins,
            "closed_positions": self.closed_positions,
            "scored_closes": self.scored_closes,
            "avg_trade_size_usd": _maybe(self.avg_trade_size_usd),
            "trade_count": self.trade_count,
            "open_positions": self.open_positions,
            "warnings": list(self.warnings),
        }


def compute_pnl_breakdown(
    deployment_id: str,
    metrics: Any,
    ledger_entries: list,
    position_events: list,
    snapshot: Any,
) -> PnLBreakdown:
    """Compute a PnLBreakdown from already-loaded persisted rows.

    Split from the CLI command so tests can drive it with in-memory fixtures.
    """
    breakdown = PnLBreakdown(deployment_id=deployment_id)

    # --- Name / chain / wallet --------------------------------------------
    # Prefer the deployment_id carried on metrics, fall back to the caller-supplied
    # value. Chain comes from the snapshot or ledger; wallet is not currently
    # persisted in PortfolioMetrics so we leave it blank for now.
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

    # --- Gross / net PnL from PortfolioMetrics ----------------------------
    if metrics is not None:
        breakdown.gross_pnl_usd = _dec(metrics.pnl_before_gas)
        breakdown.net_pnl_usd = _dec(metrics.pnl_after_gas)
    else:
        breakdown.warnings.append(
            "No PortfolioMetrics row found — gross/net PnL unavailable. "
            "Run the strategy at least once so metrics are persisted."
        )

    # --- Gas (sum across ledger) ------------------------------------------
    gas_total = Decimal("0")
    any_gas = False
    for entry in ledger_entries:
        gas_val = getattr(entry, "gas_usd", "")
        if gas_val:
            gas_total += _dec(gas_val)
            any_gas = True
    if any_gas:
        breakdown.gas_usd = gas_total
    elif metrics is not None:
        # Fall back to the rolling gas_spent_usd on metrics (authoritative).
        breakdown.gas_usd = _dec(metrics.gas_spent_usd)

    # --- Slippage (sum over swap ledger entries) --------------------------
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

    # --- Trade count / avg trade size -------------------------------------
    breakdown.trade_count = len(ledger_entries)
    if ledger_entries:
        notionals: list[Decimal] = []
        for entry in ledger_entries:
            notional = _amount_in_usd(entry)
            if notional is not None and notional > 0:
                notionals.append(notional)
        if notionals:
            breakdown.avg_trade_size_usd = sum(notionals, Decimal("0")) / Decimal(len(notionals))

    # --- Closed / open / win rate from PositionEvent rows -----------------
    # `SQLiteStore.get_position_events()` returns rows newest-first. For each
    # ``position_id`` we only care about the *latest* lifecycle event:
    #
    #   - OPEN (first-ever) / OPEN (reopen after CLOSE) -> position is OPEN
    #   - CLOSE                                         -> position is CLOSED
    #   - UPDATE / COLLECT_FEES / ...                   -> preserves prior state
    #
    # The old implementation latched ``closed = True`` on any historical
    # ``CLOSE`` regardless of later events, so a position re-opened under the
    # same ``position_id`` (e.g. resumed strategy on a restart) would be
    # miscounted as closed and its stale attribution would leak into the
    # win-rate stat. We now take the first event we see per ``pid`` (= the
    # newest, since rows are newest-first) and derive state from that.
    # Only OPEN / CLOSE are lifecycle-defining. Everything else (SNAPSHOT,
    # COLLECT_FEES, UPDATE, ...) preserves the prior state — so we walk
    # past them to find the first lifecycle event per ``pid``.
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

    closed_count = sum(1 for p in positions_by_id.values() if p["closed"])
    open_count = sum(1 for p in positions_by_id.values() if not p["closed"])
    breakdown.closed_positions = closed_count
    breakdown.open_positions = open_count

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


def render_text(breakdown: PnLBreakdown) -> str:
    """Render a human-readable PnL breakdown."""
    lines: list[str] = []
    lines.append(f"Strategy: {breakdown.name or breakdown.deployment_id}")
    lines.append(f"Chain: {breakdown.chain or _MISSING}")
    lines.append(f"Wallet: {breakdown.wallet or _MISSING}")
    lines.append("")
    lines.append("PnL Breakdown")
    lines.append("-------------")
    lines.append(f"Gross PnL:        {_fmt_money(breakdown.gross_pnl_usd)}")
    lines.append(f"Gas costs:        {_fmt_money(-breakdown.gas_usd) if breakdown.gas_usd is not None else _MISSING}")
    if breakdown.protocol_fees_usd is None:
        lines.append(f"Protocol fees:    {_MISSING}        (requires VIB-3204)")
    else:
        lines.append(f"Protocol fees:    {_fmt_money(-breakdown.protocol_fees_usd)}")
    if breakdown.slippage_usd is None:
        lines.append(f"Slippage:         {_MISSING}")
    else:
        lines.append(f"Slippage:         {_fmt_money(-breakdown.slippage_usd)}")
    if breakdown.impermanent_loss_usd is None:
        lines.append(f"Impermanent loss: {_MISSING}        (requires VIB-3205)")
    else:
        lines.append(f"Impermanent loss: {_fmt_money(-breakdown.impermanent_loss_usd)}")
    lines.append(f"Net PnL:          {_fmt_money(breakdown.net_pnl_usd)}")
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
    suffix = f" [{unattributed} unattributed]" if unattributed else ""
    if breakdown.win_rate is not None:
        lines.append(
            f"Win rate:         {_fmt_percent(breakdown.win_rate)} ({breakdown.wins}/{scored} scored closes){suffix}"
        )
    else:
        lines.append(f"Win rate:         {_MISSING} ({breakdown.wins}/{scored} scored closes){suffix}")
    if breakdown.avg_trade_size_usd is not None:
        lines.append(f"Avg trade size:   {_fmt_money(breakdown.avg_trade_size_usd)}")
    else:
        lines.append(f"Avg trade size:   {_MISSING}")
    lines.append(
        f"Trade count:      {breakdown.trade_count} total "
        f"({breakdown.closed_positions} closed, {breakdown.open_positions} open)"
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


@click.command("pnl")
@click.option(
    "--strategy-id",
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
def strat_pnl(
    strategy_id: str,
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
        acct_data = asyncio.run(
            load_accounting_data(
                resolved_db,
                strategy_id,
                ledger_limit=ledger_limit,
                position_limit=position_limit,
            )
        )
    except Exception as exc:
        click.secho(f"Failed to read state DB: {exc}", fg="red", err=True)
        sys.exit(1)

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
            f"No persisted data found for strategy '{strategy_id}' in {resolved_db}.",
            fg="red",
            err=True,
        )
        sys.exit(1)

    # Generic portfolio summary (unchanged from VIB-3206)
    breakdown = compute_pnl_breakdown(
        deployment_id=strategy_id,
        metrics=metrics,
        ledger_entries=ledger_entries,
        position_events=position_events,
        snapshot=snapshot,
    )

    # Strategy-class-specific sections
    lp_section = build_lp_report(acct_data)
    lending_section = build_lending_report(acct_data)
    pendle_section = build_pendle_report(acct_data)
    dq_section = build_data_quality(acct_data)

    if as_json:
        out: dict[str, Any] = breakdown.to_json_dict()
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
                render_lending_section(lending_section),
                render_pendle_section(pendle_section),
                render_data_quality_section(dq_section),
            ],
        )
    )
    if extra:
        click.echo(extra)
