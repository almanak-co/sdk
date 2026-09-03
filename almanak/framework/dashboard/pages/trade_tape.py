"""Trade tape — broker-statement view of every intent the strategy executed.

One row per intent (``cycle_id``), joined across ``transaction_ledger``,
``accounting_events``, and ``position_events`` server-side. Each row
shows the headline trade line; the expander reveals receipt-parsed data,
oracle quotes used at execution block, pre/post on-chain state, and
the typed accounting payload.

This is the Senior-Quant counterpart to the existing ``timeline.py``
page (which stays as the "Activity log" — heartbeat-style operational
events). The tape reads as a broker statement; the timeline reads as
an event log.

Data source: ``DashboardService.GetTradeTape`` (gateway-only, no SDK
direct DB access). The tape never papers over NULL data — every absent
field is shown verbatim with the writer's ``unavailable_reason``.
"""

from __future__ import annotations

import html
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

import streamlit as st

from almanak.framework.accounting.gas_pricing import native_token_for_chain
from almanak.framework.dashboard.gateway_client import TradeTapeRow
from almanak.framework.dashboard.theme import get_chain_color
from almanak.framework.dashboard.utils import (
    _should_scale_raw_amount,
    _try_token_decimals,
    decode_selector,
    format_chain_badge,
    format_token_amount,
    format_usd,
    get_block_explorer_url,
    is_approval_tx,
    pick_action_tx,
    registry_handle_from_payload,
)


def _e(value: Any) -> str:
    """HTML-escape a value before interpolating into ``unsafe_allow_html``.

    The dashboard runs on operator workstations alongside private keys
    and gateway tokens. Strings flowing in from the gateway —
    receipt-parsed event data, ERC-20 token symbols, protocol names,
    accounting-payload values — are not trusted: an ERC-20 ``name()``
    legally returns arbitrary bytes and would otherwise be rendered as
    raw HTML by ``st.markdown(unsafe_allow_html=True)``. Always pass
    user-controlled strings through this before interpolating.
    """
    if value is None:
        return ""
    return html.escape(str(value), quote=True)


_INTENT_ICONS = {
    "SWAP": "🔄",
    "SUPPLY": "📥",
    "WITHDRAW": "📤",
    "BORROW": "💰",
    "REPAY": "↩️",
    "LP_OPEN": "📊",
    "LP_CLOSE": "📉",
    "PERP_OPEN": "🎯",
    "PERP_CLOSE": "🏁",
    "STAKE": "🔒",
    "UNSTAKE": "🔓",
    "BRIDGE": "🌉",
}

_CONFIDENCE_BADGES = {
    "HIGH": ("#00c853", "HIGH"),
    "ESTIMATED": ("#ff9800", "ESTIMATED"),
    "STALE": ("#ff9800", "STALE"),
    "UNAVAILABLE": ("#888888", "UNAVAILABLE"),
}


def _short_hash(h: str) -> str:
    if not h or len(h) < 12:
        return h

    return f"{h[:8]}…{h[-6:]}"


def _safe_decimal(s: str | None) -> Decimal:
    if not s:
        return Decimal("0")

    try:
        return Decimal(s)
    except (ValueError, TypeError):
        return Decimal("0")


def _format_lp_ledger_amount(amount: str, symbol: str, chain: str) -> str:
    """Format an LP ledger ``amount_in/out`` field.

    The ledger ``amount_in/out`` field can land in one of THREE shapes
    depending on which fallback path in ``observability/ledger.py`` fired:

    1. **Human Decimal from ``LPOpenData`` (post-VIB-5036)** —
       ``_extract_from_lp_open`` now SCALES ``LPOpenData.amount0/amount1`` to
       human units at write time (mirroring SWAP / lending), so a 1-token WETH
       position is ``"1"``, not ``10^18``. PRE-VIB-5036 rows on this path are
       still raw integers — the magnitude heuristic below is the back-compat
       shim that scales those legacy rows. New whole-number human values
       ``< 10^6`` pass through untouched; the ``>= 10^6`` residual edge in the
       False-NEGATIVE note now applies to this path too.
    2. **Human Decimal from SwapAmounts** —
       ``_extract_from_swap_amounts`` stores ``amount_in_decimal`` /
       ``amount_out_decimal``; LP_CLOSE rides this route when the close
       receipt produced SwapAmounts rather than ``LPCloseData``. Can be
       any positive Decimal (fractional or integral).
    3. **Human Decimal from intent** —
       ``_extract_from_lp_open`` falls back to ``intent.amount0/1`` when
       ``LPOpenData`` is absent (pre-VIB-3417 rows, parser failure).
       Same shape as (2).

    Without a ``units_kind`` discriminator on the ledger row, the only
    available signal is the value's magnitude. The decision is delegated
    to ``_should_scale_raw_amount`` (utils.py) — the single chokepoint
    shared with ``format_token_amount``. Its two branches:

    - Legacy ``abs(d) >= 10**6`` for any decimals (preserves PR #2290).
    - New 8-dec dust bracket ``1000 <= abs(d) < 10**6`` (VIB-3890
      residual: catches small raw WBTC positions like ``1346``).

    Both branches require ``d`` to be integral and the resolver to know
    the symbol on the chain; otherwise the helper returns ``None`` and
    the value passes through ``format_token_amount`` unscaled (degrade
    safe — never mis-scale on uncertain input).

    DURABLE FIX (VIB-4641 follow-up — adds a writer-side ``units_kind``
    discriminator on the ledger row, deliberately deferred from VIB-3890
    because it crosses writer / schema / metrics-database boundaries):

    - **False NEGATIVE** (human mis-scaled as raw): a whole-million
      human position from path (1, post-VIB-5036) / (2) / (3) — e.g.
      ``2_000_000`` USDC LP stored as ``Decimal("2000000")`` — would be
      re-scaled by 6 dec and render as ``"2.00 USDC"``. Bounded to
      payload-absent fallback rows: ``_format_lp_direction`` short-circuits
      to ``_format_human_amount`` when the typed accounting payload's
      ``amount0/amount1`` are present, so this window only fires when the
      accounting writer hasn't run yet. The same residual edge lives in
      ``cli/strat_pnl.py:_human_amount`` — both are magnitude-heuristic
      back-compat shims that VIB-4641's ``units_kind`` discriminator retires.

    The operator-trust invariant ("operator never sees a 10**decimals
    scale lie on a normal-sized position") is preserved by both branches
    and by the payload-first ordering above.
    """
    if amount in (None, "", "—"):
        return "—"

    try:
        d = Decimal(str(amount))
    except (ArithmeticError, ValueError, TypeError):
        return format_token_amount(amount, symbol, chain)

    # Ledger fallbacks may contain raw 8-decimal dust; keep that heuristic
    # scoped to LP rows so ordinary token formatting is unchanged.
    decimals = _should_scale_raw_amount(d, symbol, chain, lp_fallback_context=True)

    if decimals is None:
        return format_token_amount(amount, symbol, chain)

    return _format_human_amount(d / (Decimal(10) ** decimals))


def _format_native_gas(gas_usd: Decimal, chain: str | None, price_inputs_json: str | None) -> str:
    """Return ``"0.00000132 ETH"``-style suffix for the gas cost, or ``""``.

    Derives the native amount as ``gas_usd / native_price_usd`` using the
    oracle quote stamped on the row's ``price_inputs_json``. Returns ``""``
    when the native quote isn't on the row (no extra network call from the
    dashboard) — the caller falls back to the bare USD figure.
    """
    if gas_usd <= 0 or not chain or not price_inputs_json:
        return ""

    symbol = native_token_for_chain(chain)

    if not symbol:
        return ""

    try:
        prices = json.loads(price_inputs_json)
    except (json.JSONDecodeError, TypeError):
        return ""

    if not isinstance(prices, dict):
        return ""

    symbol_lower = symbol.lower()
    info = next(
        (v for k, v in prices.items() if isinstance(k, str) and k.lower() == symbol_lower),
        None,
    )

    if not isinstance(info, dict):
        return ""

    raw_price = info.get("price_usd") or info.get("price")

    native_price = _safe_decimal(str(raw_price) if raw_price is not None else None)
    if native_price <= 0:
        return ""

    native_amount = gas_usd / native_price

    if abs(native_amount) >= Decimal("1"):
        amount_str = f"{native_amount:,.6f}"
    else:
        amount_str = f"{native_amount:.4g}"

    return f"{amount_str} {symbol}"


def _format_human_amount(amount: Any) -> str:
    """Display an already-decoded token amount.

    Mirrors ``format_token_amount``'s display rules (≥1 → thousands +
    2dp, sub-1 → 4 sig figs, 0 → ``"0"``, blank → ``"—"``) but skips the
    raw-integer-units heuristic. Accounting payload fields like
    ``amount0/1`` and ``fees0/1_collected`` are stamped as already-decoded
    human Decimals — feeding them to ``format_token_amount`` would
    misclassify integral large values (e.g. ``Decimal("1000000")`` for a
    1M USDC LP leg) as raw on-chain integers and rescale them by the
    token's decimals, understating the headline by 10**decimals. Use this
    helper for payload-sourced values; keep ``format_token_amount`` for
    ledger ``amount_in/out`` strings, which are raw on-chain integers.
    """
    if amount in (None, "", "—"):
        return "—"

    try:
        d = Decimal(str(amount))
    except (ArithmeticError, ValueError, TypeError):
        return str(amount)

    if not d.is_finite():
        return str(amount)

    abs_d = abs(d)

    if abs_d == 0:
        return "0"

    if abs_d >= Decimal("1"):
        return f"{d:,.2f}"

    return f"{d:.4g}"


def render_trade_tape(deployment_id: str, *, limit: int = 50) -> None:
    """Render the trade-tape tab for a strategy."""
    from almanak.framework.dashboard.data_source import (
        GatewayConnectionError,
        get_trade_tape,
    )

    try:
        response = get_trade_tape(deployment_id, limit=limit)
    except GatewayConnectionError:
        st.error("Gateway unavailable — cannot load trade tape.")
        return

    if response is None or not response.rows:
        st.info(
            "No intents yet. The trade tape lights up once the strategy "
            "executes its first SWAP / LP_OPEN / SUPPLY / etc."
        )
        return

    st.markdown(f"**{len(response.rows)} intent(s)** · newest first · click any row for the receipt-parsed expander.")

    col1, col2 = st.columns([4, 1])

    with col1:
        intent_types = sorted({row.intent_type for row in response.rows if row.intent_type})

        selected_intents = st.multiselect(
            "Action",
            intent_types,
            default=intent_types,
            key=f"tape_intents_{deployment_id}",
        )

    with col2:
        # Approvals are hidden on screen by default but remain in CSV exports
        # so audit artifacts always contain every submitted leg.
        show_approvals = st.toggle(
            "Show approvals",
            value=False,
            key=f"tape_show_approvals_{deployment_id}",
            help=(
                "When off, ERC-20 approve sub-txs are hidden from the per-intent "
                "expander and the count badge shows e.g. '1 of 3 (2 approvals hidden)'. "
                "The CSV export is unaffected — it always emits one row per sub-tx."
            ),
        )

    rows = [r for r in response.rows if r.intent_type in selected_intents]

    if not rows:
        st.info("No rows match the current filters.")
        return

    # Export only filtered intents; each export still includes every sub-tx,
    # including approvals, for a complete audit trail.
    _render_csv_export(rows, deployment_id)

    last_date = None
    for row in rows:
        ts = row.timestamp

        date_str = ts.strftime("%Y-%m-%d") if ts else "—"

        if date_str != last_date:
            st.markdown(
                f"<div style='color:#888;font-size:0.85rem;margin:0.75rem 0 0.25rem 0;'>"
                f"📅 <strong>{date_str}</strong></div>",
                unsafe_allow_html=True,
            )

            last_date = date_str

        _render_tape_row(row, show_approvals=show_approvals)


def _rows_to_csv(rows: list[TradeTapeRow]) -> tuple[str, int]:
    """Build the trade-tape CSV text. Returns ``(csv_text, sub_tx_count)``.

    VIB-3928 — original ask was a one-row-per-intent dump.
    VIB-4046 — switched to one row per *sub-tx* with a ``parent_intent_id``
    column joining back to the parent ledger row's ``id``. Single-tx
    intents (no ``all_tx_results``) still emit one row each — they
    are simply degenerate bundles. Approvals are always exported even
    when the dashboard's "Show approvals" toggle is off, so spreadsheet
    auditors get the full picture (per ticket).

    Split out from ``_render_csv_export`` (VIB-6043 leg 2) so the exported
    CONTENT can be asserted without a Streamlit runtime. It previously existed
    only inside a function whose sole output was a download button, which is why
    a column meaning "did this transaction land on-chain" could be filled from
    the framework verdict without any test noticing.
    """
    import csv as _csv
    import io

    buf = io.StringIO()

    writer = _csv.writer(buf, quoting=_csv.QUOTE_MINIMAL)

    writer.writerow(
        [
            "timestamp",
            "parent_intent_id",
            "cycle_id",
            "intent_type",
            "sub_tx_index",
            "sub_tx_count",
            "is_action_tx",
            "is_approval",
            "function_selector",
            "function_label",
            "tx_success",
            "intent_success",
            "chain",
            "protocol",
            "token_in",
            "amount_in",
            "amount_in_usd",
            "token_out",
            "amount_out",
            "amount_out_usd",
            "effective_price",
            "slippage_bps",
            "tx_gas_used",
            "intent_gas_used",
            "intent_gas_usd",
            "tx_hash",
            "confidence",
            "oracle_source",
            "position_id",
            "primary_risk_metric",
        ]
    )

    sub_tx_count = 0
    for r in rows:
        sub_txs = _resolve_legs(r)

        # ``tx_success`` records on-chain status, independently of the framework
        # verdict in ``intent_success``; a synthesized no-op stays unmeasured.
        legs = (
            sub_txs
            if sub_txs
            else [
                {
                    "tx_hash": r.tx_hash,
                    "gas_used": r.gas_used or 0,
                    "success": _synthesized_leg_status(r),
                }
            ]
        )

        action = pick_action_tx(sub_txs, r.intent_type) if len(sub_txs) > 1 else None

        action_hash = (action or {}).get("tx_hash") if action else (r.tx_hash or "")

        for idx, tx in enumerate(legs, start=1):
            tx_hash = tx.get("tx_hash") or ""

            # Receipt ``status`` outranks legacy ``success``; absent evidence is
            # unmeasured, so the audit export leaves ``tx_success`` blank.
            leg_landed = _leg_landed(tx)

            tx_success_cell = "" if leg_landed is None else ("1" if leg_landed else "0")

            tx_gas = _coerce_gas(tx.get("gas_used"))

            selector = tx.get("function_selector") or ""

            # The sole leg is the action, never an approval, so non-approval
            # action filters cannot discard it from the audit export.
            is_single_leg = len(legs) == 1

            is_action = True if is_single_leg else (bool(action_hash) and tx_hash == action_hash)

            is_approval = False if is_single_leg else is_approval_tx(tx)

            sub_tx_count += 1

            writer.writerow(
                [
                    r.timestamp.isoformat() if r.timestamp else "",
                    r.id or "",
                    r.cycle_id or "",
                    r.intent_type or "",
                    idx,
                    len(legs),
                    "1" if is_action else "0",
                    "1" if is_approval else "0",
                    selector,
                    decode_selector(selector) if selector else "",
                    tx_success_cell,
                    "1" if r.success else "0",
                    r.chain or "",
                    r.protocol or "",
                    r.token_in or "",
                    str(r.amount_in or ""),
                    str(r.amount_in_usd or ""),
                    r.token_out or "",
                    str(r.amount_out or ""),
                    str(r.amount_out_usd or ""),
                    str(r.effective_price or ""),
                    str(r.slippage_bps or ""),
                    str(tx_gas),
                    str(r.gas_used or ""),
                    str(r.gas_usd or ""),
                    tx_hash,
                    r.confidence or "",
                    getattr(r, "oracle_source", "") or "",
                    getattr(r, "position_id", "") or "",
                    getattr(r, "primary_risk_metric", "") or "",
                ]
            )

    return buf.getvalue(), sub_tx_count


def _render_csv_export(rows: list[TradeTapeRow], deployment_id: str) -> None:
    """Render a single download button for the filtered tape."""
    csv_text, sub_tx_count = _rows_to_csv(rows)

    csv_bytes = csv_text.encode("utf-8")

    fname = f"trade_tape_{deployment_id[:32]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    st.download_button(
        label=f"⬇️ Export {sub_tx_count} sub-tx row(s) from {len(rows)} intent(s) as CSV",
        data=csv_bytes,
        file_name=fname,
        mime="text/csv",
        key=f"tape_csv_{deployment_id}",
        help=(
            "Trade-tape export: one row per on-chain sub-tx, joined back to "
            "the parent intent via parent_intent_id. Always full (approvals "
            "included) regardless of the 'Show approvals' UI toggle — "
            "spreadsheet auditors need every leg."
        ),
    )


def _coerce_gas(value: object) -> int:
    """Coerce a sub-tx ``gas_used`` field to int, returning 0 on garbage.

    Receipt-parser bugs / schema-version skew can land non-numeric
    values in ``all_tx_results[*].gas_used``. The dashboard renders
    inside a Streamlit page; an uncaught ``int(...)`` ValueError on
    one bad row deletes the whole tape — exactly the failure surface
    the operator is here to investigate. Fail closed to 0 instead.
    """

    if value is None:
        return 0

    try:
        return int(value)  # type: ignore[call-overload]
    except (TypeError, ValueError):
        return 0


def _parse_extracted_data(row: TradeTapeRow) -> dict[str, Any]:
    """Decode ``extracted_data_json`` for a row, or ``{}`` on any failure.

    Centralises the parse so callers (headline-link picker, expander
    sub-tx renderer, CSV export) all see the same dict and don't drift.
    """
    if not row.extracted_data_json:
        return {}
    try:
        data = json.loads(row.extracted_data_json)
    except (json.JSONDecodeError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def _get_all_tx_results(row: TradeTapeRow) -> list[dict]:
    """Pull the ``all_tx_results`` array off a row, defensively.

    Single-tx intents (and pre-VIB-3886 rows) have no ``all_tx_results``
    — we surface the row exactly as today (no badge, no expander).
    """
    data = _parse_extracted_data(row)
    txs = data.get("all_tx_results")
    if not isinstance(txs, list):
        return []
    return [tx for tx in txs if isinstance(tx, dict)]


def _get_sub_transactions(row: TradeTapeRow) -> list[dict]:
    """Pull the ``sub_transactions`` array off a row, defensively.

    VIB-4087 emits ``sub_transactions`` for *every* row that executed at
    least one tx — single-tx included — with a per-leg ``status``
    (``"success"`` / ``"failure"``) derived from ``int(receipt.status)``
    (``observability.ledger._build_sub_transactions``). This is the
    authoritative on-chain per-leg verdict and is present on far more
    rows than the multi-tx-only ``all_tx_results``.
    """
    data = _parse_extracted_data(row)
    txs = data.get("sub_transactions")
    if not isinstance(txs, list):
        return []
    return [tx for tx in txs if isinstance(tx, dict)]


def _leg_landed(tx: dict) -> bool | None:
    """On-chain success of a single sub-tx, or ``None`` if unmeasured.

    Empty != Zero: a leg with neither ``status`` nor ``success`` is
    *unmeasured*, not failed — we return ``None`` so the caller can
    decide to defer rather than invent a verdict.
    """
    status = tx.get("status")
    if status is not None:
        return str(status).lower() == "success"
    success = tx.get("success")
    if success is not None:
        return bool(success)
    return None


# Degradation reasons are extensible; unknown tokens need an honest fallback.
_DEGRADED_REASON_LABELS: dict[str, str] = {
    "amounts_unmeasured": "books degraded (amounts unmeasured)",
}


def _measured_leg_statuses(legs: list[dict]) -> list[bool]:
    """The MEASURED on-chain verdicts among ``legs`` (unmeasured entries dropped)."""
    return [v for v in (_leg_landed(tx) for tx in legs) if v is not None]


def _submitted_nothing(row: TradeTapeRow) -> bool:
    """True when the row PROVES no transaction was ever submitted.

    This is a measured statement, not an absence of one, and that distinction is
    the whole point. Empty != Zero applies to the glyph exactly as it applies to
    a money column: a blank ``tx_hash`` with no measured leg receipt is not
    "we don't know whether it landed" — it is "nothing was sent".

    ``_resolve_onchain_display_status`` returns ``None`` for this row, and the
    headline reads ``None`` as *unknown* and falls back to the framework verdict
    ``row.success``, which a no-op sets to ``True``. That renders a green tick
    asserting a trade executed for something never submitted. The CSV lane
    already refuses to grade it (``_synthesized_leg_status`` returns ``None``),
    so without this the two lanes disagree on the same row — the surface split
    this delta exists to close, re-created one lane over.

    Reachable today: a Compound V3 ``withdraw_all`` against zero collateral, or
    a Euler V2 full exit, compiles to ``transactions=[]`` with
    ``metadata["no_op"]=True``; the orchestrator returns ``success=True`` with
    no transaction results and the runner writes ``success=1, tx_hash=""``.
    ``WITHDRAW`` is not in ``REQUIRED_MONEY_SLOTS``, so no degradation marker is
    stamped and ``landed()``'s deliberately hash-blind clean arm answers True.

    Reader-side only, and deliberately so. The root cause is that the ledger
    books a clean success for a bundle that submitted nothing — six readers
    inherit it (VIB-6176). Hoisting the hash check into ``landed()`` itself is
    NOT the fix: that predicate must keep answering for callers whose SELECT has
    no hash column in scope, and the hoist desyncs it from ``landed_sql``. It
    was measured against the suite and breaks three guard tests.
    """
    from almanak.framework.accounting.ledger_guard import degraded

    if (row.tx_hash or "").strip():
        return False

    # Hashless pre-broadcast failures must stay failures. Successful or degraded
    # hashless rows are no-ops, and a degraded no-op may carry ``success=0``.
    if not row.success and not degraded(row.error):
        return False

    legs = _resolve_legs(row)
    # A leg hash proves submission even when receipt status is unmeasured;
    # schema-skewed legs may legitimately omit a status.
    for leg in legs:
        if isinstance(leg, dict) and str(leg.get("tx_hash") or "").strip():
            return False
    return not _measured_leg_statuses(legs)


def _ledger_landed(row: TradeTapeRow) -> bool:
    """Full canonical LANDED verdict for a tape row (both arms).

    Kept a one-liner over ``ledger_guard.landed`` so the export can never
    answer this question differently from the scorecard, the CI ship-gate or
    the demo scorer.
    """
    from almanak.framework.accounting.ledger_guard import landed

    return landed(row.success, row.error, row.tx_hash)


def _synthesized_leg_status(row: TradeTapeRow) -> bool | None:
    """On-chain status of the ONE leg the CSV synthesizes for a leg-less row.

    Three-valued, because the honest answer is three-valued — the same
    Empty != Zero trichotomy the export already uses for real legs
    (:func:`_leg_landed`): ``True`` = measured success, ``False`` = measured
    failure, ``None`` = **no measurement**, exported as a blank cell.

    The blank case is the NO-OP: the degradation marker with no tx hash. Under
    the previous rule it exported ``tx_success=1``, claiming a transaction that
    was never submitted. Simply inverting that to ``0`` would swap one false
    statement for another — ``0`` says "this transaction FAILED", and a no-op
    did not fail; an ``LP_CLOSE`` against an already-empty position is the
    correct, idempotent outcome of a teardown with nothing left to close.
    Nothing landed, nothing reverted, and there is no receipt to grade. A blank
    cell is honest; a ``0`` is a claim, exactly as a fabricated ``0`` in a money
    column would be.
    """

    # Check submission before canonical landed semantics: clean no-ops can have
    # ``success=True``, while degraded no-ops can have ``success=False``.
    if not (row.tx_hash or "").strip():
        return None

    from almanak.framework.accounting.ledger_guard import degraded

    if _ledger_landed(row):
        return True
    if degraded(row.error):
        return None
    return False


def _marker_asserts_landed(row: TradeTapeRow) -> bool:
    """Does the degradation marker, plus a tx hash, establish that this row landed?

    The DEGRADED ARM ONLY — the tape headline needs it in isolation, because a
    clean row must still fall through to the per-leg receipt logic rather than
    short-circuit to green. The CSV export wants both arms and calls
    :func:`_ledger_landed`.

    The two surfaces had diverged: the headline gated on ``row.tx_hash`` (added
    to stop a NO-OP bundle rendering green) while the CSV synthesized its leg
    from the marker alone, so the no-op the headline refused to claim was still
    exported as ``tx_success=1`` — the same defect, persisted into the audit
    artifact after being removed from the screen. Two surfaces deciding the same
    question must not each own a copy of the rule.

    The rule itself is NOT spelled here — it is ``ledger_guard.landed``'s
    degraded arm, reached by passing a ``success`` the clean arm rejects so only
    that arm can fire (the callers need the marker arm alone; the clean arm is
    theirs to combine). This function used to re-implement ``degraded(error) and
    row.tx_hash``, which is the same mirror-drift the canonical module exists to
    prevent — one spelling coerced with ``bool``, the canonical one strips
    whitespace, and they would have diverged on the first oddly-shaped hash.
    """
    from almanak.framework.accounting.ledger_guard import landed

    # Force the canonical predicate through its degraded arm.
    return landed(0, row.error, row.tx_hash)


def _resolve_legs(row: TradeTapeRow) -> list[dict]:
    """Leg array to read for ``row``, preferring the authoritative source.

    ``sub_transactions`` is authoritative and present on far more rows, but the
    fallback must key on "carries no MEASURED status", not on "is an empty
    list": a schema-skewed array (entries present, no ``status``/``success``)
    is non-empty yet tells us nothing, and short-circuiting on truthiness there
    hid a measured failure sitting in ``all_tx_results``.

    Shared by the tape headline and the CSV export so the two cannot disagree
    about which array is authoritative (they did — the export read only
    ``all_tx_results``).
    """
    subs = _get_sub_transactions(row)
    if _measured_leg_statuses(subs):
        return subs
    return _get_all_tx_results(row) or subs


def _classify_downgrade_reason(error: str | None) -> str:
    """Bucket a framework downgrade error into a short, operator-readable reason.

    Mirrors the runner's ``_classify_failure_reason`` keyword buckets for
    the two paths that write ``transaction_ledger.success=False`` on a
    tx that *did* land on-chain: the slippage circuit-breaker and the
    reconciliation-failure finalizer (``strategy_runner.py``).
    """
    from almanak.framework.accounting.ledger_guard import degraded

    text = (error or "").lower()
    # Classify degradation first because its detail may contain the later
    # slippage or reconciliation keywords.
    if degraded(error):
        # Parse the extensible reason token instead of assuming one kind.
        from almanak.framework.accounting.ledger_guard import DEGRADED_PREFIX

        token = str(error or "")[len(DEGRADED_PREFIX) :].split(":", 1)[0].strip()
        return _DEGRADED_REASON_LABELS.get(token, "books degraded")
    if "slippage" in text:
        return "slippage breach"
    if "reconcil" in text or text.startswith("recon"):
        return "reconciliation downgraded"
    return "flagged post-execution"


def _resolve_onchain_display_status(row: TradeTapeRow) -> tuple[bool | None, str | None]:
    """Resolve the tape's headline status from on-chain per-leg receipts.

    ALM-2759 — ``transaction_ledger.success`` is the *framework verdict*
    ("iteration completed cleanly: execution + slippage gate +
    reconciliation"), deliberately written ``False`` on the
    slippage-breach and reconciliation-failure paths even when the tx
    LANDED on-chain. Rendering the tape headline off that verdict shows a
    red ✗ for a trade that actually executed — the bug this fixes.

    Returns ``(landed, flagged_reason)``:

    - ``landed`` — ``True`` if every measured per-leg receipt succeeded,
      ``False`` if any measured leg failed, ``None`` if the row carries
      no per-leg receipt data at all (older / unmeasured rows). ``None``
      signals the caller to fall back to the framework ``success`` verdict
      rather than fabricate a green ✓ for a genuinely unknown status.
    - ``flagged_reason`` — set only when the tx landed on-chain
      (``landed is True``) but the framework downgraded the iteration
      (``row.success`` is ``False``); a short bucketed reason for the
      amber "landed but flagged" badge. ``None`` otherwise.

    VIB-6043 leg 2 — the degradation marker is consulted BEFORE giving up on
    receipts. The write-time Empty != Zero guard stamps ``success=False`` plus
    an ``accounting_degraded:`` marker on a row whose tx really landed, and the
    lane it exists for — a Safe / bundle execution whose parser yields neither
    amounts NOR per-leg receipts — is precisely the lane where ``measured`` is
    empty. Falling through to the raw ``row.success`` there rendered a **red ✗
    "failed" for a trade that executed**, which is the same class of misreport
    ALM-2759 fixed for the slippage lane, on the row shape that is hardest for
    an operator to check by hand.

    The predicate used here is ``ledger_guard.degraded``, NOT ``landed``.
    That is deliberate: ``landed()`` is ``success == 1 or degraded(error)``, so
    it would report ``True`` for every clean row and collapse the ALM-2759
    ``None``-defer signal that tells the caller "unknown, fall back to the
    framework verdict". Only the narrower "are the books degraded?" question
    belongs here. Both live in ``ledger_guard`` so the marker is matched
    identically everywhere rather than re-spelled locally.
    """
    from almanak.framework.accounting.ledger_guard import degraded

    measured = _measured_leg_statuses(_resolve_legs(row))
    row_is_degraded = degraded(row.error)

    if not measured:
        # Without receipts, only a degradation marker backed by a transaction
        # hash supports landed-but-flagged; undegraded rows remain unknown and
        # fall back to the framework verdict. A hash is necessary, not sufficient
        # evidence because gateway callers can persist markers without validating
        # receipts, and the marker's ``chain_success`` is asserted by construction.
        if _marker_asserts_landed(row):
            return True, _classify_downgrade_reason(row.error)
        return None, None

    landed = all(measured)
    # A measured failed leg outranks the degradation marker and must stay red.
    flagged_reason = _classify_downgrade_reason(row.error) if landed and (row_is_degraded or not row.success) else None
    return landed, flagged_reason


def _parse_accounting_payload(row: TradeTapeRow) -> dict[str, Any]:
    """Decode ``accounting_payload_json`` for a row, or ``{}`` on any failure.

    LP headlines read ``token0/token1/amount0/amount1`` (and on CLOSE,
    ``fees0_collected/fees1_collected/fees_total_usd``) from the typed
    payload — those are post-decoded human Decimals stamped at execution
    block, so the dashboard does not have to re-decode raw on-chain ints.
    """
    if not row.accounting_payload_json:
        return {}
    try:
        data = json.loads(row.accounting_payload_json)
    except (json.JSONDecodeError, TypeError):
        return {}
    return data if isinstance(data, dict) else {}


def _format_direction(row: TradeTapeRow) -> tuple[str, str]:
    """Return ``(direction_html, lp_fee_line_html)`` for a tape row.

    Single-asset moves (SWAP, SUPPLY, WITHDRAW, BORROW, REPAY, BRIDGE …)
    keep the ``token_in → token_out`` shape. LP_OPEN / LP_CLOSE are
    dual-asset and render as ``amt0 tok0 + amt1 tok1`` — both legs deposit
    (OPEN) or both legs receive (CLOSE). LP_CLOSE adds a ``fees:`` sub-line
    when the typed payload carries non-zero ``fees0_collected`` /
    ``fees1_collected`` (or a USD total).
    """
    intent = row.intent_type or ""
    if intent in ("LP_OPEN", "LP_CLOSE"):
        return _format_lp_direction(row, is_close=intent == "LP_CLOSE")

    if not (row.token_in or row.token_out):
        return "", ""

    amt_in = format_token_amount(row.amount_in, row.token_in, row.chain)
    amt_out = format_token_amount(row.amount_out, row.token_out, row.chain)
    in_part = f"<code>{_e(amt_in)}</code> {_e(row.token_in)}" if row.token_in else ""
    out_part = f"<code>{_e(amt_out)}</code> {_e(row.token_out)}" if row.token_out else ""
    if in_part and out_part:
        return f"{in_part} → {out_part}", ""
    return in_part or out_part, ""


def _format_ncoin_lp_direction(row: TradeTapeRow, *, is_close: bool) -> str | None:
    """N-coin (>2) fungible LP headline — every pool coin, in pool order (VIB-5896).

    A Curve 3pool/4pool (or Balancer weighted) LP_OPEN/LP_CLOSE touches N
    coins, but both the accounting payload and the ledger row are 2-slot —
    rendering from them shows 2 of N legs (the blind dashboard-auditor read a
    $300 3-coin deposit as "$200, 2 coins"). The receipt-parsed
    ``lp_open_data`` / ``lp_close_data`` on ``extracted_data_json`` carry the
    FULL pool-coin-ordered vector (``coin_symbols`` + ``amount0/amount1`` +
    ``additional_amounts``), so the tape renders from that instead.

    Returns ``None`` for 2-coin / concentrated-liquidity venues (or when the
    extracted data is absent/misaligned) — caller falls through to the
    canonical 2-slot path unchanged. Amounts are raw on-chain integers scaled
    via the token resolver; an unscalable amount renders raw (never a guessed
    scale), and an unmeasured (null) amount renders ``—`` (Empty ≠ Zero).
    """
    data = _parse_extracted_data(row)
    lp = data.get("lp_close_data" if is_close else "lp_open_data")
    if not isinstance(lp, dict):
        return None
    coins = lp.get("coin_symbols")
    if not isinstance(coins, list) or len(coins) <= 2:
        return None

    if is_close:
        amounts: list[Any] = [lp.get("amount0_collected"), lp.get("amount1_collected")]
    else:
        amounts = [lp.get("amount0"), lp.get("amount1")]
    # Additional amounts follow pool-coin order. Require exactly indices 2..N-1;
    # gaps are malformed payloads, not intentionally unmeasured values.
    additional = lp.get("additional_amounts")
    if not isinstance(additional, dict):
        return None
    try:
        extra_keys = sorted(additional, key=int)
    except (TypeError, ValueError):
        return None
    if [int(k) for k in extra_keys] != list(range(2, len(coins))):
        return None
    amounts.extend(additional[k] for k in extra_keys)

    chain = row.chain or ""
    parts: list[str] = []
    for i, sym in enumerate(coins):
        symbol = str(sym or "")
        raw = amounts[i] if i < len(amounts) else None
        if raw is None:
            amt_str = "—"
        else:
            # Preserve raw integers when decimals are unknown rather than
            # guessing display units.
            amt_str = _scale_lp_amount(raw, symbol, chain) or str(raw)
        parts.append(f"<code>{_e(amt_str)}</code> {_e(symbol)}")
    return " + ".join(parts)


def _format_lp_direction(row: TradeTapeRow, *, is_close: bool) -> tuple[str, str]:
    """Render the LP_OPEN / LP_CLOSE headline + (CLOSE only) fee sub-line.

    N-coin (>2) fungible pools render every pool coin from the receipt-parsed
    extracted data (VIB-5896 — see ``_format_ncoin_lp_direction``). Otherwise
    prefer the accounting payload's ``token0/token1/amount0/amount1`` —
    those are post-decoded human Decimals stamped at execution block. Fall
    back to the ledger ``token_in/amount_in/token_out/amount_out`` when the
    payload is absent (pre-VIB-3417 rows, accounting events that haven't
    landed yet, etc.) so the tape still renders something useful.
    """
    ncoin = _format_ncoin_lp_direction(row, is_close=is_close)
    if ncoin:
        # N-coin close fees are bundled into returned amounts, not separable.
        return ncoin, ""

    payload = _parse_accounting_payload(row)
    token0 = payload.get("token0") or row.token_in or ""
    token1 = payload.get("token1") or row.token_out or ""

    # Accounting payload amounts are human units; ledger fallbacks may be raw
    # integers, so only the latter use the magnitude heuristic.
    if payload.get("amount0") is not None:
        amt0_str = _format_human_amount(payload["amount0"])
    else:
        amt0_str = _format_lp_ledger_amount(row.amount_in, token0, row.chain)
    if payload.get("amount1") is not None:
        amt1_str = _format_human_amount(payload["amount1"])
    else:
        amt1_str = _format_lp_ledger_amount(row.amount_out, token1, row.chain)

    parts: list[str] = []
    if token0:
        parts.append(f"<code>{_e(amt0_str)}</code> {_e(token0)}")
    if token1:
        parts.append(f"<code>{_e(amt1_str)}</code> {_e(token1)}")
    direction = " + ".join(parts)

    if not is_close:
        return direction, ""

    fees0 = payload.get("fees0_collected")
    fees1 = payload.get("fees1_collected")
    fees_usd_raw = payload.get("fees_total_usd")
    has_token_fees = (
        _safe_decimal(str(fees0) if fees0 is not None else None) > 0
        or _safe_decimal(str(fees1) if fees1 is not None else None) > 0
    )
    fees_usd_d = _safe_decimal(str(fees_usd_raw) if fees_usd_raw is not None else None)
    if not has_token_fees and fees_usd_d <= 0:
        return direction, ""

    # Fees are payload-only (no ledger sibling), so always already-decoded.
    fee_parts: list[str] = []
    if fees0 is not None and token0:
        fee_parts.append(f"<code>{_e(_format_human_amount(fees0))}</code> {_e(token0)}")
    if fees1 is not None and token1:
        fee_parts.append(f"<code>{_e(_format_human_amount(fees1))}</code> {_e(token1)}")
    fee_body = " + ".join(fee_parts) if fee_parts else ""

    fee_usd_html = ""
    if fees_usd_d > 0:
        fee_usd_html = (
            f"<span style='color:#00c853;font-weight:600;margin-left:0.4rem;'>({_e(format_usd(fees_usd_d))})</span>"
        )

    if not fee_body and not fee_usd_html:
        return direction, ""

    return direction, (
        "<div style='margin-top:0.15rem;color:#bbb;font-size:0.86rem;'>"
        f"<span style='color:#888;'>fees collected:</span> {fee_body}{fee_usd_html}</div>"
    )


_registry_handle_from_payload = registry_handle_from_payload


def _render_tape_row(row: TradeTapeRow, *, show_approvals: bool) -> None:
    """Render a single tape row with its receipt-parsed expander."""
    icon = _INTENT_ICONS.get(row.intent_type, "•")
    chain_color = get_chain_color(row.chain) if row.chain else "#888888"
    chain_badge = format_chain_badge(row.chain, chain_color) if row.chain else ""

    # Headline status follows receipts, not the framework verdict, which may be
    # downgraded after landing. Unmeasured rows fall back to that verdict.
    landed, flagged_reason = _resolve_onchain_display_status(row)

    # ``None`` means nothing was submitted; unknown receipt status has already
    # fallen back to the framework verdict.
    onchain_ok: bool | None = None if _submitted_nothing(row) else (row.success if landed is None else landed)

    if onchain_ok is None:
        # A no-op has no transaction to grade. The UI uses its missing-value
        # glyph while CSV expresses the same status as an empty cell.
        success_marker = (
            "<span style='color:#888888;' title='No transaction was submitted (no-op) — nothing to grade'>—</span>"
        )
    else:
        success_marker = (
            "<span style='color:#00c853;'>✓</span>" if onchain_ok else "<span style='color:#f44336;'>✗</span>"
        )
    confidence_color, confidence_label = _CONFIDENCE_BADGES.get(row.confidence, ("#888888", _e(row.confidence) or ""))
    registry_handle = _registry_handle_from_payload(row.accounting_payload_json)

    # Resolve legs once for UI and CSV. A bundle's ledger hash may be a trailing
    # approval reset, so both surfaces identify the action leg explicitly.
    sub_txs = _resolve_legs(row)
    is_bundle = len(sub_txs) > 1
    action_tx = pick_action_tx(sub_txs, row.intent_type) if is_bundle else None
    headline_hash = (action_tx or {}).get("tx_hash") or row.tx_hash
    approvals_hidden = sum(1 for tx in sub_txs if is_approval_tx(tx)) if is_bundle else 0

    # Headlines normalize display units; raw amounts remain in receipt details.
    # LP legs move together, so ``+`` avoids implying a swap between them.
    direction, lp_fee_line = _format_direction(row)

    cost_bits = []
    if row.gas_usd:
        gas_d = _safe_decimal(row.gas_usd)
        if gas_d > 0:
            gas_text = f"gas {format_usd(gas_d)}"
            native = _format_native_gas(gas_d, row.chain, row.price_inputs_json)
            if native:
                gas_text = f"{gas_text} ({native})"
            cost_bits.append(_e(gas_text))
    if row.slippage_bps:
        cost_bits.append(f"slip {row.slippage_bps:.1f} bps")
    cost_line = " · ".join(cost_bits) if cost_bits else ""

    # Escape explorer URLs and isolate the new tab from the dashboard origin.
    tx_link = ""
    if headline_hash:
        url = get_block_explorer_url(row.chain or "ethereum", headline_hash)
        tx_link = (
            f"<a href='{_e(url)}' target='_blank' rel='noopener noreferrer' "
            f"style='color:#2196f3;text-decoration:none;font-family:monospace;font-size:0.85rem;'>"
            f"{_e(_short_hash(headline_hash))} ↗</a>"
        )

    count_badge = ""
    if is_bundle:
        if not show_approvals and approvals_hidden:
            visible = len(sub_txs) - approvals_hidden
            label = (
                f"{visible} of {len(sub_txs)} "
                f"({approvals_hidden} approval{'s' if approvals_hidden != 1 else ''} hidden)"
            )
        else:
            label = f"{len(sub_txs)} txs"
        count_badge = (
            f"<span style='background:#1f3a5f;color:#90caf9;border-radius:4px;"
            f"padding:1px 6px;font-size:0.72rem;margin-left:0.5rem;'>{_e(label)}</span>"
        )

    time_str = row.timestamp.strftime("%H:%M:%S") if row.timestamp else ""

    # Border color follows on-chain status; no-ops stay neutral, not false.
    intent_color = "#888888" if onchain_ok is None else ("#00c853" if onchain_ok else "#f44336")
    confidence_chip = ""
    if confidence_label:
        confidence_chip = (
            f"<span style='background:{confidence_color}22;color:{confidence_color};"
            f"border-radius:4px;padding:1px 6px;font-size:0.72rem;margin-left:0.5rem;'>"
            f"{confidence_label}</span>"
        )

    unavailable_chip = ""
    if row.unavailable_reason:
        unavailable_chip = (
            f"<div style='color:#ff9800;font-size:0.78rem;margin-top:0.2rem;'>⚠️ {_e(row.unavailable_reason)}</div>"
        )

    # Amber means the transaction landed but the framework downgraded it; red
    # means receipt failure. The tooltip retains the full ledger error.
    error_chip = ""
    if flagged_reason:
        full = (row.error or "").strip()
        error_chip = (
            f"<div style='color:#ff9800;font-size:0.82rem;margin-top:0.25rem;"
            f"font-family:monospace;word-break:break-word;' title='{_e(full)}'>"
            f"<span style='font-family:inherit;'>⚠</span> landed on-chain · flagged: {_e(flagged_reason)}</div>"
        )
    # Exclude the no-op sentinel explicitly while accepting bool-like false
    # values such as protobuf integer 0.
    elif onchain_ok is not None and not onchain_ok and row.error:
        full = row.error.strip()
        short = full if len(full) <= 200 else full[:197] + "…"
        error_chip = (
            f"<div style='color:#f44336;font-size:0.82rem;margin-top:0.25rem;"
            f"font-family:monospace;word-break:break-word;' title='{_e(full)}'>"
            f"<span style='font-family:inherit;'>⛔</span> {_e(short)}</div>"
        )

    # Keep one concatenated HTML block: CommonMark terminates raw HTML at
    # whitespace-only lines created by empty interpolations.
    # Registry handles distinguish multiple LP positions in one strategy.
    handle_chip = ""
    if registry_handle:
        handle_chip = (
            f"<span style='background:#2c3e50;color:#90caf9;border-radius:4px;"
            f"padding:1px 6px;font-size:0.72rem;margin-left:0.5rem;'"
            f" title='position_reference.registry_handle'>"
            f"·&nbsp;{_e(registry_handle)}</span>"
        )

    parts = [
        f'<div style="background:#161616;border:1px solid #2a2a2a;'
        f"border-left:3px solid {intent_color};border-radius:4px;"
        f'padding:0.6rem 0.9rem;margin-bottom:0.4rem;">',
        '<div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:0.5rem;">',
        '<div style="font-size:1.0rem;">',
        f'<span style="margin-right:0.4rem;">{success_marker}</span>',
        f'<span style="margin-right:0.4rem;">{_e(icon)}</span>',
        f'<strong style="font-size:1.05rem;">{_e(row.intent_type)}</strong>',
        chain_badge,
        f'<span style="color:#888;margin-left:0.5rem;font-size:0.82rem;">{_e(row.protocol)}</span>',
        handle_chip,
        confidence_chip,
        count_badge,
        "</div>",
        f'<div style="color:#888;font-size:0.82rem;">{_e(time_str)}</div>',
        "</div>",
        f'<div style="margin-top:0.25rem;color:#ccc;font-size:0.92rem;">{direction}</div>',
        lp_fee_line,
        '<div style="margin-top:0.2rem;color:#888;font-size:0.82rem;'
        'display:flex;justify-content:space-between;flex-wrap:wrap;gap:0.5rem;">',
        f"<span>{cost_line}</span>",
        f"<span>{tx_link}</span>",
        "</div>",
        error_chip,
        unavailable_chip,
        "</div>",
    ]
    st.markdown("".join(p for p in parts if p), unsafe_allow_html=True)

    # Streamlit expander labels are Markdown, so backticks provide monospace.
    with st.expander(
        f"▸ details · cycle `{row.cycle_id[:16]}…`" if row.cycle_id else "▸ details",
        expanded=False,
    ):
        _render_expander_blocks(row, sub_txs=sub_txs, show_approvals=show_approvals)


def _render_expander_blocks(
    row: TradeTapeRow,
    *,
    sub_txs: list[dict],
    show_approvals: bool,
) -> None:
    """Render the sub-blocks of the trade tape expander."""
    if len(sub_txs) > 1:
        _render_sub_tx_block(row, sub_txs, show_approvals=show_approvals)

    st.markdown("**Receipt-parsed data**")
    _render_receipt_block(row.extracted_data_json)

    st.markdown("**Oracle quotes used (price_inputs_json)**")
    if row.price_inputs_json:
        try:
            prices = json.loads(row.price_inputs_json)
            _render_oracle_block(prices)
        except (json.JSONDecodeError, TypeError):
            st.code(row.price_inputs_json, language="json")
    else:
        st.markdown(
            "<div style='color:#666;font-style:italic;'>no oracle quotes recorded for this intent</div>",
            unsafe_allow_html=True,
        )

    st.markdown("**Typed accounting payload**")
    if row.accounting_payload_json:
        try:
            payload = json.loads(row.accounting_payload_json)
            _render_kv_block(payload, prefix="accounting", primary=True)

            version_tags = []
            if row.schema_version:
                version_tags.append(f"schema v{row.schema_version}")
            if row.formula_version:
                version_tags.append(f"formula v{row.formula_version}")
            if row.matching_policy_version:
                version_tags.append(f"matching v{row.matching_policy_version}")
            if version_tags:
                st.markdown(
                    f"<div style='color:#888;font-size:0.78rem;margin-top:0.3rem;'>{' · '.join(version_tags)}</div>",
                    unsafe_allow_html=True,
                )
        except (json.JSONDecodeError, TypeError):
            st.code(row.accounting_payload_json, language="json")
    else:
        st.markdown(
            "<div style='color:#666;font-style:italic;'>"
            "no typed accounting event for this intent (likely a bookkeeping-only "
            "or legacy row)</div>",
            unsafe_allow_html=True,
        )

    pre_col, post_col = st.columns(2)
    with pre_col:
        st.markdown("**Pre-state (on-chain, before TX)**")
        _render_state_block(row.pre_state_json)
    with post_col:
        st.markdown("**Post-state (on-chain, after TX)**")
        _render_state_block(row.post_state_json)

    if row.position_event_json and row.position_event_type:
        st.markdown(
            f"**Linked position event** &nbsp;"
            f"<code>{_e(row.position_event_type)}</code> &nbsp;"
            f"<code>{_e(row.position_id[:16])}…</code>"
            if row.position_id
            else f"**Linked position event** &nbsp;<code>{_e(row.position_event_type)}</code>",
            unsafe_allow_html=True,
        )
        try:
            pe = json.loads(row.position_event_json)
            pe_visible = _filter_position_event_fields(pe) if isinstance(pe, dict) else pe
            _render_kv_block(
                pe_visible,
                prefix="position_event",
                context=_kv_context_for_position_event(pe if isinstance(pe, dict) else {}, row),
            )
        except (json.JSONDecodeError, TypeError):
            st.code(row.position_event_json, language="json")


def _render_sub_tx_block(
    row: TradeTapeRow,
    sub_txs: list[dict],
    *,
    show_approvals: bool,
) -> None:
    """Render the sub-transaction breakdown for a multi-tx bundle (VIB-4046).

    One row per sub-tx with: explorer link, gas, status, and a
    selector-decoded label. When ``show_approvals`` is False, ERC-20
    ``approve`` sub-txs are filtered out and a "(N hidden)" hint is
    shown so the operator knows the table is incomplete by choice.
    """
    visible = sub_txs if show_approvals else [tx for tx in sub_txs if not is_approval_tx(tx)]
    hidden = len(sub_txs) - len(visible)

    header = f"**Sub-transactions** &nbsp;<span style='color:#888;font-weight:normal;'>{len(visible)} of {len(sub_txs)}"
    if hidden:
        header += f" &middot; {hidden} approval{'s' if hidden != 1 else ''} hidden — toggle 'Show approvals' to expand"
    header += "</span>"
    st.markdown(header, unsafe_allow_html=True)

    if not visible:
        st.markdown(
            "<div style='color:#666;font-style:italic;font-size:0.84rem;'>"
            "All sub-txs are approvals — toggle 'Show approvals' to see them.</div>",
            unsafe_allow_html=True,
        )
        return

    table_rows = []
    chain = row.chain or "ethereum"
    for idx, tx in enumerate(sub_txs, start=1):
        if not show_approvals and is_approval_tx(tx):
            continue
        tx_hash = tx.get("tx_hash") or ""
        gas_used = _coerce_gas(tx.get("gas_used"))
        leg_landed = _leg_landed(tx)
        selector = tx.get("function_selector") or ""
        label = decode_selector(selector) if selector else ("approve" if is_approval_tx(tx) else "action")

        link_html = "—"
        if tx_hash:
            url = get_block_explorer_url(chain, tx_hash)
            link_html = (
                f"<a href='{_e(url)}' target='_blank' rel='noopener noreferrer' "
                f"style='color:#2196f3;text-decoration:none;font-family:monospace;'>"
                f"{_e(_short_hash(tx_hash))} ↗</a>"
            )
        # Missing receipt status is unmeasured, not failed.
        if leg_landed is None:
            status_html = "<span style='color:#888;' title='on-chain status not measured'>–</span>"
        elif leg_landed:
            status_html = "<span style='color:#00c853;'>✓</span>"
        else:
            status_html = "<span style='color:#f44336;'>✗</span>"

        table_rows.append(
            "<tr>"
            f"<td style='padding:2px 6px;color:#888;'>{idx}</td>"
            f"<td style='padding:2px 6px;'>{status_html}</td>"
            f"<td style='padding:2px 6px;color:#90caf9;font-family:monospace;font-size:0.82rem;'>"
            f"{_e(label)}</td>"
            f"<td style='padding:2px 6px;color:#bbb;font-family:monospace;font-size:0.82rem;'>"
            f"{gas_used:,}</td>"
            f"<td style='padding:2px 6px;'>{link_html}</td>"
            "</tr>"
        )

    st.markdown(
        "<div style='background:#1a1a1a;border-radius:4px;padding:0.4rem;'>"
        "<table style='width:100%;border-collapse:collapse;font-size:0.84rem;'>"
        "<thead><tr style='color:#888;text-align:left;'>"
        "<th style='padding:2px 6px;'>#</th>"
        "<th style='padding:2px 6px;'></th>"
        "<th style='padding:2px 6px;'>action</th>"
        "<th style='padding:2px 6px;'>gas</th>"
        "<th style='padding:2px 6px;'>tx</th>"
        "</tr></thead><tbody>"
        f"{''.join(table_rows)}"
        "</tbody></table></div>",
        unsafe_allow_html=True,
    )


# Position events share one LP/PERP schema; omit inapplicable NULL fields so
# they are not mistaken for unmeasured values.
_POSITION_EVENT_LP_FIELDS: frozenset[str] = frozenset(
    {
        "token0",
        "token1",
        "amount0",
        "amount1",
        "tick_lower",
        "tick_upper",
        "liquidity",
        "in_range",
        "fees_token0",
        "fees_token1",
    }
)
_POSITION_EVENT_PERP_FIELDS: frozenset[str] = frozenset(
    {
        "leverage",
        "entry_price",
        "mark_price",
        "unrealized_pnl",
        "is_long",
    }
)
_POSITION_EVENT_SHARED_FIELDS: frozenset[str] = frozenset(
    {
        "id",
        "deployment_id",
        "cycle_id",
        "position_id",
        "position_type",
        "event_type",
        "timestamp",
        "protocol",
        "chain",
        "value_usd",
        "tx_hash",
        "gas_usd",
        "ledger_entry_id",
        "protocol_fees_usd",
        "attribution_json",
        "attribution_version",
        # Execution mode distinguishes live, paper, and dry-run audit records.
        "execution_mode",
    }
)

# Position-event LP amounts are raw integers; each field maps to the token
# whose decimals determine display units.
_LP_AMOUNT_FIELDS_TOKEN0: frozenset[str] = frozenset(
    {
        "amount0",
        "fees_token0",
        "fees0_collected",
    }
)
_LP_AMOUNT_FIELDS_TOKEN1: frozenset[str] = frozenset(
    {
        "amount1",
        "fees_token1",
        "fees1_collected",
    }
)

# These events cannot produce protocol fees; hide empty fields rather than
# implying a measured zero.
_NO_PROTOCOL_FEES_EVENT_TYPES: frozenset[str] = frozenset(
    {
        "OPEN",
        "ADJUST",
        "LP_OPEN",
        "PERP_OPEN",
    }
)


@dataclass(frozen=True)
class KVContext:
    """Per-block context threaded into ``_render_kv_block``.

    Centralizes the bits of row metadata the kv-block needs to make
    rendering decisions: the event_type (for the protocol_fees_usd
    hiding policy), and chain/token symbols (for scaling raw integer
    amounts via the token resolver). All fields default to empty so
    call sites that don't need scaling can omit the context entirely.
    """

    event_type: str = ""
    chain: str = ""
    token0: str = ""
    token1: str = ""


def _kv_context_for_position_event(pe: dict[str, Any], row: TradeTapeRow) -> KVContext:
    """Build a :class:`KVContext` from a position_event dict and its ledger row."""
    return KVContext(
        event_type=str(pe.get("event_type") or row.position_event_type or "").upper(),
        chain=str(pe.get("chain") or row.chain or ""),
        token0=str(pe.get("token0") or ""),
        token1=str(pe.get("token1") or ""),
    )


def _filter_position_event_fields(pe: dict[str, Any]) -> dict[str, Any]:
    """Drop PERP fields from an LP position_event (and vice-versa).

    ``position_events`` is a unified LP+PERP table; the columns that don't
    apply to the row's ``position_type`` are NULL. Rendering them as
    blanks misleads operators (Bug 2 on the position panel — an LP row
    showed ``leverage:`` ``entry_price:`` ``mark_price:`` ``is_long: null``).
    """
    position_type = str(pe.get("position_type") or "").upper()
    if position_type == "LP":
        kept = _POSITION_EVENT_SHARED_FIELDS | _POSITION_EVENT_LP_FIELDS
    elif position_type == "PERP":
        kept = _POSITION_EVENT_SHARED_FIELDS | _POSITION_EVENT_PERP_FIELDS
    else:
        # Unknown types retain all fields to avoid dropping future schema data.
        return pe
    return {k: v for k, v in pe.items() if k in kept}


def _scale_lp_amount(raw: Any, symbol: str, chain: str) -> str | None:
    """Scale a raw on-chain integer amount to human units using the resolver.

    Returns ``None`` when scaling cannot be done (no decimals, non-integer
    value, missing symbol/chain) — caller falls back to the raw string.
    """
    if raw in (None, ""):
        return None
    try:
        d = Decimal(str(raw))
    except (ArithmeticError, ValueError, TypeError):
        return None
    if not d.is_finite() or d != d.to_integral_value():
        return None
    if not (symbol and chain):
        return None
    decimals = _try_token_decimals(symbol, chain)
    if decimals is None or decimals <= 0:
        return None
    return _format_human_amount(d / (Decimal(10) ** decimals))


def _format_scalar_kv_value(k: str, v: Any, ctx: KVContext) -> str | None:
    """Return the inner-``<span>`` HTML for a (k, v) row, or None to hide it.

    Encapsulates the two value-shape policies for the kv block:

    * **Bug 4** — empty/None ``protocol_fees_usd`` is rendered as
      "unmeasured" for events that *can* produce fees, and hidden entirely
      for OPEN / ADJUST events (which can't, by definition). Never
      substituted with ``0`` (AGENTS §Accounting: Empty ≠ Zero).
    * **Bug 5** — raw on-chain integer LP amounts (``amount0`` / ``amount1``
      / ``fees_token0`` / ``fees_token1`` / ``fees{0,1}_collected``) are
      scaled via the token resolver and labelled with their symbol so the
      same logical value is no longer rendered raw on one panel and scaled
      on an adjacent panel of the same card.
    """
    if k == "protocol_fees_usd" and (v is None or v == ""):
        if ctx.event_type in _NO_PROTOCOL_FEES_EVENT_TYPES:
            return None
        return (
            "<span style='color:#888;font-style:italic;' "
            "title='Empty ≠ Zero — this fee category was not measured "
            "for this event'>unmeasured</span>"
        )

    if k in _LP_AMOUNT_FIELDS_TOKEN0:
        scaled = _scale_lp_amount(v, ctx.token0, ctx.chain)
        if scaled is not None:
            return f"{_e(scaled)} <span style='color:#888;'>{_e(ctx.token0)}</span>"
    elif k in _LP_AMOUNT_FIELDS_TOKEN1:
        scaled = _scale_lp_amount(v, ctx.token1, ctx.chain)
        if scaled is not None:
            return f"{_e(scaled)} <span style='color:#888;'>{_e(ctx.token1)}</span>"

    return _format_value(v)


def _render_kv_block(
    data: Any,
    *,
    prefix: str = "",
    primary: bool = False,
    indent: int = 0,
    context: KVContext | None = None,
) -> None:
    """Render a dict / list as a borderless monospace key-value block.

    Nested dict/list values are split out into ``st.json`` widgets so the
    operator can expand / collapse them rather than reading a 120-char
    truncated preview (Bug 7).
    """
    ctx = context or KVContext()
    if isinstance(data, dict):
        scalar_items: list[tuple[str, Any]] = []
        nested_items: list[tuple[str, Any]] = []
        for k, v in data.items():
            if k.startswith("_"):
                continue
            if isinstance(v, dict | list):
                nested_items.append((k, v))
            else:
                scalar_items.append((k, v))

        rows_html: list[str] = []
        for k, v in scalar_items:
            v_repr = _format_scalar_kv_value(k, v, ctx)
            if v_repr is None:
                continue
            k_repr = _e(k)
            color = "#ddd" if primary else "#bbb"
            highlight = ""
            if k.endswith("_usd"):
                highlight = "color:#00c853;font-weight:600;"
            elif k == "unavailable_reason" and v:
                highlight = "color:#ff9800;"
            elif k in ("event_type", "asset", "protocol"):
                highlight = "color:#2196f3;font-weight:600;"
            rows_html.append(
                f"<div style='font-family:monospace;font-size:0.84rem;color:{color};'>"
                f"<span style='color:#888;'>{k_repr}:</span> "
                f"<span style='{highlight}'>{v_repr}</span></div>"
            )

        if rows_html:
            st.markdown(
                "<div style='background:#1a1a1a;border-radius:4px;padding:0.5rem 0.75rem;'>"
                + "".join(rows_html)
                + "</div>",
                unsafe_allow_html=True,
            )

        for k, v in nested_items:
            st.markdown(
                f"<div style='font-family:monospace;font-size:0.84rem;color:#888;margin-top:0.25rem;'>{_e(k)}:</div>",
                unsafe_allow_html=True,
            )
            st.json(v, expanded=False)
    elif isinstance(data, list):
        for i, item in enumerate(data):
            st.markdown(f"**[{i}]**")
            _render_kv_block(
                item,
                prefix=f"{prefix}[{i}]",
                indent=indent + 1,
                context=context,
            )
    else:
        st.code(str(data), language="text")


def _format_value(v: Any) -> str:
    """Return an HTML-safe representation of a JSON scalar value for the kv block.

    Dict / list values are handled separately by ``_render_kv_block``
    (which delegates to ``st.json`` for expand/collapse); this helper is
    now scalar-only. The dict/list branch is kept as a defensive fallback
    for callers that bypass ``_render_kv_block`` and feed values directly.
    All gateway-sourced strings flowing through here are escaped before
    being interpolated into ``st.markdown(unsafe_allow_html=True)``.
    """
    if v is None:
        return "<span style='color:#666;'>null</span>"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, dict | list):
        try:
            return f"<code>{_e(json.dumps(v, default=str)[:120])}</code>"
        except Exception:  # noqa: BLE001
            return "<code>{...}</code>"
    s = str(v)
    pretty = _prettify_iso_datetime(s)
    if pretty is not None:
        return _e(pretty)
    if len(s) > 100:
        s = s[:97] + "…"
    return _e(s)


def _prettify_iso_datetime(s: str) -> str | None:
    """Reformat ISO-8601 timestamps in kv blocks to a human-readable form.

    The gateway serializes ``datetime`` fields on ``position_event`` /
    accounting payloads via ``.isoformat()``, which renders as e.g.
    ``2026-05-05T08:48:37.831059+00:00`` — unscannable for an operator.
    Reformat to ``2026-05-05 08:48:37 UTC`` (drops microseconds, swaps
    ``T`` for a space, and resolves ``+00:00`` to ``UTC``).

    Returns ``None`` when the string is not a parseable ISO timestamp,
    so the caller falls back to the generic str path.
    """
    if not (19 <= len(s) <= 40) or "T" not in s[:11] or " " in s:
        return None
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    out = dt.strftime("%Y-%m-%d %H:%M:%S")
    if dt.tzinfo is None:
        return out
    offset = dt.utcoffset()
    if offset is not None and offset.total_seconds() == 0:
        return f"{out} UTC"
    return f"{out} {dt.strftime('%z')}"


def _render_oracle_block(prices: Any) -> None:
    """Format a price_inputs_json payload — typically {symbol: {price_usd, source}}."""
    if not isinstance(prices, dict):
        st.code(json.dumps(prices, indent=2), language="json")
        return
    rows = []
    for symbol, info in prices.items():
        if isinstance(info, dict):
            price = info.get("price_usd") or info.get("price") or "—"
            source = info.get("oracle_source") or info.get("source") or info.get("provider") or "—"
            rows.append(
                f"<tr>"
                f"<td style='color:#2196f3;padding:2px 6px;'>{_e(symbol)}</td>"
                f"<td style='color:#00c853;font-family:monospace;padding:2px 6px;'>${_e(price)}</td>"
                f"<td style='color:#888;padding:2px 6px;'>{_e(source)}</td>"
                f"</tr>"
            )
        else:
            rows.append(
                f"<tr>"
                f"<td style='color:#2196f3;padding:2px 6px;'>{_e(symbol)}</td>"
                f"<td style='color:#888;padding:2px 6px;' colspan='2'>{_e(info)}</td>"
                f"</tr>"
            )
    if rows:
        st.markdown(
            "<div style='background:#1a1a1a;border-radius:4px;padding:0.4rem;'>"
            "<table style='width:100%;border-collapse:collapse;font-size:0.84rem;'>"
            f"{''.join(rows)}"
            "</table></div>",
            unsafe_allow_html=True,
        )


def _render_receipt_block(extracted_data_json: str) -> None:
    """Render the receipt-parsed extracted_data dict.

    ``all_tx_results`` is shown structurally by ``_render_sub_tx_block``
    above, so we strip it from the raw kv view to avoid duplicating
    the same data twice in the same expander.
    """
    if not extracted_data_json:
        st.markdown(
            "<div style='color:#666;font-style:italic;'>no receipt-parsed data on this row</div>",
            unsafe_allow_html=True,
        )
        return
    try:
        data = json.loads(extracted_data_json)
    except (json.JSONDecodeError, TypeError):
        st.code(extracted_data_json or "—", language="text")
        return
    # Remove duplicated bundle details only when the sub-tx table renders them;
    # retain a single leg in the raw receipt view.
    if isinstance(data, dict):
        legs = data.get("all_tx_results")
        if isinstance(legs, list) and len(legs) > 1:
            data.pop("all_tx_results", None)
    if isinstance(data, dict) and not data:
        st.markdown(
            "<div style='color:#666;font-style:italic;'>"
            "(other receipt fields rendered in the sub-transactions table above)"
            "</div>",
            unsafe_allow_html=True,
        )
        return
    _render_kv_block(data, prefix="extracted_data")


def _render_state_block(state_json: str) -> None:
    """Render pre/post state JSON, or the unavailable_reason if NULL."""
    if not state_json:
        st.markdown(
            "<div style='color:#ff9800;font-style:italic;font-size:0.84rem;'>"
            "NULL — connector pre/post-state pipeline not wired"
            "</div>",
            unsafe_allow_html=True,
        )
        return
    try:
        state = json.loads(state_json)
        _render_kv_block(state, prefix="state")
    except (json.JSONDecodeError, TypeError):
        st.code(state_json, language="json")
