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
from datetime import datetime
from decimal import Decimal
from typing import Any

import streamlit as st

from almanak.framework.dashboard.gateway_client import TradeTapeRow
from almanak.framework.dashboard.theme import get_chain_color
from almanak.framework.dashboard.utils import (
    format_chain_badge,
    format_token_amount,
    format_usd,
    get_block_explorer_url,
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


def render_trade_tape(strategy_id: str, *, limit: int = 50) -> None:
    """Render the trade-tape tab for a strategy."""
    from almanak.framework.dashboard.data_source import (
        GatewayConnectionError,
        get_trade_tape,
    )

    try:
        response = get_trade_tape(strategy_id, limit=limit)
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

    # Top-level filters
    col1, col2, col3 = st.columns([2, 2, 2])
    with col1:
        intent_types = sorted({row.intent_type for row in response.rows if row.intent_type})
        selected_intents = st.multiselect(
            "Intent types",
            intent_types,
            default=intent_types,
            key=f"tape_intents_{strategy_id}",
        )
    with col2:
        success_filter = st.selectbox(
            "Status",
            ["All", "Success only", "Failures only"],
            key=f"tape_status_{strategy_id}",
        )
    with col3:
        confidence_filter = st.selectbox(
            "Confidence",
            ["All", "HIGH only", "Include ESTIMATED/STALE", "UNAVAILABLE only"],
            key=f"tape_confidence_{strategy_id}",
        )

    rows = [r for r in response.rows if r.intent_type in selected_intents]
    if success_filter == "Success only":
        rows = [r for r in rows if r.success]
    elif success_filter == "Failures only":
        rows = [r for r in rows if not r.success]
    if confidence_filter == "HIGH only":
        rows = [r for r in rows if r.confidence == "HIGH"]
    elif confidence_filter == "Include ESTIMATED/STALE":
        rows = [r for r in rows if r.confidence in ("HIGH", "ESTIMATED", "STALE")]
    elif confidence_filter == "UNAVAILABLE only":
        rows = [r for r in rows if r.confidence == "UNAVAILABLE"]

    if not rows:
        st.info("No rows match the current filters.")
        return

    # VIB-3928 — CSV export of the filtered tape. Tax / audit ask the
    # team has had open since April; the tape carries everything a
    # quant needs (token amounts, USD value, gas, slippage, oracle
    # source, position id) so a single CSV download replaces a manual
    # SQL pull from sqlite. Only the currently-filtered rows are
    # exported so the operator can scope the file before downloading.
    _render_csv_export(rows, strategy_id)

    # Group by date for scannability
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
        _render_tape_row(row)


def _render_csv_export(rows: list[TradeTapeRow], strategy_id: str) -> None:
    """VIB-3928 — render a single download button for the filtered tape."""
    import csv as _csv
    import io

    buf = io.StringIO()
    writer = _csv.writer(buf, quoting=_csv.QUOTE_MINIMAL)
    writer.writerow(
        [
            "timestamp",
            "cycle_id",
            "intent_type",
            "success",
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
            "gas_used",
            "gas_usd",
            "tx_hash",
            "confidence",
            "oracle_source",
            "position_id",
            "primary_risk_metric",
        ]
    )
    for r in rows:
        writer.writerow(
            [
                r.timestamp.isoformat() if r.timestamp else "",
                r.cycle_id or "",
                r.intent_type or "",
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
                str(r.gas_used or ""),
                str(r.gas_usd or ""),
                r.tx_hash or "",
                r.confidence or "",
                getattr(r, "oracle_source", "") or "",
                getattr(r, "position_id", "") or "",
                getattr(r, "primary_risk_metric", "") or "",
            ]
        )

    csv_bytes = buf.getvalue().encode("utf-8")
    fname = f"trade_tape_{strategy_id[:32]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    st.download_button(
        label=f"⬇️ Export {len(rows)} row(s) as CSV",
        data=csv_bytes,
        file_name=fname,
        mime="text/csv",
        key=f"tape_csv_{strategy_id}",
        help=(
            "Joined trade-tape view: transaction_ledger × accounting_events × "
            "position_events. Filtered rows only. Honest empty cells where "
            "data is unavailable (per CONF / VIB-3886)."
        ),
    )


def _render_tape_row(row: TradeTapeRow) -> None:
    """Render a single tape row with its receipt-parsed expander."""
    icon = _INTENT_ICONS.get(row.intent_type, "•")
    chain_color = get_chain_color(row.chain) if row.chain else "#888888"
    chain_badge = format_chain_badge(row.chain, chain_color) if row.chain else ""
    success_marker = "<span style='color:#00c853;'>✓</span>" if row.success else "<span style='color:#f44336;'>✗</span>"
    confidence_color, confidence_label = _CONFIDENCE_BADGES.get(row.confidence, ("#888888", _e(row.confidence) or ""))

    # Direction line: token_in → token_out (when applicable).
    # VIB-3890: ``format_token_amount`` normalises raw on-chain integers
    # (LP_OPEN amount0/1 in 18-dec / 6-dec) and full-precision Decimals
    # (SWAP amount_out 0.000868768309352546) into a Quant-readable
    # headline. Raw audit-grade amounts remain in the receipt-parsed
    # expander block.
    direction = ""
    if row.token_in or row.token_out:
        amt_in = format_token_amount(row.amount_in, row.token_in, row.chain)
        amt_out = format_token_amount(row.amount_out, row.token_out, row.chain)
        in_part = f"<code>{_e(amt_in)}</code> {_e(row.token_in)}" if row.token_in else ""
        out_part = f"<code>{_e(amt_out)}</code> {_e(row.token_out)}" if row.token_out else ""
        if in_part and out_part:
            direction = f"{in_part} → {out_part}"
        else:
            direction = in_part or out_part

    # Cost line
    cost_bits = []
    if row.gas_usd:
        gas_d = _safe_decimal(row.gas_usd)
        if gas_d > 0:
            cost_bits.append(f"gas {_e(format_usd(gas_d))}")
    if row.slippage_bps:
        cost_bits.append(f"slip {row.slippage_bps:.1f} bps")
    cost_line = " · ".join(cost_bits) if cost_bits else ""

    # tx hash link — escape href + add rel="noopener noreferrer" so the
    # block-explorer link can't be hijacked into a same-origin window
    tx_link = ""
    if row.tx_hash:
        url = get_block_explorer_url(row.chain or "ethereum", row.tx_hash)
        tx_link = (
            f"<a href='{_e(url)}' target='_blank' rel='noopener noreferrer' "
            f"style='color:#2196f3;text-decoration:none;font-family:monospace;font-size:0.85rem;'>"
            f"{_e(_short_hash(row.tx_hash))} ↗</a>"
        )

    # Time
    time_str = row.timestamp.strftime("%H:%M:%S") if row.timestamp else ""

    # Headline card
    intent_color = "#00c853" if row.success else "#f44336"
    confidence_chip = ""
    if confidence_label:
        # confidence_color comes from a hardcoded map; confidence_label
        # is either a map-looked-up label or the gateway-supplied
        # confidence string fallback (already escaped above).
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

    # icon and confidence_label/color are looked up from constant maps;
    # row.intent_type / row.protocol / time_str / chain_badge come from
    # the gateway and must be escaped before HTML interpolation.
    st.markdown(
        f"""
        <div style="background:#161616;border:1px solid #2a2a2a;
                    border-left:3px solid {intent_color};
                    border-radius:4px;padding:0.6rem 0.9rem;
                    margin-bottom:0.4rem;">
          <div style="display:flex;justify-content:space-between;align-items:flex-start;
                      flex-wrap:wrap;gap:0.5rem;">
            <div style="font-size:1.0rem;">
              <span style="margin-right:0.4rem;">{success_marker}</span>
              <span style="margin-right:0.4rem;">{_e(icon)}</span>
              <strong style="font-size:1.05rem;">{_e(row.intent_type)}</strong>
              {chain_badge}
              <span style="color:#888;margin-left:0.5rem;font-size:0.82rem;">
                {_e(row.protocol)}
              </span>
              {confidence_chip}
            </div>
            <div style="color:#888;font-size:0.82rem;">{_e(time_str)}</div>
          </div>
          <div style="margin-top:0.25rem;color:#ccc;font-size:0.92rem;">
            {direction}
          </div>
          <div style="margin-top:0.2rem;color:#888;font-size:0.82rem;
                      display:flex;justify-content:space-between;flex-wrap:wrap;gap:0.5rem;">
            <span>{cost_line}</span>
            <span>{tx_link}</span>
          </div>
          {unavailable_chip}
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Expander with the four data blocks. ``st.expander`` renders its label
    # as Markdown, not raw HTML — use backticks for inline code formatting
    # so the cycle id renders monospace instead of as the literal text.
    with st.expander(
        f"▸ details · cycle `{row.cycle_id[:16]}…`" if row.cycle_id else "▸ details",
        expanded=False,
    ):
        _render_expander_blocks(row)


def _render_expander_blocks(row: TradeTapeRow) -> None:
    """Render the four sub-blocks of the trade tape expander."""
    block_col1, block_col2 = st.columns(2)

    # Block 1 — Receipt-parsed extracted data (left column, top)
    with block_col1:
        st.markdown("**Receipt-parsed data**")
        if row.extracted_data_json:
            try:
                data = json.loads(row.extracted_data_json)
                _render_kv_block(data, prefix="extracted_data")
            except (json.JSONDecodeError, TypeError):
                st.code(row.extracted_data_json or "—", language="text")
        else:
            st.markdown(
                "<div style='color:#666;font-style:italic;'>no receipt-parsed data on this row</div>",
                unsafe_allow_html=True,
            )

    # Block 2 — Oracle quotes used (right column, top)
    with block_col2:
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

    # Block 3 — Accounting payload (full width)
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
            "or pre-VIB-3417 row)</div>",
            unsafe_allow_html=True,
        )

    # Block 4 — Pre/post on-chain state (two columns)
    pre_col, post_col = st.columns(2)
    with pre_col:
        st.markdown("**Pre-state (on-chain, before TX)**")
        _render_state_block(row.pre_state_json)
    with post_col:
        st.markdown("**Post-state (on-chain, after TX)**")
        _render_state_block(row.post_state_json)

    # Block 5 — Linked position event (LP/PERP only)
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
            _render_kv_block(pe, prefix="position_event")
        except (json.JSONDecodeError, TypeError):
            st.code(row.position_event_json, language="json")


def _render_kv_block(
    data: Any,
    *,
    prefix: str = "",
    primary: bool = False,
    indent: int = 0,
) -> None:
    """Render a dict / list as a borderless monospace key-value block."""
    if isinstance(data, dict):
        rows_html = []
        for k, v in data.items():
            if k.startswith("_"):
                continue
            # ``v_repr`` is already HTML-escaped by ``_format_value``.
            # The key ``k`` is dict-derived from receipt-parsed data on
            # the gateway side and must also be escaped before
            # interpolating into ``unsafe_allow_html``.
            v_repr = _format_value(v)
            k_repr = _e(k)
            color = "#ddd" if primary else "#bbb"
            highlight = ""
            if k.endswith("_usd") and isinstance(v_repr, str):
                highlight = "color:#00c853;font-weight:600;"
            elif k in ("unavailable_reason",) and v:
                highlight = "color:#ff9800;"
            elif k in ("event_type", "asset", "protocol"):
                highlight = "color:#2196f3;font-weight:600;"
            rows_html.append(
                f"<div style='font-family:monospace;font-size:0.84rem;color:{color};'>"
                f"<span style='color:#888;'>{k_repr}:</span> "
                f"<span style='{highlight}'>{v_repr}</span></div>"
            )
        st.markdown(
            "<div style='background:#1a1a1a;border-radius:4px;padding:0.5rem 0.75rem;'>"
            + "".join(rows_html)
            + "</div>",
            unsafe_allow_html=True,
        )
    elif isinstance(data, list):
        for i, item in enumerate(data):
            st.markdown(f"**[{i}]**")
            _render_kv_block(item, prefix=f"{prefix}[{i}]", indent=indent + 1)
    else:
        st.code(str(data), language="text")


def _format_value(v: Any) -> str:
    """Return an HTML-safe representation of a JSON value for the kv block.

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
    if len(s) > 100:
        s = s[:97] + "…"
    return _e(s)


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


def _render_state_block(state_json: str) -> None:
    """Render pre/post state JSON, or the unavailable_reason if NULL."""
    if not state_json:
        st.markdown(
            "<div style='color:#ff9800;font-style:italic;font-size:0.84rem;'>"
            "NULL — connector pre/post-state pipeline not wired (VIB-3474 pending)"
            "</div>",
            unsafe_allow_html=True,
        )
        return
    try:
        state = json.loads(state_json)
        _render_kv_block(state, prefix="state")
    except (json.JSONDecodeError, TypeError):
        st.code(state_json, language="json")
