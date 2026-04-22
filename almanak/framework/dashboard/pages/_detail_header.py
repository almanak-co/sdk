"""Header and key-metrics renderers for the strategy detail page.

Extracted from ``detail.py`` (Phase 5b of the Dashboard refactor plan) to isolate
the header chrome (title + status badge, chain/protocol info row) and the
four-column key-metrics row from the surrounding page layout. All functions in
this module are pure Streamlit UI renderers with no I/O, which makes them
``AppTest``-able without a running gateway.

Public helpers:
    * ``render_strategy_header`` - title + status badge (phase 2a).
    * ``render_chain_info_row`` - single-chain or multi-chain info display
      including chain-health indicators for multi-chain strategies (phase 2b).
    * ``render_key_metrics`` - total-value / 24h-PnL (net) / LP-or-health /
      7d-PnL four-column metric row (phase 4).

Behaviour is preserved verbatim - including known latent issues tracked in
issues #1711-#1716 - so this module is a pure refactor.
"""

from __future__ import annotations

import streamlit as st

from almanak.framework.dashboard.models import Strategy
from almanak.framework.dashboard.theme import get_chain_color, get_status_color
from almanak.framework.dashboard.utils import (
    format_chain_badge,
    format_usd,
    get_status_icon,
)


def render_strategy_header(strategy: Strategy) -> None:
    """Render the strategy title and status badge.

    Emits a single inline-HTML block containing the strategy name as an ``h2``
    and a coloured status badge (icon + status value). Mirrors the original
    inline block in ``detail.page`` verbatim.

    Args:
        strategy: The strategy whose detail page is being rendered.
    """
    status_icon = get_status_icon(strategy.status)
    status_color = get_status_color(strategy.status)

    st.markdown(
        f"""
        <div style="display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem;">
            <h2 style="margin: 0;">{strategy.name}</h2>
            <span style="
                background-color: {status_color}22;
                color: {status_color};
                padding: 0.25rem 0.75rem;
                border-radius: 16px;
                font-weight: bold;
            ">{status_icon} {strategy.status.value}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_chain_info_row(strategy: Strategy) -> None:
    """Render the chain / protocol / last-action info row.

    Branches between the multi-chain display (chain badges, chain health
    indicators, two-column protocol + last-action row) and the single-chain
    display (three-column chain badge + protocol + last-action row). Calls
    ``render_chain_health_indicators`` from ``detail`` for the multi-chain
    health panel to avoid duplicating that helper.

    Args:
        strategy: The strategy whose detail page is being rendered.
    """
    # Imported lazily to avoid a circular import with ``detail`` (which
    # imports this module for delegation).
    from almanak.framework.dashboard.pages.detail import render_chain_health_indicators

    if strategy.is_multi_chain and strategy.chains:
        chain_badges_html = ""
        for chain in strategy.chains:
            chain_color = get_chain_color(chain)
            chain_badges_html += format_chain_badge(chain, chain_color)

        st.markdown(
            f"""
            <div style="display: flex; align-items: center; gap: 1rem; margin-bottom: 0.5rem;">
                <strong>Chains:</strong> {chain_badges_html}
            </div>
            """,
            unsafe_allow_html=True,
        )

        render_chain_health_indicators(strategy)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Protocols:** {strategy.protocol}")
        with col2:
            if strategy.last_action_at:
                st.markdown(f"**Last Action:** {strategy.last_action_at.strftime('%Y-%m-%d %H:%M')}")
        return

    col1, col2, col3 = st.columns(3)
    with col1:
        chain_color = get_chain_color(strategy.chain)
        chain_badge = format_chain_badge(strategy.chain, chain_color)
        st.markdown(f"**Chain:** {chain_badge}", unsafe_allow_html=True)
    with col2:
        st.markdown(f"**Protocol:** {strategy.protocol}")
    with col3:
        if strategy.last_action_at:
            st.markdown(f"**Last Action:** {strategy.last_action_at.strftime('%Y-%m-%d %H:%M')}")


def render_key_metrics(strategy: Strategy) -> None:
    """Render the four-column key-metrics row.

    Columns (left to right):
        1. Total Value - with a ``value_confidence`` indicator in the ``help``
           tooltip when confidence is anything other than ``HIGH``.
        2. 24h PnL (Net) - the 24h PnL minus bridge fees; ``help`` shows the
           bridge-fee deduction when non-zero.
        3. LP Value / Health Factor / "Positions N/A" - the first non-zero
           candidate wins.
        4. 7d PnL - last entry of ``pnl_history`` (absolute value + signed
           delta). Rendered only when ``pnl_history`` is non-empty.

    Args:
        strategy: The strategy whose detail page is being rendered.
    """
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        confidence = getattr(strategy, "value_confidence", None)
        if confidence and confidence != "HIGH":
            confidence_icons = {
                "ESTIMATED": "\u26a0\ufe0f",  # Warning sign
                "STALE": "\u23f0",  # Alarm clock
                "UNAVAILABLE": "\u2753",  # Question mark
            }
            confidence_icon = confidence_icons.get(confidence, "")
            st.metric(
                "Total Value",
                format_usd(strategy.total_value_usd),
                help=f"Value confidence: {confidence} {confidence_icon}",
            )
        else:
            st.metric("Total Value", format_usd(strategy.total_value_usd))

    with col2:
        net_pnl = strategy.pnl_24h_usd - strategy.bridge_fees_usd
        pnl_delta = f"{'+' if net_pnl >= 0 else ''}{net_pnl:,.2f}"
        st.metric(
            "24h PnL (Net)",
            format_usd(abs(net_pnl)),
            delta=pnl_delta,
            help=f"Includes ${strategy.bridge_fees_usd:,.2f} in bridge fees" if strategy.bridge_fees_usd > 0 else None,
        )

    with col3:
        if strategy.position and strategy.position.total_lp_value_usd > 0:
            st.metric("LP Value", format_usd(strategy.position.total_lp_value_usd))
        elif strategy.position and strategy.position.health_factor:
            st.metric("Health Factor", f"{strategy.position.health_factor:.2f}")
        else:
            st.metric("Positions", "N/A")

    with col4:
        if strategy.pnl_history:
            pnl_7d = strategy.pnl_history[-1].pnl_usd
            st.metric("7d PnL", format_usd(abs(pnl_7d)), delta=f"{'+' if pnl_7d >= 0 else ''}{pnl_7d:,.2f}")
