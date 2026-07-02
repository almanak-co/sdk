"""hyperliquid_trailing_perp custom dashboard.

Renders the framework's perp dashboard template (title, header, PnL → Cost
Stack → Trade Tape audit sections, plus position / leverage / liquidation
panels) for the HyperEVM CoreWriter perp, then appends a small card describing
this strategy's distinctive exit logic — the ratcheting trailing stop — since
that state is strategy-specific and not part of the generic perp template.

The renderer owns the title + the audit sections; do NOT wrap it with an extra
``st.title``. Strategy-specific cards are rendered AFTER it so they sit below
the audit panels.
"""

from __future__ import annotations

from typing import Any

import streamlit as st

from almanak.framework.dashboard.templates import get_hyperliquid_config, render_perp_dashboard


def render_custom_dashboard(
    deployment_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    # render_perp_dashboard reads market / collateral_token / chain / protocol
    # from strategy_config (overriding the config defaults), so the HyperEVM
    # values from config.json flow through; the config supplies the leverage /
    # liquidation panel bounds. chain="hyperevm" (the venue is "hyperliquid").
    config = get_hyperliquid_config(
        market=str(strategy_config.get("market", "ETH/USD")),
        collateral_token=str(strategy_config.get("collateral_token", "USDC")),
        chain="hyperevm",
    )
    render_perp_dashboard(deployment_id, strategy_config, session_state, config)

    # --- Strategy-specific: ratcheting trailing-stop exit config ---------------
    st.divider()
    st.subheader("Trailing-stop exit")
    st.caption(
        "Exit is evaluated strategy-side each tick (Hyperliquid has no native "
        "trigger orders) and fired as a market reduce-only close: take-profit "
        "caps the win, a hard stop is the liquidation buffer, and once the trade "
        "is up by the activation threshold a trailing stop ratchets behind the "
        "high-water PnL."
    )

    def _pct(key: str, default: str) -> str:
        try:
            return f"{float(str(strategy_config.get(key, default))) * 100:.2f}%"
        except (TypeError, ValueError):
            return str(strategy_config.get(key, default))

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Take-profit", _pct("take_profit_pct", "0.02"))
    c2.metric("Hard stop", _pct("stop_loss_pct", "0.03"))
    c3.metric("Trail activate at", _pct("trail_activation_pct", "0.01"))
    c4.metric("Trail giveback", _pct("trail_pct", "0.015"))
    st.caption(
        f"Direction: **{'LONG' if strategy_config.get('is_long', True) else 'SHORT'}** · "
        f"Size: **${strategy_config.get('size_usd', '15')}** · "
        f"Leverage: **{strategy_config.get('leverage', '2.0')}x** · "
        f"Re-enter after close: **{bool(strategy_config.get('reenter_after_close', True))}**"
    )
