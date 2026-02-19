"""
Leverage Loop Cross-Chain Strategy Dashboard.

Custom dashboard showing multi-chain positions, health factor,
leverage metrics, and performance across Base and Arbitrum.
"""

from decimal import Decimal
from typing import Any

import streamlit as st


def render_custom_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    """Render the Leverage Loop Cross-Chain custom dashboard."""
    st.title("Leverage Loop Cross-Chain Dashboard")

    # Extract config
    swap_amount = strategy_config.get("swap_amount_usd", 10)
    borrow_amount = strategy_config.get("borrow_amount_usd", 5)
    perp_size = strategy_config.get("perp_size_usd", 10)
    max_leverage = strategy_config.get("max_leverage", 2.0)
    min_health_factor = strategy_config.get("min_health_factor", 1.5)

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown("**Chains:** Base -> Arbitrum")
    st.markdown("**Protocols:** Enso (bridge) | Aave V3 (lending) | GMX V2 (perps)")

    st.divider()

    # Multi-chain balances
    st.subheader("Multi-Chain Balances")
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Base:**")
        base_usdc = Decimal(str(session_state.get("base_usdc", "0")))
        st.metric("USDC", f"${float(base_usdc):,.2f}")

    with col2:
        st.markdown("**Arbitrum:**")
        arb_weth = Decimal(str(session_state.get("arb_weth", "0")))
        arb_usdc = Decimal(str(session_state.get("arb_usdc", "0")))
        col2a, col2b = st.columns(2)
        with col2a:
            st.metric("WETH", f"{float(arb_weth):.4f}")
        with col2b:
            st.metric("USDC", f"${float(arb_usdc):,.2f}")

    st.divider()

    # Aave Position (Arbitrum)
    st.subheader("Aave V3 Position (Arbitrum)")
    health_factor = Decimal(str(session_state.get("health_factor", "0")))
    supplied = Decimal(str(session_state.get("aave_supplied", "0")))
    borrowed = Decimal(str(session_state.get("aave_borrowed", "0")))

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("WETH Supplied", f"{float(supplied):.4f}")
    with col2:
        st.metric("USDC Borrowed", f"${float(borrowed):,.2f}")
    with col3:
        if health_factor >= min_health_factor:
            st.metric("Health Factor", f"{float(health_factor):.2f}", delta="SAFE")
        elif health_factor > 1.0:
            st.metric("Health Factor", f"{float(health_factor):.2f}", delta="WARNING", delta_color="inverse")
        else:
            st.metric("Health Factor", f"{float(health_factor):.2f}", delta="DANGER", delta_color="inverse")

    # Health factor visualization
    if health_factor > 0:
        normalized_hf = min(float(health_factor) / 3.0, 1.0)  # Normalize to 0-1 (3.0 = very safe)
        st.progress(normalized_hf, text=f"Health Factor: {float(health_factor):.2f}")

    st.divider()

    # GMX Position (Arbitrum)
    st.subheader("GMX V2 Position (Arbitrum)")
    perp_size_current = Decimal(str(session_state.get("perp_size", "0")))
    perp_pnl = Decimal(str(session_state.get("perp_pnl", "0")))
    leverage = Decimal(str(session_state.get("leverage", "1")))
    is_long = session_state.get("is_long", True)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        direction = "LONG" if is_long else "SHORT"
        st.metric("Direction", f"ETH {direction}")
    with col2:
        st.metric("Size", f"${float(perp_size_current):,.2f}")
    with col3:
        st.metric("Leverage", f"{float(leverage):.1f}x")
    with col4:
        delta_color = "normal" if perp_pnl >= 0 else "inverse"
        st.metric("PnL", f"${float(perp_pnl):+,.2f}", delta_color=delta_color)

    if perp_size_current > 0:
        if is_long:
            st.info(f"ETH Long: ${float(perp_size_current):,.2f} notional at {float(leverage):.1f}x leverage")
        else:
            st.info(f"ETH Short: ${float(perp_size_current):,.2f} notional at {float(leverage):.1f}x leverage")

    st.divider()

    # Loop flow visualization
    st.subheader("Leverage Loop Flow")
    st.markdown("""
    1. **Cross-Chain Swap** (Base -> Arbitrum via Enso)
       - USDC on Base -> WETH on Arbitrum
    2. **Supply to Aave** (Arbitrum)
       - WETH as collateral
    3. **Borrow from Aave** (Arbitrum)
       - USDC against WETH
    4. **Open Perp on GMX** (Arbitrum)
       - ETH Long with borrowed USDC
    """)

    st.divider()

    # Configuration
    st.subheader("Configuration")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Swap Amount", f"${float(swap_amount):,.2f}")
        st.metric("Min Health", str(min_health_factor))
    with col2:
        st.metric("Borrow Amount", f"${float(borrow_amount):,.2f}")
        st.metric("Max Leverage", f"{float(max_leverage)}x")
    with col3:
        st.metric("Perp Size", f"${float(perp_size):,.2f}")
        interest_mode = strategy_config.get("interest_rate_mode", "variable")
        st.metric("Interest Mode", interest_mode)

    st.divider()

    # Performance
    st.subheader("Performance")
    _render_performance(session_state)


def _render_performance(session_state: dict[str, Any]) -> None:
    total_pnl = Decimal(str(session_state.get("total_pnl", "0")))
    total_loops = session_state.get("total_loops", 0)
    funding_paid = Decimal(str(session_state.get("funding_paid", "0")))
    interest_paid = Decimal(str(session_state.get("interest_paid", "0")))

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Loops", str(total_loops))
    with col2:
        st.metric("Total PnL", f"${float(total_pnl):+,.2f}")
    with col3:
        st.metric("Funding Paid", f"${float(funding_paid):,.2f}")
    with col4:
        st.metric("Interest Paid", f"${float(interest_paid):,.2f}")
