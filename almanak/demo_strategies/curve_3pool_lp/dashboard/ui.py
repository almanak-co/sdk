"""
Curve 3pool LP Strategy Dashboard.

Custom dashboard showing the 3-coin stableswap position (DAI/USDC/USDT),
the fungible 3Crv LP token, and standard PnL / cost-stack / trade-tape
sections shared by all demo dashboards.
"""

from decimal import Decimal
from typing import Any

import streamlit as st

from almanak.framework.dashboard import (
    render_cost_stack_section,
    render_nav_history_section,
    render_pnl_section,
    render_trade_tape_section,
)


def render_custom_dashboard(
    deployment_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    """Render the Curve 3pool LP custom dashboard.

    Shows:
    - The 3-coin deposit (DAI / USDC / USDT)
    - Position status (fungible 3Crv LP token)
    - Standard PnL, cost stack, and trade tape sections
    """
    st.title("Curve 3pool LP Strategy Dashboard")
    render_pnl_section(deployment_id)
    # NAV / PnL / drawdown over time — the lifetime portfolio-value chart
    # (reads portfolio_snapshots via the gateway; no fabricated data).
    render_nav_history_section(deployment_id, default_range="All")

    pool = strategy_config.get("pool", "3pool")

    # Strategy info header
    st.markdown(f"**Deployment ID:** `{deployment_id}`")
    st.markdown(f"**Pool:** {pool} (DAI / USDC / USDT)")
    st.markdown("**Protocol:** Curve Finance (StableSwap)")
    st.markdown("**Chain:** Ethereum")
    st.markdown("**LP Token:** 3Crv (fungible ERC20)")

    st.divider()

    # 3-coin position section
    st.subheader("3-coin Stableswap Position")
    _render_position(strategy_config, session_state)

    st.divider()

    st.subheader("Why 3pool is different")
    st.markdown(
        """
        - **3 coins, not 2.** 3pool holds DAI, USDC, and USDT. A real deposit
          funds all three legs via the per-coin allocation vector
          `coin_amounts`, not just the 2-slot `amount0`/`amount1` path.
        - **Fungible LP token.** Curve mints a single 3Crv ERC20 LP token
          (no NFT), so closing burns the LP token and returns the three coins
          proportionally.
        - **StableSwap curve.** Near the 1:1:1 peg the pool behaves almost like
          constant-sum, giving very low slippage between the stablecoins.
        """
    )

    render_cost_stack_section(deployment_id)
    render_trade_tape_section(deployment_id)


def _render_position(strategy_config: dict[str, Any], session_state: dict[str, Any]) -> None:
    """Render the 3-coin position details."""
    has_position = session_state.get("has_position", False)
    amount_dai = Decimal(str(strategy_config.get("amount_dai", "0")))
    amount_usdc = Decimal(str(strategy_config.get("amount_usdc", "0")))
    amount_usdt = Decimal(str(strategy_config.get("amount_usdt", "0")))

    status_col, value_col = st.columns(2)
    with status_col:
        st.metric(
            "Position Status",
            "Active" if has_position else "Inactive",
            help="Whether there is an active 3pool LP position",
        )
    # Only show deposited amounts when a position is actually open — otherwise
    # the configured target amounts would misrepresent an empty position.
    dep_dai = amount_dai if has_position else Decimal("0")
    dep_usdc = amount_usdc if has_position else Decimal("0")
    dep_usdt = amount_usdt if has_position else Decimal("0")

    with value_col:
        # All three coins are USD stablecoins (~$1), so the deposited notional
        # is a good estimate of position value.
        total_value = dep_dai + dep_usdc + dep_usdt
        st.metric(
            "Deposited Notional",
            f"${float(total_value):,.2f}",
            help="Sum of the three stablecoin deposit amounts (~$1 each)",
        )

    dai_col, usdc_col, usdt_col = st.columns(3)
    with dai_col:
        st.metric("DAI (idx 0)", f"{float(dep_dai):.2f}", help="Coin index 0")
    with usdc_col:
        st.metric("USDC (idx 1)", f"{float(dep_usdc):.2f}", help="Coin index 1")
    with usdt_col:
        st.metric("USDT (idx 2)", f"{float(dep_usdt):.2f}", help="Coin index 2")
