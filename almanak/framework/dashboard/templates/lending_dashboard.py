"""
Lending Protocol Dashboard Template.

Reusable template for creating dashboards for lending strategies on protocols
like Aave V3, Morpho Blue, Compound V3, and Spark.

Scope (single-signal / single-position): the template renders **one**
collateral / borrow pair with **one** scalar health factor and LTV — the
shape of a single supply-borrow loop. Multi-collateral is partially
supported through ``collateral_assets`` (a dict feeding the breakdown
plot), but the headline metrics remain singular. The 3 accounting
sections (PnL, Cost Stack, Trade Tape) are baked in so every lending
dashboard ships with full accounting. For multi-signal layouts (e.g.
"supply on Aave + supply on Morpho" as separate motivations), do not
parameterize this template — write a custom dashboard composed from the
section helpers (``render_pnl_section``, ``render_cost_stack_section``,
``render_trade_tape_section``) plus primitive plot helpers from
``almanak.framework.dashboard.plots`` directly. See the dashboard
blueprints for the recommended composition.

Usage:
    from almanak.framework.dashboard.templates import LendingDashboardConfig, render_lending_dashboard

    config = LendingDashboardConfig(
        protocol="aave_v3",
        collateral_token="WETH",
        borrow_token="USDC",
    )

    def render_custom_dashboard(deployment_id, strategy_config, api_client, session_state):
        render_lending_dashboard(deployment_id, strategy_config, session_state, config)
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import streamlit as st

from almanak.framework.dashboard.plots import (
    plot_collateral_breakdown,
    plot_health_factor_gauge,
    plot_lending_rates_comparison,
    plot_ltv_ratio,
)
from almanak.framework.dashboard.sections import (
    render_cost_stack_section,
    render_pnl_section,
    render_trade_tape_section,
)


@dataclass
class LendingDashboardConfig:
    """Configuration for a lending dashboard.

    Attributes:
        protocol: Protocol name (e.g., "aave_v3", "morpho_blue", "compound_v3")
        collateral_token: Primary collateral token symbol
        borrow_token: Primary borrow token symbol
        chain: Chain name
        liquidation_threshold: Health factor threshold for liquidation (default 1.0)
        safe_threshold: Health factor threshold considered safe (default 1.5)
        max_ltv: Maximum LTV ratio (default 0.8)
        liquidation_ltv: LTV at which liquidation occurs (default 0.85)
        show_health_factor: Whether to show health factor gauge
        show_ltv: Whether to show LTV ratio visualization
        show_collateral_breakdown: Whether to show collateral breakdown
        show_rate_comparison: Whether to show rate comparison (for multi-protocol)
    """

    protocol: str = "aave_v3"
    collateral_token: str = "WETH"
    borrow_token: str = "USDC"
    chain: str = "arbitrum"
    liquidation_threshold: float = 1.0
    safe_threshold: float = 1.5
    max_ltv: float = 0.8
    liquidation_ltv: float = 0.85
    show_health_factor: bool = True
    show_ltv: bool = True
    show_collateral_breakdown: bool = True
    show_rate_comparison: bool = False


def _as_decimal(value: Any, default: Decimal = Decimal("0")) -> Decimal:
    if value is None or value == "":
        return default
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001
        return default


def _positive_decimal(value: Any) -> bool:
    return _as_decimal(value) > 0


def _decimal_or_none(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None


def _price(api_client: Any, token: str, chain: str) -> Decimal:
    if token.upper() in {"USDC", "USDT", "DAI"}:
        return Decimal("1")
    try:
        price = api_client.get_price(token, chain=chain)
    except Exception:  # noqa: BLE001
        price = None
    return _as_decimal(price, Decimal("0"))


def _apply_risk_metrics(
    hydrated: dict[str, Any],
    *,
    collateral_value: Decimal,
    borrowed_value: Decimal,
    collateral_token: str,
    borrow_token: str,
    config: LendingDashboardConfig,
) -> None:
    if collateral_value > 0:
        hydrated["ltv"] = str(borrowed_value / collateral_value)
        available_to_borrow = (collateral_value * Decimal(str(config.max_ltv))) - borrowed_value
        hydrated["available_to_borrow_usd"] = str(max(Decimal("0"), available_to_borrow))
        net_value = collateral_value - borrowed_value
        if net_value > 0:
            hydrated["leverage"] = str(collateral_value / net_value)

    # Only synthesize ``health_factor`` from config when the strategy did not
    # already provide one. A lending strategy that queries the on-chain
    # ``getUserAccountData().healthFactor`` puts the authoritative value here
    # (often via ``api_client.get_state()`` above). Overwriting that with a
    # static-config approximation (``liquidation_ltv`` from
    # ``LendingDashboardConfig`` is a dashboard default, not the on-chain
    # per-asset ``liquidationThreshold``) would silently mask the real
    # liquidation distance — for a money-critical metric, that is the exact
    # failure mode this dashboard exists to prevent.
    if borrowed_value > 0 and not _positive_decimal(hydrated.get("health_factor")):
        hydrated["health_factor"] = str((collateral_value * Decimal(str(config.liquidation_ltv))) / borrowed_value)
    elif borrowed_value <= 0:
        # No debt -> no liquidation risk. Drop any stale ``health_factor``
        # left over from a previous iteration so the template default
        # ("safe") renders instead of a misleading liquidation reading.
        hydrated.pop("health_factor", None)

    if collateral_value > 0 and not hydrated.get("collateral_assets"):
        hydrated["collateral_assets"] = {collateral_token: float(collateral_value)}
    if borrowed_value > 0 and not hydrated.get("borrow_assets"):
        hydrated["borrow_assets"] = {borrow_token: float(borrowed_value)}


def prepare_lending_session_state(
    api_client: Any,
    *,
    session_state: dict[str, Any],
    config: LendingDashboardConfig,
    strategy_config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Hydrate generic lending dashboard fields from strategy state.

    Hosted/custom dashboards receive raw strategy persistence, which often
    stores domain names such as ``supplied_token_amount`` and
    ``borrowed_token_amount``. The lending template renders generic fields
    (``collateral_amount``, ``borrowed_amount``, ``ltv``, ``health_factor``).
    This adapter keeps strategy dashboards thin while making the SDK template
    useful for Aave-style supply/borrow loops.
    """
    strategy_config = strategy_config or {}
    hydrated = dict(session_state or {})

    try:
        raw_state = api_client.get_state()
    except Exception:  # noqa: BLE001
        raw_state = {}
    if isinstance(raw_state, dict):
        for key, value in raw_state.items():
            if key not in hydrated or hydrated[key] in (None, ""):
                hydrated[key] = value

    collateral_token = str(strategy_config.get("collateral_token") or config.collateral_token)
    borrow_token = str(strategy_config.get("borrow_token") or config.borrow_token)
    chain = str(strategy_config.get("chain") or config.chain)

    supplied_amount = _as_decimal(
        hydrated.get("collateral_amount", hydrated.get("supplied_token_amount", hydrated.get("_supplied_token_amount")))
    )
    borrowed_amount = _as_decimal(
        hydrated.get("borrowed_amount", hydrated.get("borrowed_token_amount", hydrated.get("_borrowed_token_amount")))
    )

    collateral_price = _price(api_client, collateral_token, chain)
    borrow_price = _price(api_client, borrow_token, chain)
    collateral_value = _as_decimal(hydrated.get("collateral_value_usd"))
    borrowed_value = _as_decimal(hydrated.get("borrowed_value_usd"))
    if collateral_value <= 0 and supplied_amount > 0 and collateral_price > 0:
        collateral_value = supplied_amount * collateral_price
    if borrowed_value <= 0 and borrowed_amount > 0 and borrow_price > 0:
        borrowed_value = borrowed_amount * borrow_price

    if supplied_amount > 0 and not _positive_decimal(hydrated.get("collateral_amount")):
        hydrated["collateral_amount"] = str(supplied_amount)
    if borrowed_amount > 0 and not _positive_decimal(hydrated.get("borrowed_amount")):
        hydrated["borrowed_amount"] = str(borrowed_amount)
    if collateral_value > 0 and not _positive_decimal(hydrated.get("collateral_value_usd")):
        hydrated["collateral_value_usd"] = str(collateral_value)
    if borrowed_value > 0 and not _positive_decimal(hydrated.get("borrowed_value_usd")):
        hydrated["borrowed_value_usd"] = str(borrowed_value)

    _apply_risk_metrics(
        hydrated,
        collateral_value=collateral_value,
        borrowed_value=borrowed_value,
        collateral_token=collateral_token,
        borrow_token=borrow_token,
        config=config,
    )

    return hydrated


def render_lending_dashboard(
    deployment_id: str,
    strategy_config: dict[str, Any],
    session_state: dict[str, Any],
    config: LendingDashboardConfig,
) -> None:
    """Render a lending strategy dashboard using the provided configuration.

    Single-signal / single-position template — one collateral/borrow pair
    with scalar health factor and LTV. Bakes in the 3 accounting sections
    (PnL → primitive content → Cost Stack → Trade Tape). For multi-
    position or multi-signal layouts, compose a custom dashboard from
    the section helpers directly rather than parameterizing this template.

    Args:
        deployment_id: The deployment identifier
        strategy_config: Strategy configuration dictionary
        session_state: Current session state with position data
        config: LendingDashboardConfig for this dashboard
    """
    # Extract config overrides
    collateral_token = strategy_config.get("collateral_token", config.collateral_token)
    borrow_token = strategy_config.get("borrow_token", config.borrow_token)
    chain = strategy_config.get("chain", config.chain)
    protocol = strategy_config.get("protocol", config.protocol)

    st.title(f"{protocol.replace('_', ' ').title()} Lending Dashboard")

    st.markdown(f"**Deployment ID:** `{deployment_id}`")
    st.markdown(f"**Collateral:** {collateral_token} | **Borrow:** {borrow_token}")
    st.markdown(f"**Chain:** {chain.title()}")

    # Eyeball — am I making or losing money?
    render_pnl_section(deployment_id)

    # Health Factor and LTV Section
    col1, col2 = st.columns(2)

    with col1:
        if config.show_health_factor:
            health_factor = float(session_state.get("health_factor", 2.0))
            fig = plot_health_factor_gauge(
                health_factor=health_factor,
                liquidation_threshold=config.liquidation_threshold,
                safe_threshold=config.safe_threshold,
            )
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if config.show_ltv:
            current_ltv = float(session_state.get("ltv", 0.5))
            fig = plot_ltv_ratio(
                current_ltv=current_ltv,
                max_ltv=config.max_ltv,
                liquidation_ltv=config.liquidation_ltv,
            )
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Position Details
    st.subheader("Position Details")
    _render_position_details(session_state, collateral_token, borrow_token)

    st.divider()

    # Collateral / Borrow Breakdown
    if config.show_collateral_breakdown:
        st.subheader("Collateral / Borrow Breakdown")
        _render_asset_breakdown(session_state, collateral_token, borrow_token)

    st.divider()

    # Rate Comparison (optional)
    if config.show_rate_comparison and session_state.get("rate_comparison"):
        st.subheader("Rate Comparison")
        rate_data = session_state["rate_comparison"]
        fig = plot_lending_rates_comparison(
            protocols=rate_data.get("protocols", []),
            supply_rates=rate_data.get("supply_rates", []),
            borrow_rates=rate_data.get("borrow_rates", []),
            asset_symbol=borrow_token,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Performance Summary
    st.subheader("Performance Summary")
    _render_performance_summary(session_state)

    # Audit — life-to-date costs + per-intent trade tape
    render_cost_stack_section(deployment_id)
    render_trade_tape_section(deployment_id)


def _render_position_details(
    session_state: dict[str, Any],
    collateral_token: str,
    borrow_token: str,
) -> None:
    """Render the position details section."""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        collateral_amount = Decimal(str(session_state.get("collateral_amount", "0")))
        st.metric(f"Collateral ({collateral_token})", f"{float(collateral_amount):.4f}")

    with col2:
        collateral_value = Decimal(str(session_state.get("collateral_value_usd", "0")))
        st.metric("Collateral Value", f"${float(collateral_value):,.2f}")

    with col3:
        borrowed_amount = Decimal(str(session_state.get("borrowed_amount", "0")))
        st.metric(f"Borrowed ({borrow_token})", f"{float(borrowed_amount):,.2f}")

    with col4:
        borrowed_value = Decimal(str(session_state.get("borrowed_value_usd", "0")))
        st.metric("Borrowed Value", f"${float(borrowed_value):,.2f}")

    # Second row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        supply_apy = _decimal_or_none(session_state.get("supply_apy"))
        st.metric("Supply APY", "N/A" if supply_apy is None else f"{float(supply_apy) * 100:.2f}%")

    with col2:
        borrow_apy = _decimal_or_none(session_state.get("borrow_apy"))
        st.metric("Borrow APY", "N/A" if borrow_apy is None else f"{float(borrow_apy) * 100:.2f}%")

    with col3:
        net_apy = _decimal_or_none(session_state.get("net_apy"))
        st.metric("Net APY", "N/A" if net_apy is None else f"{float(net_apy) * 100:+.2f}%")

    with col4:
        leverage = Decimal(str(session_state.get("leverage", "1")))
        st.metric("Leverage", f"{float(leverage):.2f}x")


def _render_asset_breakdown(
    session_state: dict[str, Any],
    collateral_token: str,
    borrow_token: str,
) -> None:
    collateral_assets = session_state.get("collateral_assets", {})
    if not collateral_assets:
        collateral_value = float(_as_decimal(session_state.get("collateral_value_usd")))
        if collateral_value > 0:
            collateral_assets = {collateral_token: collateral_value}

    borrow_assets = session_state.get("borrow_assets", {})
    if not borrow_assets:
        borrowed_value = float(_as_decimal(session_state.get("borrowed_value_usd")))
        if borrowed_value > 0:
            borrow_assets = {borrow_token: borrowed_value}

    if not collateral_assets and not borrow_assets:
        st.info("No collateral or borrow data available")
        return

    col1, col2 = st.columns(2)
    with col1:
        if collateral_assets:
            fig = plot_collateral_breakdown(
                assets=collateral_assets,
                title="Collateral",
                show_values=True,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No collateral data available")

    with col2:
        if borrow_assets:
            fig = plot_collateral_breakdown(
                assets=borrow_assets,
                title="Borrow",
                show_values=True,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No borrow data available")


def _render_performance_summary(session_state: dict[str, Any]) -> None:
    """Render the performance summary section."""
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        interest_earned = _decimal_or_none(session_state.get("interest_earned_usd"))
        st.metric("Interest Earned", "N/A" if interest_earned is None else f"${float(interest_earned):,.2f}")

    with col2:
        interest_paid = _decimal_or_none(session_state.get("interest_paid_usd"))
        st.metric("Interest Paid", "N/A" if interest_paid is None else f"${float(interest_paid):,.2f}")

    with col3:
        net_interest = _decimal_or_none(session_state.get("net_interest_usd"))
        st.metric("Net Interest", "N/A" if net_interest is None else f"${float(net_interest):+,.2f}")

    with col4:
        total_pnl = _decimal_or_none(session_state.get("total_pnl_usd"))
        st.metric("Total PnL", "N/A" if total_pnl is None else f"${float(total_pnl):+,.2f}")


# Pre-configured templates for common lending protocols


def get_aave_v3_config(
    collateral_token: str = "WETH",
    borrow_token: str = "USDC",
    chain: str = "arbitrum",
) -> LendingDashboardConfig:
    """Get pre-configured Aave V3 lending dashboard config."""
    return LendingDashboardConfig(
        protocol="aave_v3",
        collateral_token=collateral_token,
        borrow_token=borrow_token,
        chain=chain,
        max_ltv=0.80,
        liquidation_ltv=0.825,
    )


def get_morpho_blue_config(
    collateral_token: str = "wstETH",
    borrow_token: str = "USDC",
    chain: str = "ethereum",
) -> LendingDashboardConfig:
    """Get pre-configured Morpho Blue lending dashboard config."""
    return LendingDashboardConfig(
        protocol="morpho_blue",
        collateral_token=collateral_token,
        borrow_token=borrow_token,
        chain=chain,
        max_ltv=0.77,
        liquidation_ltv=0.80,
    )


def get_compound_v3_config(
    collateral_token: str = "WETH",
    borrow_token: str = "USDC",
    chain: str = "ethereum",
) -> LendingDashboardConfig:
    """Get pre-configured Compound V3 lending dashboard config."""
    return LendingDashboardConfig(
        protocol="compound_v3",
        collateral_token=collateral_token,
        borrow_token=borrow_token,
        chain=chain,
        max_ltv=0.83,
        liquidation_ltv=0.90,
    )


def get_spark_config(
    collateral_token: str = "WETH",
    borrow_token: str = "DAI",
    chain: str = "ethereum",
) -> LendingDashboardConfig:
    """Get pre-configured Spark lending dashboard config."""
    return LendingDashboardConfig(
        protocol="spark",
        collateral_token=collateral_token,
        borrow_token=borrow_token,
        chain=chain,
        max_ltv=0.80,
        liquidation_ltv=0.825,
    )
