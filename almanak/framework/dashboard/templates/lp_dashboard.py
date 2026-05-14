"""
Liquidity Provider (LP) Dashboard Template.

Reusable template for creating dashboards for LP strategies on concentrated
liquidity protocols like Uniswap V3, PancakeSwap V3, TraderJoe V2, and Aerodrome.

Scope (single-signal / single-position): the template renders **one**
active LP position on **one** ``token0``/``token1`` pair. ``LPSessionState``
is intentionally scalar (``position_id``, ``range_lower``, ``range_upper``);
strategies that hold multiple LP NFTs simultaneously are not modelled here
even though the gateway data model (``PositionSummary.lp_positions``) is
multi-aware. The 3 accounting sections (PnL, Cost Stack, Trade Tape) are
baked in so every LP dashboard ships with full accounting. For multi-
position or multi-signal layouts, compose a custom dashboard from the
section helpers (``render_pnl_section``, ``render_cost_stack_section``,
``render_trade_tape_section``) plus primitive plot helpers from
``almanak.framework.dashboard.plots`` directly. See the dashboard
blueprints for the recommended composition.

Usage:
    from almanak.framework.dashboard.templates import (
        LPDashboardConfig,
        render_lp_dashboard,
        prepare_lp_session_state,
        get_uniswap_v3_config,
    )

    config = get_uniswap_v3_config(token0="WETH", token1="USDC")

    def render_custom_dashboard(strategy_id, strategy_config, api_client, session_state):
        session_state = prepare_lp_session_state(api_client, config=config)
        render_lp_dashboard(strategy_id, strategy_config, session_state, config)
"""

import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, TypedDict

import streamlit as st

logger = logging.getLogger(__name__)

from almanak.framework.dashboard.plots import (
    plot_fee_accumulation,
    plot_impermanent_loss,
    plot_liquidity_distribution,
    plot_position_range_status,
    plot_positions_over_time,
)
from almanak.framework.dashboard.sections import (
    render_cost_stack_section,
    render_pnl_section,
    render_trade_tape_section,
)


@dataclass
class LPDashboardConfig:
    """Configuration for an LP dashboard.

    Attributes:
        protocol: Protocol name (e.g., "uniswap_v3", "aerodrome", "traderjoe_v2")
        token0: First token symbol
        token1: Second token symbol
        fee_tier: Fee tier display string (e.g., "0.30%")
        chain: Chain name
        show_liquidity_distribution: Whether to show liquidity distribution chart
        show_position_history: Whether to show position history chart
        show_impermanent_loss: Whether to show IL tracking
        show_fee_accumulation: Whether to show fee accumulation chart
        invert_prices: Whether to invert price display
        position_bounds_ratio: Ratio for position bounds lines (None to disable)
    """

    protocol: str = "uniswap_v3"
    token0: str = "WETH"
    token1: str = "USDC"
    fee_tier: str = "0.30%"
    chain: str = "arbitrum"
    show_liquidity_distribution: bool = True
    show_position_history: bool = True
    show_impermanent_loss: bool = True
    show_fee_accumulation: bool = True
    invert_prices: bool = False
    position_bounds_ratio: float | None = 0.8


class LPSessionState(TypedDict, total=False):
    """Keys expected by ``render_lp_dashboard()`` in ``session_state``.

    Use ``prepare_lp_session_state(api_client, config=config)`` to populate
    this automatically from the gateway.

    Keys from strategy state (read directly, no mapping):
        position_id: Active LP position identifier.
        range_lower: Lower price bound of the LP position.
        range_upper: Upper price bound of the LP position.
        total_value_usd: Total position value in USD.

    Keys derived/loaded by prepare_lp_session_state():
        is_active: Whether a position is currently active.
        current_price: Current market price of token0 in USD.
        in_range: Whether current_price is within [range_lower, range_upper].
        token0_amount: Amount of token0 in the position.
        token1_amount: Amount of token1 in the position.

    Optional keys (strategy may or may not provide):
        total_fees_usd, impermanent_loss_pct, net_pnl_usd: Performance metrics.
        tick_data, lower_tick, upper_tick, current_tick: Liquidity distribution.
        position_history, price_history, fee_history, il_history: Chart data.
    """

    # From strategy state
    position_id: str | None
    range_lower: float | str | None
    range_upper: float | str | None
    total_value_usd: str

    # Derived/loaded
    is_active: bool
    current_price: float | None
    in_range: bool | None
    token0_amount: float
    token1_amount: float

    # Performance (optional)
    total_fees_usd: str
    impermanent_loss_pct: str
    net_pnl_usd: str

    # Chart data (optional)
    tick_data: Any
    lower_tick: int | None
    upper_tick: int | None
    current_tick: int
    position_history: Any
    price_history: Any
    fee_history: Any
    il_history: Any


LP_CRITICAL_KEYS: list[str] = [
    "position_id",
    "range_lower",
    "range_upper",
    "total_value_usd",
    "is_active",
    "current_price",
    "in_range",
    "token0_amount",
    "token1_amount",
]
"""Keys that ``prepare_lp_session_state`` must produce and the template reads."""


def prepare_lp_session_state(
    api_client: Any,
    session_state: dict[str, Any] | None = None,
    config: LPDashboardConfig | None = None,
) -> dict[str, Any]:
    """Load strategy data from the gateway and enrich for the LP dashboard.

    Fetches strategy state via ``api_client.get_state()``, adds derived fields
    (``is_active``, ``in_range``), and loads live market data (``current_price``,
    token amounts).  State keys pass through directly -- no mapping layer.

    Args:
        api_client: DashboardAPIClient instance.
        session_state: Optional pre-existing state to enrich.  If *None*,
            state is loaded fresh from ``api_client.get_state()``.
            Values already present are never overwritten.
        config: LPDashboardConfig -- needed to know which token to price.
            If *None*, ``current_price`` will not be fetched.

    Returns:
        Enriched dict containing all :data:`LP_CRITICAL_KEYS`.
    """
    # Load state from gateway, guarding against API failures
    if session_state is None:
        try:
            result: dict[str, Any] = api_client.get_state() if api_client else {}
        except Exception:
            logger.warning("get_state() failed — falling back to empty state")
            result = {}
    else:
        result = dict(session_state)
        # Merge in API state for any keys not already present
        if api_client:
            try:
                for k, v in api_client.get_state().items():
                    result.setdefault(k, v)
            except Exception:
                logger.debug("get_state() merge failed — using caller-provided state only")

    # Derive is_active from position_id
    result.setdefault("is_active", result.get("position_id") is not None)

    # Fetch current price if config available
    if config is not None and "current_price" not in result:
        try:
            result["current_price"] = api_client.get_price(config.token0, "USD") if api_client else None
        except Exception:
            logger.debug("Failed to fetch current price for %s", config.token0)
            result["current_price"] = None
    else:
        result.setdefault("current_price", None)

    # Derive in_range
    if "in_range" not in result:
        current = result.get("current_price")
        lower = result.get("range_lower")
        upper = result.get("range_upper")
        if current is not None and lower is not None and upper is not None:
            try:
                result["in_range"] = float(lower) <= float(current) <= float(upper)
            except (ValueError, TypeError):
                logger.debug("Non-numeric range bounds — cannot compute in_range")
                result["in_range"] = None
        else:
            result["in_range"] = None

    # Load token amounts from position snapshot
    if "token0_amount" not in result or "token1_amount" not in result:
        try:
            position = api_client.get_position() if api_client else {}
            balances = position.get("token_balances", [])
            # Match by symbol when config is available, fall back to index order
            t0_amount = 0.0
            t1_amount = 0.0
            if config and balances:
                bal_map = {b["symbol"].upper(): float(b.get("balance", 0)) for b in balances}
                t0_amount = bal_map.get(config.token0.upper(), 0.0)
                t1_amount = bal_map.get(config.token1.upper(), 0.0)
            elif balances:
                t0_amount = float(balances[0].get("balance", 0)) if len(balances) >= 1 else 0.0
                t1_amount = float(balances[1].get("balance", 0)) if len(balances) >= 2 else 0.0
            result.setdefault("token0_amount", t0_amount)
            result.setdefault("token1_amount", t1_amount)
        except Exception:
            logger.warning("Failed to load position data for LP dashboard")
            result.setdefault("token0_amount", 0)
            result.setdefault("token1_amount", 0)

    # Ensure all critical state keys have defaults
    result.setdefault("position_id", None)
    result.setdefault("range_lower", None)
    result.setdefault("range_upper", None)
    result.setdefault("total_value_usd", "0")

    return result


def render_lp_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    session_state: dict[str, Any],
    config: LPDashboardConfig,
) -> None:
    """Render an LP strategy dashboard using the provided configuration.

    Single-signal / single-position template — renders one active LP
    position on one configured pair. Bakes in the 3 accounting sections
    (PnL → primitive content → Cost Stack → Trade Tape). For multi-
    position or multi-signal layouts, compose a custom dashboard from
    the section helpers directly rather than parameterizing this template.

    Args:
        strategy_id: The strategy identifier
        strategy_config: Strategy configuration dictionary
        session_state: Current session state with position data.
            Use :func:`prepare_lp_session_state` to populate this from the
            gateway before calling this function.
        config: LPDashboardConfig for this dashboard
    """
    # Warn about missing critical keys so silent N/A failures are visible
    missing = [k for k in LP_CRITICAL_KEYS if k not in session_state]
    if missing:
        logger.warning(
            "LP dashboard missing critical session_state keys: %s. "
            "Call prepare_lp_session_state(api_client, config=config) to populate them.",
            missing,
        )

    # Extract config overrides
    token0 = strategy_config.get("token0", config.token0)
    token1 = strategy_config.get("token1", config.token1)
    chain = strategy_config.get("chain", config.chain)
    protocol = strategy_config.get("protocol", config.protocol)
    fee_tier = strategy_config.get("fee_tier", config.fee_tier)

    st.title(f"{protocol.replace('_', ' ').title()} LP Dashboard")

    st.markdown(f"**Strategy ID:** `{strategy_id}`")
    st.markdown(f"**Pool:** {token0}/{token1} ({fee_tier})")
    st.markdown(f"**Chain:** {chain.title()}")

    # Eyeball — am I making or losing money?
    render_pnl_section(strategy_id)

    # Position Status Section
    st.subheader("Position Status")
    _render_position_status_panel(session_state, config)

    st.divider()

    # Helper to check for non-empty data (handles DataFrames, lists, and other types)
    def _has_data(data: object) -> bool:
        if data is None:
            return False
        if hasattr(data, "empty"):  # pandas DataFrame/Series
            return not data.empty
        if hasattr(data, "__len__"):  # lists, tuples, etc.
            return len(data) > 0
        return True

    # Position Range Status
    current_price = session_state.get("current_price")
    range_lower = session_state.get("range_lower")
    range_upper = session_state.get("range_upper")
    try:
        if current_price is not None and range_lower is not None and range_upper is not None:
            fig = plot_position_range_status(
                current_price=float(current_price),
                lower_bound=float(range_lower),
                upper_bound=float(range_upper),
                token_pair=f"{token0}/{token1}",
                invert_prices=config.invert_prices,
            )
            st.plotly_chart(fig, use_container_width=True)
    except (ValueError, TypeError):
        logger.debug("Non-numeric range data — skipping position range chart")

    st.divider()

    # Liquidity Distribution
    tick_data = session_state.get("tick_data")
    lower_tick = session_state.get("lower_tick")
    upper_tick = session_state.get("upper_tick")
    position_bounds = (lower_tick, upper_tick) if lower_tick is not None and upper_tick is not None else None

    if config.show_liquidity_distribution and _has_data(tick_data):
        st.subheader("Liquidity Distribution")
        fig = plot_liquidity_distribution(
            tick_data=tick_data,
            current_tick=session_state.get("current_tick", 0),
            position_bounds=position_bounds,
            token_pair=f"{token0}/{token1}",
            fee_tier=fee_tier,
            invert_prices=config.invert_prices,
        )
        st.plotly_chart(fig, use_container_width=True)
    elif config.show_liquidity_distribution:
        st.info("Liquidity distribution data not available")

    st.divider()

    # Position History
    if config.show_position_history:
        st.subheader("Position History")
        position_history = session_state.get("position_history")
        price_history = session_state.get("price_history")
        if _has_data(position_history) and _has_data(price_history) and position_history is not None:
            fig = plot_positions_over_time(
                positions=position_history,
                price_data=price_history,
                invert_prices=config.invert_prices,
                price_bounds_ratio=config.position_bounds_ratio,
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Position history data not available")

    st.divider()

    # Metrics Section
    col1, col2 = st.columns(2)

    with col1:
        # Fee Accumulation
        fee_history = session_state.get("fee_history")
        if config.show_fee_accumulation and _has_data(fee_history):
            st.subheader("Fee Accumulation")
            fig = plot_fee_accumulation(
                fee_data=fee_history,
                show_cumulative=True,
                fee_unit="USD",
            )
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Impermanent Loss
        il_history = session_state.get("il_history")
        if config.show_impermanent_loss and _has_data(il_history):
            st.subheader("Impermanent Loss")
            fig = plot_impermanent_loss(
                il_data=il_history,
                show_cumulative=True,
            )
            st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Performance Summary
    st.subheader("Performance Summary")
    _render_performance_summary(session_state)

    # Audit — life-to-date costs + per-intent trade tape
    render_cost_stack_section(strategy_id)
    render_trade_tape_section(strategy_id)


def _render_position_status_panel(
    session_state: dict[str, Any],
    config: LPDashboardConfig,
) -> None:
    """Top-level dispatch: single position vs multi-position.

    Strategies running a single LP (the historical default) populate the
    scalar ``position_id`` / ``range_lower`` / ``range_upper`` /
    ``token0_amount`` / ``token1_amount`` keys on ``session_state``. We
    render that as one panel — backward compatible with every demo today.

    Multi-position strategies (e.g. ``lp_dual`` / ``lp_triple``) populate
    ``session_state["positions"]`` with a list of per-position dicts (each
    shaped like the scalar keys above, plus an optional ``label``). We
    render each in its own ``st.expander``, stacked, so an operator can
    see all legs at a glance and drill into any one without losing the
    others. Tabs were considered and rejected — they hide N-1 positions
    behind a click.
    """
    positions = session_state.get("positions")
    if isinstance(positions, list) and positions:
        # Defensive: a strategy author who populates ``positions`` with
        # anything that is not a dict (tuple, Pydantic model, string)
        # would 500 the dashboard via ``AttributeError`` on ``.get``.
        # Skip such entries so a single typo can't take the panel down.
        valid = [p for p in positions if isinstance(p, dict)]
        if not valid:
            _render_position_status(session_state, config)
            return
        if len(valid) == 1:
            _render_position_status(_merge_state(session_state, valid[0]), config)
            return
        for idx, pos in enumerate(valid, start=1):
            # Bug 6 — surface the strategy-stamped registry_handle (e.g.
            # ``leg_narrow`` / ``leg_wide``) directly. The dashboard used
            # to read ``label``, which the lp_dual / lp_triple strategies
            # don't populate — the operator saw "Position 1" / "Position 2"
            # instead of the strategy's own per-leg names. Fall back to
            # ``label`` (older strategies) and then to a numeric stub.
            leg_name = pos.get("registry_handle") or pos.get("label") or f"Position {idx}"
            pid = pos.get("position_id")
            header = f"{leg_name} — id {pid}" if pid else leg_name
            with st.expander(header, expanded=idx == 1):
                _render_position_status(_merge_state(session_state, pos), config)
        return

    _render_position_status(session_state, config)


def _merge_state(base: dict[str, Any], pos: dict[str, Any]) -> dict[str, Any]:
    """Overlay per-position fields on the strategy-wide session state."""
    merged = dict(base)
    merged.update(pos)
    return merged


def _render_position_status(
    session_state: dict[str, Any],
    config: LPDashboardConfig,
) -> None:
    """Render the position status section.

    Layout: 2 columns × 4 rows. The 4-col layout truncated long values
    (``Out of Range``, ``$80,950.0123``); 2 columns gives each metric
    enough horizontal room to render at full ``st.metric`` size without
    ellipsizing.
    """
    # Row 1
    col_a, col_b = st.columns(2)
    with col_a:
        position_id = session_state.get("position_id", "N/A")
        pid_str = str(position_id)
        st.metric("Position ID", pid_str if len(pid_str) <= 24 else pid_str[:24] + "...")
    with col_b:
        is_active = session_state.get("is_active", False)
        st.metric("Status", "Active" if is_active else "Inactive")

    # Row 2 — Range Status full-width (Current Price removed pending
    # pool-relative-unit fix; today's USD value didn't compose with the
    # tick-derived range bounds, so we drop it rather than display
    # something misleading).
    in_range = session_state.get("in_range", None)
    if in_range is None:
        range_status = "Unknown"
    else:
        range_status = "In Range" if in_range else "Out of Range"
    st.metric("Range Status", range_status)

    # Row 3
    col_a, col_b = st.columns(2)
    with col_a:
        st.metric("Lower Bound", _fmt_pool_price(session_state.get("range_lower"), config))
    with col_b:
        st.metric("Upper Bound", _fmt_pool_price(session_state.get("range_upper"), config))

    # Row 4
    col_a, col_b = st.columns(2)
    with col_a:
        st.metric(config.token0, _fmt_token_amount(session_state.get("token0_amount", 0)))
    with col_b:
        st.metric(config.token1, _fmt_token_amount(session_state.get("token1_amount", 0)))


def _fmt_token_amount(value: Any) -> str:
    """Format a token amount with adaptive precision.

    Fixed ``:.4f`` / ``:,.2f`` rounds sub-1 amounts to ``0.0000`` /
    ``0.00`` — a 0.0001346 WBTC position disappears entirely. Use 4
    significant figures for sub-1 values and 2dp thousands-separated for
    ≥1, matching the trade-tape headline convention.
    """
    try:
        d = Decimal(str(value)) if value is not None else Decimal("0")
    except (ArithmeticError, ValueError, TypeError):
        return str(value)
    if not d.is_finite():
        return str(value)
    abs_d = abs(d)
    if abs_d == 0:
        return "0"
    if abs_d >= Decimal("1"):
        return f"{d:,.2f}"
    return f"{d:.4g}"


def _fmt_pool_price(value: Any, config: LPDashboardConfig) -> str:
    """Format an LP price value with adaptive precision.

    Always preserves the strategy-provided unit — the template is
    agnostic to whether ``current_price``/``range_lower``/``range_upper``
    are pool-relative (``token1 per token0``, the natural Uniswap V3
    tick output) or USD-denominated. The display picks ``:.4g`` for
    sub-1 values so a ratio like ``0.000868 BTC/ETH`` doesn't collapse
    to ``0.0009``.
    """
    if value is None or value == "":
        return "N/A"
    try:
        d = Decimal(str(value))
    except (ArithmeticError, ValueError, TypeError):
        return "N/A"
    if not d.is_finite():
        return "N/A"
    if abs(d) >= Decimal("1"):
        return f"{d:,.4f}"
    return f"{d:.4g}"


def _render_performance_summary(
    session_state: dict[str, Any],
) -> None:
    """Render the performance summary section."""

    def _safe_decimal(value: Any, fallback: str = "0") -> Decimal:
        try:
            return Decimal(str(value)) if value is not None else Decimal(fallback)
        except Exception:
            return Decimal(fallback)

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        total_fees = _safe_decimal(session_state.get("total_fees_usd", "0"))
        st.metric("Total Fees", f"${float(total_fees):,.2f}")

    with col2:
        il = _safe_decimal(session_state.get("impermanent_loss_pct", "0"))
        st.metric("Impermanent Loss", f"{float(il):+.2f}%")

    with col3:
        net_pnl = _safe_decimal(session_state.get("net_pnl_usd", "0"))
        st.metric("Net PnL", f"${float(net_pnl):+,.2f}")

    with col4:
        position_value = _safe_decimal(session_state.get("total_value_usd", "0"))
        st.metric("Position Value", f"${float(position_value):,.2f}")


# Pre-configured templates for common LP protocols


def get_uniswap_v3_config(
    token0: str = "WETH",
    token1: str = "USDC",
    fee_tier: str = "0.30%",
    chain: str = "arbitrum",
) -> LPDashboardConfig:
    """Get pre-configured Uniswap V3 LP dashboard config."""
    return LPDashboardConfig(
        protocol="uniswap_v3",
        token0=token0,
        token1=token1,
        fee_tier=fee_tier,
        chain=chain,
    )


def get_aerodrome_config(
    token0: str = "WETH",
    token1: str = "USDC",
    pool_type: str = "volatile",
    chain: str = "base",
) -> LPDashboardConfig:
    """Get pre-configured Aerodrome LP dashboard config."""
    return LPDashboardConfig(
        protocol="aerodrome",
        token0=token0,
        token1=token1,
        fee_tier=pool_type,
        chain=chain,
    )


def get_traderjoe_v2_config(
    token0: str = "WAVAX",
    token1: str = "USDC",
    bin_step: str = "20",
    chain: str = "avalanche",
) -> LPDashboardConfig:
    """Get pre-configured TraderJoe V2 LP dashboard config."""
    return LPDashboardConfig(
        protocol="traderjoe_v2",
        token0=token0,
        token1=token1,
        fee_tier=f"Bin Step {bin_step}",
        chain=chain,
    )


def get_pancakeswap_v3_config(
    token0: str = "WBNB",
    token1: str = "USDT",
    fee_tier: str = "0.25%",
    chain: str = "bsc",
) -> LPDashboardConfig:
    """Get pre-configured PancakeSwap V3 LP dashboard config."""
    return LPDashboardConfig(
        protocol="pancakeswap_v3",
        token0=token0,
        token1=token1,
        fee_tier=fee_tier,
        chain=chain,
    )
