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
        # Pass api_client so the template renders the gateway-backed
        # Positions registry + Position Lifecycle sections (PR #2373).
        render_lp_dashboard(strategy_id, strategy_config, session_state, config, api_client=api_client)
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
    pool_address: str | None = None
    token0_address: str | None = None
    token1_address: str | None = None


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

    _normalize_position_id(result)

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

    # VIB-4347: populate position_history + price_history_by_pool via the
    # shared OHLCV stack (DashboardAPIClient.get_ohlcv → factory →
    # OHLCVRouter). Always preserve caller-provided ``price_history`` /
    # ``position_history`` — custom dashboards that supply their own chart
    # data must not regress.
    events = _populate_position_history(api_client, result, config)
    _hydrate_active_position_from_events(result, events, config)
    _populate_price_history_by_pool(api_client, result, config)
    _populate_liquidity_distribution(api_client, result, config)
    _refresh_in_range(result)

    return result


def _normalize_position_id(result: dict[str, Any]) -> None:
    """Map hosted/local active-position state into the LP template key."""
    if not result.get("position_id") and result.get("current_position_id"):
        result["position_id"] = result.get("current_position_id")


def _populate_position_history(
    api_client: Any,
    result: dict[str, Any],
    config: LPDashboardConfig | None = None,
) -> list[dict[str, Any]]:
    """Fetch LP position events into ``session_state``.

    Caller-provided ``position_history`` is left untouched. On any RPC
    error, the function silently logs at debug level — the dashboard
    template already renders a "Position history data not available"
    info banner when the key is missing.
    """
    if api_client is None:
        return []
    if "position_history" in result:
        return []
    try:
        # ``position_type`` column stores ``"LP"`` / ``"PERP"`` etc.;
        # ``event_type`` holds ``OPEN`` / ``CLOSE``. Filter on the
        # column the RPC actually filters on (Codex P2 on PR #2270).
        events = api_client.get_position_events(position_types=["LP"])
        if events:
            # Defer the heavy import until we actually have rows to convert.
            from almanak.framework.dashboard.custom.position_event_adapter import (
                position_events_to_position_data_dicts,
            )

            result["position_history"] = position_events_to_position_data_dicts(
                events,
                token0=config.token0 if config else None,
                token1=config.token1 if config else None,
            )
        else:
            result["position_history"] = []
        return events
    except Exception:
        logger.debug("Failed to fetch position events for LP dashboard", exc_info=True)
        return []


def _hydrate_active_position_from_events(
    result: dict[str, Any],
    events: list[dict[str, Any]],
    config: LPDashboardConfig | None,
) -> None:
    """Fill scalar LP status fields from the latest open position event."""
    if not events:
        return

    by_position: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        pid = str(event.get("position_id") or "")
        if pid:
            by_position.setdefault(pid, []).append(event)

    # Pick the newest active OPEN (no matching CLOSE in its group). The
    # previous loop silently overwrote ``active_open`` per group and was
    # therefore order-dependent — for multi-position strategies (lp_dual,
    # lp_triple) "active" must be the latest unclosed leg by timestamp.
    active_candidates: list[dict[str, Any]] = []
    for group in by_position.values():
        ordered = sorted(group, key=lambda e: e.get("timestamp") or "")
        open_row = next((e for e in ordered if e.get("event_type") == "OPEN"), None)
        close_row = next((e for e in ordered if e.get("event_type") == "CLOSE"), None)
        if open_row is not None and close_row is None:
            active_candidates.append(open_row)

    if not active_candidates:
        return
    active_open = max(active_candidates, key=lambda e: e.get("timestamp") or "")

    if not result.get("position_id"):
        result["position_id"] = active_open.get("position_id")
    result["is_active"] = True
    result.setdefault("lower_tick", active_open.get("tick_lower"))
    result.setdefault("upper_tick", active_open.get("tick_upper"))
    amount0 = _token_amount_to_display(active_open.get("amount0"), config.token0 if config else None)
    amount1 = _token_amount_to_display(active_open.get("amount1"), config.token1 if config else None)
    if amount0 is not None:
        result["token0_amount"] = amount0
    if amount1 is not None:
        result["token1_amount"] = amount1
    result.setdefault("total_value_usd", active_open.get("value_usd") or result.get("total_value_usd") or "0")

    if config is not None:
        lower = _tick_to_display_price(active_open.get("tick_lower"), config)
        upper = _tick_to_display_price(active_open.get("tick_upper"), config)
        if lower is not None and not result.get("range_lower"):
            result["range_lower"] = lower
        if upper is not None and not result.get("range_upper"):
            result["range_upper"] = upper


def _tick_to_display_price(tick: Any, config: LPDashboardConfig) -> float | None:
    """Convert a Uniswap-style tick into token1-per-token0 display price."""
    from math import exp, log

    from almanak.framework.dashboard.custom._token_decimals import TOKEN_DECIMALS

    decimals0 = TOKEN_DECIMALS.get(config.token0.upper())
    decimals1 = TOKEN_DECIMALS.get(config.token1.upper())
    if tick is None or decimals0 is None or decimals1 is None:
        return None
    try:
        return exp(int(tick) * log(1.0001)) * (10 ** (decimals0 - decimals1))
    except (TypeError, ValueError, OverflowError):
        return None


def _token_amount_to_display(amount: Any, symbol: str | None) -> float | None:
    from almanak.framework.dashboard.custom._token_decimals import TOKEN_DECIMALS

    if amount in (None, "") or symbol is None:
        return None
    token_decimals = TOKEN_DECIMALS.get(symbol.upper())
    if token_decimals is None:
        return None
    try:
        return float(Decimal(str(amount)) / (Decimal(10) ** token_decimals))
    except (ArithmeticError, ValueError, TypeError):
        return None


def _refresh_in_range(result: dict[str, Any]) -> None:
    current = result.get("current_price")
    lower = result.get("range_lower")
    upper = result.get("range_upper")
    if current is None or lower is None or upper is None:
        return
    try:
        result["in_range"] = float(lower) <= float(current) <= float(upper)
    except (ValueError, TypeError):
        logger.debug("Non-numeric range bounds — cannot compute in_range")


def _collect_pool_keys(result: dict[str, Any]) -> list[tuple[str, str]]:
    """Collect distinct ``(chain, pool_address)`` tuples from session_state.

    Positions may live under either ``positions`` (custom payload) or
    ``position_history`` (populated by :func:`_populate_position_history`).
    Output order is deterministic (first-seen wins).
    """
    seen: set[tuple[str, str]] = set()
    candidates: list[tuple[str, str]] = []
    sources: list[list[Any]] = []
    if isinstance(result.get("positions"), list):
        sources.append(result["positions"])
    if isinstance(result.get("position_history"), list):
        sources.append(result["position_history"])

    for source in sources:
        for pos in source:
            if not isinstance(pos, dict):
                continue
            pool = pos.get("pool_address") or pos.get("pool")
            chain = pos.get("chain")
            if pool and chain:
                key = (str(chain), str(pool))
                if key not in seen:
                    seen.add(key)
                    candidates.append(key)
    return candidates


def _fetch_pool_candles(
    api_client: Any,
    chain: str,
    pool_address: str | None,
    token0: str,
) -> list[Any]:
    """Best-effort OHLCV fetch for a single pool. Empty list on any failure."""
    try:
        return api_client.get_ohlcv(
            token=token0,
            quote="USD",
            timeframe="1h",
            limit=168,
            chain=chain,
            pool_address=pool_address,
        )
    except Exception:
        logger.debug(
            "Failed to fetch OHLCV for chain=%s pool=%s",
            chain,
            pool_address,
            exc_info=True,
        )
        return []


def _populate_price_history_by_pool(
    api_client: Any,
    result: dict[str, Any],
    config: LPDashboardConfig | None,
) -> None:
    """Fetch lifetime-windowed OHLCV per distinct ``(chain, pool_address)``.

    Grouping by ``(chain, pool_address)`` rather than ``pool_address`` alone
    handles the multi-chain same-address case (a strategy holding analog
    LP positions on Aerodrome / Optimism vs. Base, for example) — without
    the tuple key the second chain would silently overwrite the first.

    Preserves caller-provided ``price_history`` (legacy single-pool field)
    AND caller-provided ``price_history_by_pool`` (new multi-pool field).
    """
    if api_client is None:
        return

    candidates = _collect_pool_keys(result)

    # Always honor caller-provided overrides — never overwrite.
    by_pool = result.get("price_history_by_pool")
    if not isinstance(by_pool, dict):
        by_pool = {}

    token0 = config.token0 if config else "WETH"
    for chain, pool_address in candidates:
        if (chain, pool_address) in by_pool:
            continue
        candles = _fetch_pool_candles(api_client, chain, pool_address, token0)
        if candles:
            by_pool[(chain, pool_address)] = candles

    if by_pool and "price_history_by_pool" not in result:
        result["price_history_by_pool"] = by_pool

    # For legacy single-pool callers, populate ``price_history`` ONLY if the
    # caller didn't provide one (preservation as override) AND there is
    # exactly one pool — picking arbitrarily across pools would mislead.
    if "price_history" not in result and len(by_pool) == 1:
        ((_chain, _pool), candles) = next(iter(by_pool.items()))
        result["price_history"] = candles
    elif "price_history" not in result and not by_pool and config is not None and _has_position_history(result):
        candles = _fetch_pool_candles(api_client, config.chain, None, token0)
        if candles:
            result["price_history"] = candles


def _has_position_history(result: dict[str, Any]) -> bool:
    history = result.get("position_history")
    if isinstance(history, list) and history:
        return True
    positions = result.get("positions")
    return isinstance(positions, list) and bool(positions)


def _populate_liquidity_distribution(
    api_client: Any,
    result: dict[str, Any],
    config: LPDashboardConfig | None,
) -> None:
    if api_client is None or config is None or "tick_data" in result:
        return

    pool_address = config.pool_address
    fee_tier = _fee_tier_to_bps(config.fee_tier)
    if not pool_address and config.token0_address and config.token1_address:
        try:
            pool_address = api_client.get_v3_pool_address(
                chain=config.chain,
                protocol=config.protocol,
                token0_address=config.token0_address,
                token1_address=config.token1_address,
                fee_tier=fee_tier,
            )
        except Exception:
            logger.debug("Failed to resolve LP pool address for liquidity distribution", exc_info=True)
            pool_address = None

    if not pool_address:
        return

    try:
        rows = api_client.get_liquidity_distribution(
            pool_address=pool_address,
            chain=config.chain,
            fee_tier=fee_tier,
            token0=config.token0,
            token1=config.token1,
        )
    except Exception:
        logger.debug("Failed to fetch liquidity distribution", exc_info=True)
        rows = []

    if rows:
        result["tick_data"] = rows
        current_tick = rows[0].get("current_tick") if isinstance(rows[0], dict) else None
        if current_tick is not None:
            result.setdefault("current_tick", current_tick)


def _fee_tier_to_bps(value: Any) -> int:
    if isinstance(value, str) and value.endswith("%"):
        try:
            return int(round(float(value[:-1]) * 10000))
        except ValueError:
            return 3000
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 3000


def render_lp_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    session_state: dict[str, Any],
    config: LPDashboardConfig,
    api_client: Any | None = None,
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
        api_client: Optional ``DashboardAPIClient`` (the same one passed
            into ``render_custom_dashboard``). When supplied, the template
            renders the gateway-backed Positions registry and Position
            Lifecycle sections so AlmanakCode-generated LP dashboards
            inherit the same tables the local detail page shows (PR 2 /
            Problem A2). When ``None`` (legacy callers), those sections
            are skipped silently — backward-compatible with every existing
            ``render_lp_dashboard(...)`` call site.
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

    # Liquidity Distribution
    tick_data = session_state.get("tick_data")
    lower_tick = session_state.get("lower_tick")
    upper_tick = session_state.get("upper_tick")
    position_bounds: tuple[int, int] | list[dict[str, Any]] | None = _liquidity_position_bounds(session_state) or None
    if position_bounds is None and lower_tick is not None and upper_tick is not None:
        position_bounds = (int(lower_tick), int(upper_tick))

    if config.show_liquidity_distribution and _has_data(tick_data):
        st.subheader("Liquidity Distribution")
        fig = plot_liquidity_distribution(
            tick_data=tick_data,
            current_tick=session_state.get("current_tick", 0),
            position_bounds=position_bounds,
            token_pair=f"{token0}/{token1}",
            fee_tier=fee_tier,
            invert_prices=config.invert_prices,
            auto_zoom=False,
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

    # Position registry + lifecycle sections (PR 2 / Problem A2). Reuses the
    # existing gateway-backed ``render_positions_section`` and the new
    # gateway-backed ``render_position_lifecycle_section`` so AlmanakCode-
    # generated LP dashboards (and lp_dual / lp_triple multi-position
    # fixtures, once they ship custom UIs) show the same registry +
    # lifecycle tables that the local detail page renders today. Skipped
    # silently for legacy callers that don't thread ``api_client`` through.
    if api_client is not None:
        _render_lp_position_panels(strategy_id, api_client)

    # Audit — life-to-date costs + per-intent trade tape
    render_cost_stack_section(strategy_id)
    render_trade_tape_section(strategy_id)


def _render_lp_position_panels(strategy_id: str, api_client: Any) -> None:
    """Render the gateway-backed Positions + Position Lifecycle sections.

    Split out so a missing api_client falls through cleanly without
    leaking the import / RPC fanout into the main template body.

    Known gateway-side limitations operators should be aware of:

    - ``DashboardService.GetPositions`` returns OPEN positions only (see
      ``almanak/gateway/services/dashboard_service.py`` — closed/
      reorg_invalidated rows are deferred to a future RPC). On hosted, this
      means that after a multi-leg position is closed its alias
      (``leg_narrow`` / ``leg_wide``) no longer resolves and the lifecycle
      table's CLOSE row renders without the Alias column. Locally, the
      detail-page SQLite reader returns both open and closed rows, so the
      same multi-leg fixture renders WITH the alias. This is a hosted-vs-
      local alias-parity gap that has to be closed via a gateway extension.
    """
    from almanak.framework.dashboard.sections import (
        render_position_lifecycle_section,
    )
    from almanak.framework.dashboard.sections_reconciliation import (
        render_positions_section,
    )
    from almanak.framework.dashboard.service_client import (
        DashboardClientError,
        get_dashboard_service_client,
    )

    # Authoritative registry feed (handles + cutover-state pills). The
    # service client is a lazy singleton — connect before the first call
    # so the section does not surface a "Not connected to gateway" info
    # banner on first paint (the lifecycle section happens to connect the
    # same singleton later, which only helps on a SUBSEQUENT rerun —
    # operators were seeing the registry panel appear broken on first
    # render then work after one Streamlit reload). Mirrors the connect
    # pattern at ``pages/detail.py`` (the local Command Center path).
    service_client = get_dashboard_service_client()
    try:
        if not service_client.is_connected:
            service_client.connect()
        render_positions_section(strategy_id, service_client, heading="### Positions")
    except DashboardClientError as exc:
        st.info(f"Positions temporarily unavailable: {exc}")
    except Exception:
        # Belt-and-braces for unexpected programmer errors (AttributeError
        # from a future edit, Streamlit upgrade incompatibility, …). LOG
        # AT EXCEPTION LEVEL — a DEBUG-only log would make this catch
        # observationally indistinguishable from a successful render with
        # no positions, which the UAT card §A explicitly forbids. The
        # visible warning gives the operator something to correlate with
        # the logs.
        logger.exception("render_positions_section unexpected failure for %s", strategy_id)
        st.warning("Positions panel failed to render — check logs.")

    # Lifecycle table (OPEN / CLOSE rows + PnL attribution). Filters on
    # LP/PERP position types — covers every primitive that emits
    # position_events today.
    try:
        render_position_lifecycle_section(
            strategy_id,
            api_client,
            position_types=["LP", "PERP"],
        )
    except Exception:
        # Same belt-and-braces semantics as the registry section above —
        # log at EXCEPTION level + visible warning so unexpected failures
        # are not laundered as "section rendered successfully with no
        # data" (UAT card §A).
        logger.exception("render_position_lifecycle_section unexpected failure for %s", strategy_id)
        st.warning("Position lifecycle panel failed to render — check logs.")


def _liquidity_position_bounds(session_state: dict[str, Any]) -> list[dict[str, Any]]:
    positions = session_state.get("positions")
    if not isinstance(positions, list):
        return []

    bounds: list[dict[str, Any]] = []
    for idx, pos in enumerate(positions, start=1):
        if not isinstance(pos, dict) or pos.get("is_active") is False:
            continue
        lower = pos.get("lower_tick", pos.get("tick_lower"))
        upper = pos.get("upper_tick", pos.get("tick_upper"))
        if lower is None or upper is None:
            continue
        label = pos.get("registry_handle") or pos.get("label") or pos.get("position_id") or f"Position {idx}"
        bounds.append(
            {
                "lower_tick": lower,
                "upper_tick": upper,
                "label": str(label),
            }
        )
    return bounds


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
        valid = [p for p in positions if isinstance(p, dict) and p.get("is_active") is not False]
        if not valid:
            st.info("No active LP positions.")
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
        position_value = _position_value_usd_for_summary(session_state, _safe_decimal)
        st.metric("Position Value", f"${float(position_value):,.2f}")


def _position_value_usd_for_summary(
    session_state: dict[str, Any],
    parse_decimal: Any,
) -> Decimal:
    positions = session_state.get("positions")
    if not isinstance(positions, list):
        return parse_decimal(session_state.get("total_value_usd", "0"))

    total = Decimal("0")
    seen = False
    for position in positions:
        if not isinstance(position, dict):
            continue
        if position.get("is_active") is False:
            continue
        total += parse_decimal(position.get("total_value_usd", "0"))
        seen = True

    if seen:
        return total
    return parse_decimal(session_state.get("total_value_usd", "0"))


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
