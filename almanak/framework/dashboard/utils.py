"""Utility functions for the Almanak Operator Dashboard.

Contains formatting helpers and other utility functions.
"""

import html
import logging
import re
from decimal import Decimal

logger = logging.getLogger(__name__)

# CSS color pattern: hex colors (#RGB, #RRGGBB, #RRGGBBAA), named colors, rgb/rgba
# Note: rgba() restricted to digits, commas, dots, spaces, and % to prevent attribute injection
_COLOR_PATTERN = re.compile(r"^(#[0-9a-fA-F]{3,8}|[a-zA-Z]+|rgba?\([0-9,.\s%]+\))$")

from almanak.framework.dashboard.config import BLOCK_EXPLORER_URLS
from almanak.framework.dashboard.models import (
    AvailableAction,
    ChainHealthStatus,
    Severity,
    StrategyStatus,
    TimelineEventType,
)


def format_usd(value: Decimal) -> str:
    """Format a decimal value as USD."""
    if value >= 0:
        return f"${value:,.2f}"
    else:
        return f"-${abs(value):,.2f}"


def format_token_amount(amount: str | Decimal | int | float, symbol: str = "", chain: str = "") -> str:
    """Render a token amount for the trade-tape headline (VIB-3890).

    Pre-VIB-3890 the trade tape rendered ``891556839636852 WETH`` (raw 18-dec
    integer) and ``0.000868768309352546 WETH`` (full 18-dec precision)
    side-by-side — both unreadable to a Quant scanning a tape.

    The formatter:
    1. Returns the input unchanged when not numerically parseable (``""``,
       protocol-specific aliases like "max", etc.).
    2. Detects raw on-chain integer amounts (heuristic: integer ≥ 10⁶ and
       the token resolver knows decimals) and scales them down before
       formatting.
    3. Renders ≥ 1 with two decimals + thousands separator (``2,294.33``);
       < 1 with up to four significant figures (``0.0008688``); scientific
       for tiny values (≤ 1e-9 → ``8.69e-13``).

    Raw amounts MUST stay verbatim in the receipt-parsed expander block —
    that's the audit trail the Quant audience needs. The formatter is for
    headlines only.
    """
    if amount in (None, "", "—"):
        return "—"
    try:
        d = Decimal(str(amount))
    except (ArithmeticError, ValueError, TypeError):
        return str(amount)
    if not d.is_finite():
        return str(amount)

    # Heuristic raw-units detection: integers ≥ 10⁶ that match a known
    # token's decimals get scaled down. False positives are bounded —
    # human amounts ≥ 10⁶ are rare for the assets that use 18 decimals
    # (a $1M position in WETH is 4_000+ ETH; the raw-int representation
    # would be 4e21, not 4_000). For 6-dec USDC, 1M raw = 1 USDC, also
    # within human range. The resolver's ``decimals`` value is the
    # truth source.
    if symbol and chain and d.is_finite() and d == d.to_integral_value() and abs(d) >= Decimal("1000000"):
        decimals = _try_token_decimals(symbol, chain)
        if decimals is not None and decimals > 0:
            scale = Decimal(10) ** decimals
            d = d / scale

    abs_d = abs(d)
    if abs_d == 0:
        return "0"
    if abs_d >= Decimal("1"):
        return f"{d:,.2f}"
    if abs_d >= Decimal("0.0001"):
        # 4 significant figures for sub-1 values — keep precision for
        # WETH-scale amounts (e.g. 0.000868 stays ``0.0008688``).
        return f"{d:.4g}"
    # Scientific for sub-0.0001 to avoid ``0.0000000000869``.
    return f"{d:.4g}"


def _try_token_decimals(symbol: str, chain: str) -> int | None:
    """Best-effort decimals lookup. Returns None on any failure."""
    try:
        from almanak.framework.data.tokens.resolver import get_token_resolver

        resolver = get_token_resolver()
        info = resolver.resolve(symbol, chain=chain)
        return info.decimals if info is not None else None
    except Exception:
        return None


def format_pnl(value: Decimal) -> str:
    """Format PnL with sign indicator."""
    if value >= 0:
        return f"+${value:,.2f}"
    else:
        return f"-${abs(value):,.2f}"


def pnl_color(value: Decimal, is_stale: bool = False) -> str:
    """Return a CSS hex color for a PnL value.

    Stale/no-data state (is_stale=True) always returns grey regardless of value.
    Zero maps to grey so it is visually distinct from a genuine profit.
    """
    if is_stale or value == 0:
        return "#9e9e9e"
    return "#00c853" if value > 0 else "#f44336"


def format_pnl_display(value: Decimal, is_stale: bool = False) -> str:
    """Return display string for PnL — '--' when stale, otherwise formatted value."""
    if is_stale:
        return "--"
    return format_pnl(value)


def maybe_auto_select_strategy(strategies: list) -> None:
    """Auto-select the sole RUNNING strategy if no strategy_id query param is set.

    Imported by detail, timeline, config, and teardown pages to avoid duplicating
    the same three-line pattern across four files.  Mutates st.query_params and
    calls st.rerun() when it fires — the calling page's execution is aborted.
    """
    import streamlit as st

    if st.query_params.get("strategy_id"):
        return
    running = [s for s in strategies if s.status == StrategyStatus.RUNNING]
    if len(running) == 1:
        st.query_params["strategy_id"] = running[0].id
        st.rerun()


def get_status_icon(status: StrategyStatus) -> str:
    """Get icon for strategy status."""
    icons = {
        StrategyStatus.RUNNING: "\U0001f7e2",  # Green circle
        StrategyStatus.STUCK: "\U0001f7e1",  # Yellow circle
        StrategyStatus.PAUSED: "\u23f8\ufe0f",  # Pause button
        StrategyStatus.ERROR: "\U0001f534",  # Red circle
        StrategyStatus.PAPER_TRADING: "\U0001f535",  # Blue circle
    }
    return icons.get(status, "\u26aa")  # White circle default


def get_severity_icon(severity: Severity) -> str:
    """Get icon for severity level."""
    icons = {
        Severity.LOW: "\u2139\ufe0f",  # Info
        Severity.MEDIUM: "\u26a0\ufe0f",  # Warning
        Severity.HIGH: "\U0001f6a8",  # Alert
        Severity.CRITICAL: "\U0001f534",  # Red circle
    }
    return icons.get(severity, "\u26aa")  # White circle default


def get_action_label(action: AvailableAction) -> str:
    """Get human-readable label for action."""
    labels = {
        AvailableAction.BUMP_GAS: "\u26fd Bump Gas",
        AvailableAction.CANCEL_TX: "\u274c Cancel TX",
        AvailableAction.PAUSE: "\u23f8\ufe0f Pause Strategy",
        AvailableAction.RESUME: "\u25b6\ufe0f Resume Strategy",
        AvailableAction.EMERGENCY_UNWIND: "\U0001f6a8 Emergency Unwind",
    }
    return labels.get(action, action.value)


def get_timeline_event_icon(event_type: TimelineEventType) -> str:
    """Get icon for timeline event type."""
    icons = {
        TimelineEventType.TRADE: "\U0001f504",  # Arrows
        TimelineEventType.SWAP: "\U0001f504",  # Arrows
        TimelineEventType.REBALANCE: "\u2696\ufe0f",  # Balance scale
        TimelineEventType.DEPOSIT: "\U0001f4e5",  # Inbox
        TimelineEventType.WITHDRAWAL: "\U0001f4e4",  # Outbox
        TimelineEventType.LP_OPEN: "\U0001f7e2",  # Green circle
        TimelineEventType.LP_CLOSE: "\U0001f534",  # Red circle
        TimelineEventType.BORROW: "\U0001f4b3",  # Credit card
        TimelineEventType.REPAY: "\U0001f4b0",  # Money bag
        TimelineEventType.ALERT: "\u26a0\ufe0f",  # Warning
        TimelineEventType.ERROR: "\u274c",  # X mark
        TimelineEventType.CONFIG_UPDATE: "\u2699\ufe0f",  # Gear
        TimelineEventType.STATE_CHANGE: "\U0001f4cb",  # Clipboard
        TimelineEventType.TRANSACTION_SUBMITTED: "\u27a1\ufe0f",  # Right arrow
        TimelineEventType.TRANSACTION_CONFIRMED: "\u2705",  # Check mark
        TimelineEventType.TRANSACTION_FAILED: "\u274c",  # X mark
        TimelineEventType.TRANSACTION_REVERTED: "\u274c",  # X mark
        TimelineEventType.STRATEGY_STARTED: "\U0001f680",  # Rocket
        TimelineEventType.STRATEGY_PAUSED: "\u23f8\ufe0f",  # Pause
        TimelineEventType.STRATEGY_RESUMED: "\u25b6\ufe0f",  # Play
        TimelineEventType.STRATEGY_STOPPED: "\U0001f6d1",  # Stop sign
        TimelineEventType.OPERATOR_ACTION_EXECUTED: "\U0001f464",  # Person
        TimelineEventType.RISK_GUARD_TRIGGERED: "\U0001f6e1\ufe0f",  # Shield
        TimelineEventType.CIRCUIT_BREAKER_TRIGGERED: "\u26a1",  # Lightning
        # Bridge events
        TimelineEventType.BRIDGE_INITIATED: "\U0001f310",  # Globe
        TimelineEventType.BRIDGE_COMPLETED: "\u2705",  # Check mark
        TimelineEventType.BRIDGE_FAILED: "\u274c",  # X mark
    }
    return icons.get(event_type, "\U0001f4cc")  # Pin default


def get_block_explorer_url(chain: str, tx_hash: str) -> str:
    """Get block explorer URL for a transaction."""
    chain_lower = chain.lower()
    base_url = BLOCK_EXPLORER_URLS.get(chain_lower)
    if base_url is None:
        logger.warning("No block explorer configured for chain '%s', falling back to etherscan", chain)
        base_url = "https://etherscan.io/tx/"
    return f"{base_url}{tx_hash}"


def get_event_type_category(event_type: TimelineEventType, description: str | None = None) -> str:
    """Categorize event type for color coding.

    Args:
        event_type: The type of timeline event
        description: Optional event description to detect failures in TRADE events

    Returns:
        Category string: "success", "warning", "error", or "neutral"
    """
    # Check if this is a failed TRADE event based on description
    if event_type == TimelineEventType.TRADE and description:
        description_lower = description.lower()
        # Check for failure indicators in description
        failure_indicators = ["failed", "error", "✗", "cannot", "connection", "timeout", "revert"]
        if any(indicator in description_lower for indicator in failure_indicators):
            return "error"

    success_types = {
        TimelineEventType.TRADE,
        TimelineEventType.SWAP,
        TimelineEventType.DEPOSIT,
        TimelineEventType.LP_OPEN,
        TimelineEventType.REPAY,
        TimelineEventType.BRIDGE_COMPLETED,
        TimelineEventType.TRANSACTION_CONFIRMED,
        TimelineEventType.STRATEGY_STARTED,
        TimelineEventType.STRATEGY_RESUMED,
    }
    warning_types = {
        TimelineEventType.ALERT,
        TimelineEventType.REBALANCE,
        TimelineEventType.CONFIG_UPDATE,
        TimelineEventType.STATE_CHANGE,
        TimelineEventType.WITHDRAWAL,
        TimelineEventType.BORROW,
        TimelineEventType.BRIDGE_INITIATED,
        TimelineEventType.TRANSACTION_SUBMITTED,
        TimelineEventType.STRATEGY_PAUSED,
        TimelineEventType.STRATEGY_STOPPED,
        TimelineEventType.OPERATOR_ACTION_EXECUTED,
        TimelineEventType.RISK_GUARD_TRIGGERED,
        TimelineEventType.LP_CLOSE,
    }
    error_types = {
        TimelineEventType.ERROR,
        TimelineEventType.BRIDGE_FAILED,
        TimelineEventType.TRANSACTION_FAILED,
        TimelineEventType.TRANSACTION_REVERTED,
        TimelineEventType.CIRCUIT_BREAKER_TRIGGERED,
    }

    if event_type in success_types:
        return "success"
    elif event_type in warning_types:
        return "warning"
    elif event_type in error_types:
        return "error"
    return "neutral"


def get_chain_icon(chain: str) -> str:
    """Get icon for a chain."""
    chain_icons = {
        "ethereum": "\u2666\ufe0f",  # Diamond
        "arbitrum": "\U0001f535",  # Blue circle
        "optimism": "\U0001f534",  # Red circle
        "base": "\U0001f7e6",  # Blue square
        "polygon": "\U0001f7e3",  # Purple circle
        "avalanche": "\u2744\ufe0f",  # Snowflake
        "bsc": "\U0001f7e1",  # Yellow circle
    }
    return chain_icons.get(chain.lower(), "\U0001f310")  # Globe default


def get_chain_health_icon(status: ChainHealthStatus) -> str:
    """Get icon for chain health status."""
    icons = {
        ChainHealthStatus.HEALTHY: "\U0001f7e2",  # Green circle
        ChainHealthStatus.DEGRADED: "\U0001f7e1",  # Yellow circle
        ChainHealthStatus.UNAVAILABLE: "\U0001f534",  # Red circle
    }
    return icons.get(status, "\u26aa")  # White circle default


def _sanitize_color(color: str) -> str:
    """Validate and return color, or fallback to safe default.

    Prevents XSS via malicious color values that could break out of style attributes.
    """
    if _COLOR_PATTERN.match(color):
        return color
    return "#888888"  # Safe fallback


def format_chain_badge(chain: str, color: str) -> str:
    """Generate HTML for a chain badge."""
    safe_chain = html.escape(chain)
    safe_color = _sanitize_color(color)
    return f'<span style="background-color: {safe_color}22; color: {safe_color}; padding: 0.15rem 0.5rem; border-radius: 12px; font-size: 0.75rem; font-weight: bold; text-transform: uppercase; margin-right: 0.25rem;">{safe_chain}</span>'


def format_bridge_progress(from_chain: str, to_chain: str, status: str, progress_pct: int = 0) -> str:
    """Generate HTML for bridge transfer progress visualization."""
    from almanak.framework.dashboard.theme import get_chain_color

    from_color = get_chain_color(from_chain)
    to_color = get_chain_color(to_chain)

    # Status-specific styling
    if status == "COMPLETED":
        progress_color = "#00c853"
        progress_pct = 100
    elif status == "FAILED":
        progress_color = "#f44336"
    else:
        progress_color = "#2196f3"

    return (
        f'<div style="display: flex; align-items: center; gap: 0.5rem; margin: 0.5rem 0;">'
        f'<span style="background-color: {from_color}22; color: {from_color}; padding: 0.2rem 0.5rem; border-radius: 4px; font-size: 0.8rem; font-weight: bold;">{from_chain.upper()}</span>'
        f'<div style="flex-grow: 1; height: 4px; background-color: #333; border-radius: 2px; position: relative; overflow: hidden;">'
        f'<div style="position: absolute; left: 0; top: 0; height: 100%; width: {progress_pct}%; background-color: {progress_color}; border-radius: 2px;"></div>'
        f"</div>"
        f'<span style="background-color: {to_color}22; color: {to_color}; padding: 0.2rem 0.5rem; border-radius: 4px; font-size: 0.8rem; font-weight: bold;">{to_chain.upper()}</span>'
        f"</div>"
    )


def format_timeline_summary(event_type: TimelineEventType, description: str, details: dict[str, object]) -> str:
    """Render a concise human-readable timeline summary."""

    def _to_text(value: object) -> str:
        if value is None:
            return ""
        return str(value).strip()

    def _safe(value: str) -> str:
        # Timeline strings are rendered inside unsafe_allow_html blocks.
        return html.escape(value)

    token_in = str(details.get("token_in", "")).strip()
    token_out = str(details.get("token_out", "")).strip()
    amount_in = str(details.get("amount_in", "")).strip()
    amount_out = str(details.get("amount_out", "")).strip()
    protocol = _to_text(details.get("protocol"))
    slippage = _to_text(details.get("slippage"))

    if event_type in {TimelineEventType.SWAP, TimelineEventType.TRADE} and token_in and token_out:
        if amount_in and amount_out:
            summary = f"Swapped {amount_in} {token_in} -> {amount_out} {token_out}"
        else:
            summary = f"Swapped {token_in} -> {token_out}"
        if protocol:
            summary += f" on {protocol}"
        if slippage:
            summary += f" (slippage: {slippage})"
        return _safe(summary)

    if event_type == TimelineEventType.LP_OPEN:
        pool = str(details.get("pool", "")).strip()
        position_id = str(details.get("position_id", "")).strip()
        liquidity_usd = _to_text(details.get("liquidity_usd"))
        if pool and position_id:
            summary = f"Opened LP position #{position_id} in {pool}"
            if liquidity_usd:
                summary += f" (${liquidity_usd})"
            return _safe(summary)
        if pool:
            return _safe(f"Opened LP position in {pool}")

    if event_type == TimelineEventType.TRANSACTION_CONFIRMED:
        block = details.get("block_number")
        gas = details.get("gas_used")
        if block is not None and gas is not None:
            return _safe(f"Transaction confirmed in block {block} (gas: {gas})")
        return _safe("Transaction confirmed")

    if event_type == TimelineEventType.TRANSACTION_SUBMITTED:
        return _safe("Transaction submitted")

    if event_type in {TimelineEventType.STRATEGY_PAUSED, TimelineEventType.STRATEGY_RESUMED}:
        reason = str(details.get("pause_reason", "")).strip()
        if reason:
            return _safe(f"{description} ({reason})")

    if event_type == TimelineEventType.OPERATOR_ACTION_EXECUTED:
        action = _to_text(details.get("action"))
        actor = _to_text(details.get("actor"))
        if action and actor:
            return _safe(f"Operator action: {action} by {actor}")
        if action:
            return _safe(f"Operator action: {action}")

    if event_type in {TimelineEventType.RISK_GUARD_TRIGGERED, TimelineEventType.CIRCUIT_BREAKER_TRIGGERED}:
        reason = _to_text(details.get("reason")) or _to_text(details.get("message"))
        if reason:
            return _safe(reason)

    return _safe(description)
