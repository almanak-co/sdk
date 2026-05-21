"""Uniswap V3 LP demo dashboard.

Canonical example of the LP template path: build an ``LPDashboardConfig``
from the strategy's ``config.json``, enrich session state via
``prepare_lp_session_state`` (gateway-backed), then call
``render_lp_dashboard``. The renderer owns the title, the strategy header,
and the three audit sections (PnL / Cost Stack / Trade Tape).

For multi-position or multi-signal layouts, hand-roll using the section
helpers and primitive plots directly (see ``blueprints/23-dashboard-plots-and-templates.md``).
"""

from __future__ import annotations

from typing import Any

from almanak.framework.dashboard.templates import (
    LPDashboardConfig,
    prepare_lp_session_state,
    render_lp_dashboard,
)

_FEE_BPS_TO_PCT = {
    "100": "0.01%",
    "500": "0.05%",
    "3000": "0.30%",
    "10000": "1.00%",
}


def _parse_pool(pool: str, default_fee_tier: str) -> tuple[str, str, str]:
    """Parse ``TOKEN0/TOKEN1[/FEE_BPS]`` from ``config.json``.

    ``fee_tier`` can be either embedded in the pool string (``WETH/USDC/3000``)
    or a separate config field. Both layouts are seen across demos.
    """
    parts = [p.strip() for p in pool.split("/") if p.strip()]
    if len(parts) >= 3:
        return parts[0], parts[1], _format_fee_tier(parts[2])
    if len(parts) == 2:
        return parts[0], parts[1], default_fee_tier
    return "WETH", "USDC", default_fee_tier


def _format_fee_tier(value: Any) -> str:
    """Normalise a fee_tier config value to a display string."""
    if isinstance(value, str) and value.endswith("%"):
        return value
    try:
        return _FEE_BPS_TO_PCT.get(str(int(value)), f"{int(value) / 10000:.2f}%")
    except (TypeError, ValueError):
        return "0.30%"


def _token_address(strategy_config: dict[str, Any], symbol: str) -> str | None:
    for item in strategy_config.get("token_funding", []):
        if not isinstance(item, dict):
            continue
        if str(item.get("symbol", "")).upper() == symbol.upper():
            address = item.get("address")
            return str(address) if address else None
    return None


def render_custom_dashboard(
    strategy_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    default_fee_tier = _format_fee_tier(strategy_config.get("fee_tier", 3000))
    token0, token1, fee_tier = _parse_pool(
        str(strategy_config.get("pool", "WETH/USDC")),
        default_fee_tier=default_fee_tier,
    )

    config = LPDashboardConfig(
        protocol="uniswap_v3",
        token0=token0,
        token1=token1,
        fee_tier=fee_tier,
        chain=str(strategy_config.get("chain", "arbitrum")),
        pool_address=str(strategy_config["pool_address"]) if strategy_config.get("pool_address") else None,
        token0_address=_token_address(strategy_config, token0),
        token1_address=_token_address(strategy_config, token1),
    )

    session_state = prepare_lp_session_state(
        api_client,
        session_state=session_state,
        config=config,
    )

    # Pass api_client through so the LP template renders the gateway-backed
    # Positions registry + Position Lifecycle sections (PR #2373 / Problem A2).
    render_lp_dashboard(strategy_id, strategy_config, session_state, config, api_client=api_client)
