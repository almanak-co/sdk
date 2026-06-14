"""Multi-position dual-range LP demo dashboard.

Uses the canonical LP template path (``LPDashboardConfig`` +
``prepare_lp_session_state`` + ``render_lp_dashboard``). The Positions
registry section will show both narrow and wide legs once they are
populated; the per-leg ``registry_handle`` (``leg_narrow`` / ``leg_wide``)
disambiguates them in the registry view.
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
    parts = [p.strip() for p in pool.split("/") if p.strip()]
    if len(parts) >= 3:
        return parts[0], parts[1], _format_fee_tier(parts[2])
    if len(parts) == 2:
        return parts[0], parts[1], default_fee_tier
    return "WETH", "USDC", default_fee_tier


def _format_fee_tier(value: Any) -> str:
    if isinstance(value, str) and value.endswith("%"):
        return value
    try:
        return _FEE_BPS_TO_PCT.get(str(int(value)), f"{int(value) / 10000:.2f}%")
    except (TypeError, ValueError):
        return "0.05%"


def render_custom_dashboard(
    deployment_id: str,
    strategy_config: dict[str, Any],
    api_client: Any,
    session_state: dict[str, Any],
) -> None:
    default_fee_tier = _format_fee_tier(strategy_config.get("fee_tier", 500))
    token0, token1, fee_tier = _parse_pool(
        str(strategy_config.get("pool", "WETH/USDC/500")),
        default_fee_tier=default_fee_tier,
    )

    config = LPDashboardConfig(
        protocol="uniswap_v3",
        token0=token0,
        token1=token1,
        fee_tier=fee_tier,
        chain=str(strategy_config.get("chain", "arbitrum")),
    )

    session_state = prepare_lp_session_state(
        api_client,
        session_state=session_state,
        config=config,
        deployment_id=deployment_id,
    )

    render_lp_dashboard(deployment_id, strategy_config, session_state, config, api_client=api_client)
