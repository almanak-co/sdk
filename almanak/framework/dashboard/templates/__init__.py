"""Dashboard templates for common strategy types.

This module provides pre-built dashboard templates that can be easily
customized for specific strategies. Each template uses the standardized
plots from almanak.framework.dashboard.plots.

Available Templates:
- TADashboardConfig / render_ta_dashboard: Technical Analysis strategies
- LPDashboardConfig / render_lp_dashboard: Liquidity Provider strategies
- LendingDashboardConfig / render_lending_dashboard: Lending protocol strategies
- PerpDashboardConfig / render_perp_dashboard: Perpetual futures strategies
- PredictionDashboardConfig / render_prediction_dashboard: Prediction market strategies

Example:
    from almanak.framework.dashboard.templates import (
        LPDashboardConfig,
        render_lp_dashboard,
        get_uniswap_v3_config,
    )

    # Option 1: Use pre-configured template
    config = get_uniswap_v3_config(token0="WETH", token1="USDC")

    # Option 2: Custom configuration
    config = LPDashboardConfig(
        protocol="uniswap_v3",
        token0="WETH",
        token1="USDC",
        fee_tier="0.30%",
        chain="arbitrum",
    )

    def render_custom_dashboard(strategy_id, strategy_config, api_client, session_state):
        render_lp_dashboard(strategy_id, strategy_config, session_state, config)
"""

# Technical Analysis templates
# Lending protocol templates
from almanak.framework.dashboard.templates.lending_dashboard import (
    LendingDashboardConfig,
    get_aave_v3_config,
    get_compound_v3_config,
    get_morpho_blue_config,
    get_spark_config,
    render_lending_dashboard,
)

# LP/DEX templates
from almanak.framework.dashboard.templates.lp_dashboard import (
    LP_CRITICAL_KEYS,
    LPDashboardConfig,
    LPSessionState,
    get_aerodrome_config,
    get_pancakeswap_v3_config,
    get_traderjoe_v2_config,
    get_uniswap_v3_config,
    prepare_lp_session_state,
    render_lp_dashboard,
)

# Perpetual futures templates
from almanak.framework.dashboard.templates.perp_dashboard import (
    PerpDashboardConfig,
    get_gmx_v2_config,
    get_hyperliquid_config,
    render_perp_dashboard,
)

# Prediction market templates
from almanak.framework.dashboard.templates.prediction_dashboard import (
    PredictionDashboardConfig,
    get_polymarket_arbitrage_config,
    get_polymarket_config,
    render_prediction_dashboard,
)
from almanak.framework.dashboard.templates.ta_dashboard import (
    TADashboardConfig,
    get_adx_config,
    get_atr_config,
    get_bollinger_config,
    get_cci_config,
    get_macd_config,
    get_rsi_config,
    get_stochastic_config,
    prepare_ta_session_state,
    render_ta_dashboard,
)

__all__ = [
    # TA templates
    "TADashboardConfig",
    "render_ta_dashboard",
    "prepare_ta_session_state",
    "get_rsi_config",
    "get_macd_config",
    "get_cci_config",
    "get_stochastic_config",
    "get_atr_config",
    "get_adx_config",
    "get_bollinger_config",
    # LP templates
    "LPDashboardConfig",
    "LPSessionState",
    "LP_CRITICAL_KEYS",
    "render_lp_dashboard",
    "prepare_lp_session_state",
    "get_uniswap_v3_config",
    "get_aerodrome_config",
    "get_traderjoe_v2_config",
    "get_pancakeswap_v3_config",
    # Lending templates
    "LendingDashboardConfig",
    "render_lending_dashboard",
    "get_aave_v3_config",
    "get_morpho_blue_config",
    "get_compound_v3_config",
    "get_spark_config",
    # Perp templates
    "PerpDashboardConfig",
    "render_perp_dashboard",
    "get_gmx_v2_config",
    "get_hyperliquid_config",
    # Prediction templates
    "PredictionDashboardConfig",
    "render_prediction_dashboard",
    "get_polymarket_config",
    "get_polymarket_arbitrage_config",
]
