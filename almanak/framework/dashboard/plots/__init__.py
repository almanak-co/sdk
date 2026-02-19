"""Standardized plotting components for Almanak Strategy Dashboards.

This module provides reusable, production-ready visualization components
for strategy dashboards. All plots follow consistent styling and can be
easily integrated into custom strategy dashboards.

Plot Categories:
- LP Plots: Liquidity distribution, positions over time, impermanent loss
- TA Plots: RSI, MACD, Bollinger Bands, Stochastic, price with signals
- Lending Plots: Health factor gauge, LTV ratio, collateral breakdown
- Portfolio Plots: Value over time, PnL waterfall, asset allocation
- Perp Plots: Position dashboard, funding rates, liquidation levels
- Prediction Plots: Binary outcome charts, probability over time

Example:
    from almanak.framework.dashboard.plots import (
        plot_liquidity_distribution,
        plot_positions_over_time,
        plot_rsi_indicator,
        plot_health_factor_gauge,
    )

    # In your strategy dashboard
    fig = plot_liquidity_distribution(
        tick_data=tick_df,
        current_tick=pool.active_tick,
        position_bounds=(lower_tick, upper_tick),
    )
    st.plotly_chart(fig)
"""

# Base utilities and configuration
from almanak.framework.dashboard.plots.base import (
    ChartTheme,
    PlotColors,
    PlotConfig,
    PlotResult,
    get_default_config,
    hex_to_rgba,
)

# Lending protocol plots
from almanak.framework.dashboard.plots.lending_plots import (
    plot_borrow_utilization,
    plot_collateral_breakdown,
    plot_health_factor_gauge,
    plot_lending_rates_comparison,
    plot_ltv_ratio,
)

# LP/DEX plots
from almanak.framework.dashboard.plots.lp_plots import (
    plot_fee_accumulation,
    plot_impermanent_loss,
    plot_liquidity_distribution,
    plot_position_range_status,
    plot_positions_over_time,
)

# Perpetuals plots
from almanak.framework.dashboard.plots.perp_plots import (
    plot_funding_rate_history,
    plot_leverage_gauge,
    plot_liquidation_levels,
    plot_perp_position_dashboard,
)

# Portfolio plots
from almanak.framework.dashboard.plots.portfolio_plots import (
    plot_asset_allocation,
    plot_pnl_waterfall,
    plot_portfolio_value_over_time,
    plot_trade_history,
)

# Prediction market plots
from almanak.framework.dashboard.plots.prediction_plots import (
    plot_arbitrage_opportunity,
    plot_market_outcomes,
    plot_prediction_pnl_breakdown,
    plot_prediction_position,
    plot_probability_over_time,
)

# Technical analysis plots
from almanak.framework.dashboard.plots.ta_plots import (
    calculate_ta_metrics,
    plot_bollinger_bands,
    plot_macd_indicator,
    plot_price_with_signals,
    plot_rsi_indicator,
    plot_stochastic_indicator,
    plot_ta_performance_metrics,
)

__all__ = [
    # Base
    "ChartTheme",
    "PlotColors",
    "PlotConfig",
    "PlotResult",
    "get_default_config",
    "hex_to_rgba",
    # Lending plots
    "plot_borrow_utilization",
    "plot_collateral_breakdown",
    "plot_health_factor_gauge",
    "plot_lending_rates_comparison",
    "plot_ltv_ratio",
    # LP plots
    "plot_fee_accumulation",
    "plot_impermanent_loss",
    "plot_liquidity_distribution",
    "plot_position_range_status",
    "plot_positions_over_time",
    # Perp plots
    "plot_funding_rate_history",
    "plot_leverage_gauge",
    "plot_liquidation_levels",
    "plot_perp_position_dashboard",
    # Portfolio plots
    "plot_asset_allocation",
    "plot_pnl_waterfall",
    "plot_portfolio_value_over_time",
    "plot_trade_history",
    # Prediction plots
    "plot_arbitrage_opportunity",
    "plot_market_outcomes",
    "plot_prediction_pnl_breakdown",
    "plot_prediction_position",
    "plot_probability_over_time",
    # TA plots
    "calculate_ta_metrics",
    "plot_bollinger_bands",
    "plot_macd_indicator",
    "plot_price_with_signals",
    "plot_rsi_indicator",
    "plot_stochastic_indicator",
    "plot_ta_performance_metrics",
]
