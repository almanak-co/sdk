#!/usr/bin/env python3
"""
===============================================================================
ALMANAK SDK DEMO: Dashboard Templates Showcase
===============================================================================

This demo showcases how EASY it is to create DeFi strategy dashboards using
the Almanak SDK's built-in dashboard templates.

✨ KEY POINT: You don't write any Streamlit plot code - just call the template!

Example:
    from almanak.framework.dashboard.templates import (
        get_uniswap_v3_config,
        render_lp_dashboard,
    )
    
    config = get_uniswap_v3_config(token0="WETH", token1="USDC")
    render_lp_dashboard(strategy_id, strategy_config, session_state, config)
    # That's it! Full dashboard with all LP plots rendered automatically.

This demo shows all available dashboard templates:
- LP Dashboard (Uniswap V3, TraderJoe V2, Aerodrome, PancakeSwap V3)
- TA Dashboard (RSI, MACD, Bollinger Bands, Stochastic)
- Lending Dashboard (Aave V3, Morpho Blue, Compound V3)
- Perp Dashboard (GMX V2, Hyperliquid)
- Prediction Dashboard (Polymarket)

USAGE:
------
    streamlit run examples/demo_dashboard.py
    
    # Or with custom port
    streamlit run examples/demo_dashboard.py --server.port 8502

===============================================================================
"""

import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import pandas as pd
import streamlit as st

# Import dashboard templates
from almanak.framework.dashboard.templates import (
    LPDashboardConfig,
    LendingDashboardConfig,
    PerpDashboardConfig,
    PredictionDashboardConfig,
    TADashboardConfig,
    get_aave_v3_config,
    get_rsi_config,
    get_uniswap_v3_config,
    render_lending_dashboard,
    render_lp_dashboard,
    render_perp_dashboard,
    render_prediction_dashboard,
    render_ta_dashboard,
)

# Import individual plots for custom sections
from almanak.framework.dashboard.plots import (
    plot_asset_allocation,
    plot_pnl_waterfall,
    plot_portfolio_value_over_time,
)

# Mock data generators
from almanak.framework.dashboard.plots.lp_plots import PositionData, TickData


def generate_lp_tick_data(current_price: float = 3400.0, num_ticks: int = 200) -> list[TickData]:
    """Generate mock tick data for liquidity distribution."""
    tick_data = []
    base_tick = 200000  # Approximate tick for $3400 ETH/USDC
    
    for i in range(-num_ticks // 2, num_ticks // 2):
        tick_idx = base_tick + i * 60  # 60 ticks per 1% price change
        price = current_price * (1.01 ** (i / 100))
        
        # Generate liquidity with concentration around current price
        distance = abs(i)
        liquidity = max(1000000 * (0.95 ** distance), 10000)
        
        tick_data.append(
            TickData(
                tick_idx=tick_idx,
                liquidity_active=Decimal(str(liquidity)),
                price0=price,
                price1=1.0 / price,
            )
        )
    
    return tick_data


def generate_lp_position_history() -> list[PositionData]:
    """Generate mock LP position history."""
    base_time = datetime.now(UTC) - timedelta(days=30)
    positions = []
    
    # Position 1: Closed position
    positions.append(
        PositionData(
            position_id="pos_001",
            date_start=base_time,
            date_end=base_time + timedelta(days=10),
            bound_tick_lower=198000,
            bound_tick_upper=202000,
            bound_price_lower=3200.0,
            bound_price_upper=3600.0,
            token0_amount=Decimal("0.1"),
            token1_amount=Decimal("340"),
            fees_collected=Decimal("12.50"),
            is_active=False,
        )
    )
    
    # Position 2: Active position
    positions.append(
        PositionData(
            position_id="pos_002",
            date_start=base_time + timedelta(days=15),
            date_end=None,
            bound_tick_lower=199000,
            bound_tick_upper=201000,
            bound_price_lower=3300.0,
            bound_price_upper=3500.0,
            token0_amount=Decimal("0.15"),
            token1_amount=Decimal("510"),
            fees_collected=Decimal("8.20"),
            is_active=True,
        )
    )
    
    return positions


def generate_price_history(days: int = 30) -> list[tuple[datetime, float]]:
    """Generate mock price history."""
    base_time = datetime.now(UTC) - timedelta(days=days)
    prices = []
    base_price = 3400.0
    
    for i in range(days * 24):  # Hourly data
        timestamp = base_time + timedelta(hours=i)
        # Random walk with slight upward trend
        change = (i % 100 - 50) / 1000.0 + 0.0001
        base_price *= (1 + change)
        prices.append((timestamp, base_price))
    
    return prices


# =============================================================================
# PAGE CONFIGURATION
# =============================================================================

st.set_page_config(
    page_title="Almanak Dashboard Showcase",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# SIDEBAR NAVIGATION
# =============================================================================

st.sidebar.title("📊 Dashboard Showcase")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Select Dashboard Type",
    [
        "🏠 Overview",
        "💧 LP Dashboard",
        "📈 TA Dashboard",
        "🏦 Lending Dashboard",
        "⚡ Perp Dashboard",
        "🔮 Prediction Dashboard",
        "💼 Portfolio Plots",
    ],
)

st.sidebar.markdown("---")
st.sidebar.markdown("### About")
st.sidebar.info(
    """
    This showcase demonstrates all available dashboard
    templates and plots in the Almanak SDK.
    
    Each dashboard uses mock data to show how the
    templates work with real strategy data.
    """
)

# =============================================================================
# MOCK SESSION STATE
# =============================================================================

# Initialize session state with mock data
if "initialized" not in st.session_state:
    st.session_state.initialized = True
    
    # LP data
    st.session_state.lp_tick_data = generate_lp_tick_data()
    st.session_state.lp_current_tick = 200000
    st.session_state.lp_position_history = generate_lp_position_history()
    st.session_state.lp_current_price = Decimal("3450.0")
    st.session_state.lp_lower_price = Decimal("3300.0")
    st.session_state.lp_upper_price = Decimal("3500.0")
    st.session_state.lp_position_id = "pos_002"
    st.session_state.lp_is_active = True
    st.session_state.lp_in_range = True
    st.session_state.lp_token0_amount = Decimal("0.15")
    st.session_state.lp_token1_amount = Decimal("510")
    st.session_state.lp_fee_history = [
        (datetime.now(UTC) - timedelta(days=30 - i), 0.5 + i * 0.3)
        for i in range(30)
    ]
    st.session_state.lp_il_history = [
        (datetime.now(UTC) - timedelta(days=30 - i), -0.5 - i * 0.1)
        for i in range(30)
    ]
    
    # TA data
    ta_price_history = generate_price_history()
    st.session_state.ta_price_history = ta_price_history
    ta_rsi_data = [
        (ts, 30 + (i % 60))
        for i, (ts, _) in enumerate(ta_price_history[::24])
    ]
    st.session_state.ta_rsi_data = ta_rsi_data
    
    # Generate buy/sell signals based on RSI
    buy_signals = []
    sell_signals = []
    for i, (ts, rsi) in enumerate(ta_rsi_data):
        if rsi < 30:  # Oversold - buy signal
            price_idx = min(i * 24, len(ta_price_history) - 1)
            price_at_time = ta_price_history[price_idx][1]
            buy_signals.append((ts, price_at_time))
        elif rsi > 70:  # Overbought - sell signal
            price_idx = min(i * 24, len(ta_price_history) - 1)
            price_at_time = ta_price_history[price_idx][1]
            sell_signals.append((ts, price_at_time))
    
    st.session_state.ta_buy_signals = buy_signals[:10] if buy_signals else []  # Limit to 10 for demo
    st.session_state.ta_sell_signals = sell_signals[:10] if sell_signals else []  # Limit to 10 for demo
    
    # Lending data
    st.session_state.lending_health_factor = Decimal("1.85")
    st.session_state.lending_ltv = Decimal("0.65")
    st.session_state.lending_collateral_assets = {
        "WETH": 50000,
        "wstETH": 30000,
        "USDC": 20000,
    }
    
    # Perp data
    st.session_state.perp_entry_price = 2500.0
    st.session_state.perp_current_price = 2650.0
    st.session_state.perp_liquidation_price = 2100.0
    st.session_state.perp_is_long = True
    st.session_state.perp_size_usd = 50000.0
    st.session_state.perp_leverage = 5.0
    st.session_state.perp_collateral_usd = 10000.0
    st.session_state.perp_unrealized_pnl = 3000.0
    
    # Prediction data
    st.session_state.prediction_yes_shares = 1000.0
    st.session_state.prediction_no_shares = 0.0
    st.session_state.prediction_yes_price = 0.65
    st.session_state.prediction_no_price = 0.35
    st.session_state.prediction_cost_basis = 600.0
    st.session_state.prediction_market_question = "Will ETH reach $5000 by Dec 2024?"


# =============================================================================
# PAGES
# =============================================================================

if page == "🏠 Overview":
    st.title("📊 Almanak Dashboard Showcase")
    st.markdown("---")
    
    st.markdown("""
    ## 🎯 Almanak SDK Dashboard Templates
    
    **The whole point**: Create professional DeFi strategy dashboards with **zero custom plot code**.
    
    Just call a template function and get a complete dashboard with all relevant plots
    for your strategy type. The SDK handles all the Streamlit rendering, plot creation,
    and layout automatically.
    
    ### How Easy Is It?
    
    ```python
    # That's literally it - 3 lines of code!
    from almanak.framework.dashboard.templates import get_uniswap_v3_config, render_lp_dashboard
    
    config = get_uniswap_v3_config(token0="WETH", token1="USDC")
    render_lp_dashboard(strategy_id, strategy_config, session_state, config)
    ```
    
    You get:
    - ✅ Liquidity distribution chart
    - ✅ Position history over time
    - ✅ Fee accumulation tracking
    - ✅ Impermanent loss monitoring
    - ✅ Range status indicators
    - ✅ All properly styled and laid out
    
    **No custom Streamlit code needed!**
    
    ### Available Dashboard Templates:
    
    #### 💧 LP Dashboard
    - Liquidity distribution visualization
    - Position history over time
    - Fee accumulation tracking
    - Impermanent loss monitoring
    - Range status indicators
    
    #### 📈 TA Dashboard
    - Price charts with trading signals
    - RSI, MACD, Bollinger Bands indicators
    - Stochastic oscillator
    - Performance metrics
    
    #### 🏦 Lending Dashboard
    - Health factor gauge
    - LTV ratio visualization
    - Collateral breakdown
    - Borrow utilization
    - Rate comparisons across protocols
    
    #### ⚡ Perp Dashboard
    - Position dashboard with entry/current/liquidation prices
    - Funding rate history
    - Leverage gauge
    - Liquidation level indicators
    
    #### 🔮 Prediction Dashboard
    - Position visualization
    - Probability over time
    - Market outcomes comparison
    - Arbitrage opportunities
    - PnL breakdown
    
    #### 💼 Portfolio Plots
    - Portfolio value over time
    - PnL waterfall breakdown
    - Asset allocation pie charts
    - Trade history visualization
    
    ---
    
    ### Try It Out
    
    1. **Select a dashboard type** from the sidebar
    2. **See the complete dashboard** rendered automatically
    3. **Notice**: All plots are SDK-provided, no custom code!
    
    ### What You Get
    
    Each template provides **strategy-specific visualizations**:
    
    - **LP Dashboard**: Liquidity distribution, position ranges, fees, IL
    - **TA Dashboard**: Price charts, RSI/MACD indicators, performance metrics
    - **Lending Dashboard**: Health factor gauges, LTV ratios, collateral breakdowns
    - **Perp Dashboard**: Position dashboards, funding rates, liquidation levels
    - **Prediction Dashboard**: Market positions, probability charts, arbitrage analysis
    
    All plots are **DeFi-aware** and **protocol-specific** - the SDK knows what metrics
    matter for each strategy type.
    """)

elif page == "💧 LP Dashboard":
    st.header("💧 LP Dashboard Template")
    st.info("✨ This entire dashboard is rendered by calling `render_lp_dashboard()` - no custom Streamlit code!")
    
    with st.expander("📝 See the code that renders this dashboard"):
        st.code("""
from almanak.framework.dashboard.templates import (
    get_uniswap_v3_config,
    render_lp_dashboard,
)

config = get_uniswap_v3_config(token0="WETH", token1="USDC", fee_tier="0.30%")
render_lp_dashboard(strategy_id, strategy_config, session_state, config)
# That's it! Full dashboard with all LP plots.
""", language="python")
    
    config = get_uniswap_v3_config(
        token0="WETH",
        token1="USDC",
        fee_tier="0.30%",
        chain="arbitrum",
    )
    
    # Mock strategy config and session state
    strategy_config = {
        "protocol": "uniswap_v3",
        "token0": "WETH",
        "token1": "USDC",
        "fee_tier": "0.30%",
        "chain": "arbitrum",
    }
    
    # Convert price history list to DataFrame (required by plot_positions_over_time)
    price_history_list = st.session_state.ta_price_history
    price_df = pd.DataFrame(price_history_list, columns=["timestamp", "price"])
    
    session_state = {
        "position_id": st.session_state.lp_position_id,
        "is_active": st.session_state.lp_is_active,
        "in_range": st.session_state.lp_in_range,
        "current_price": st.session_state.lp_current_price,
        "lower_price": st.session_state.lp_lower_price,
        "upper_price": st.session_state.lp_upper_price,
        "token0_amount": st.session_state.lp_token0_amount,
        "token1_amount": st.session_state.lp_token1_amount,
        "tick_data": st.session_state.lp_tick_data,
        "current_tick": st.session_state.lp_current_tick,
        "position_history": st.session_state.lp_position_history,
        "price_history": price_df,  # DataFrame instead of list
        "fee_history": st.session_state.lp_fee_history,
        "il_history": st.session_state.lp_il_history,
    }
    
    render_lp_dashboard(
        strategy_id="demo_lp_strategy",
        strategy_config=strategy_config,
        session_state=session_state,
        config=config,
    )

elif page == "📈 TA Dashboard":
    st.header("📈 TA Dashboard Template")
    st.info("✨ This entire dashboard is rendered by calling `render_ta_dashboard()` - no custom Streamlit code!")
    
    with st.expander("📝 See the code that renders this dashboard"):
        st.code("""
from almanak.framework.dashboard.templates import get_rsi_config, render_ta_dashboard

config = get_rsi_config(period=14, overbought=70, oversold=30)
render_ta_dashboard(strategy_id, strategy_config, session_state, config)
# Complete TA dashboard with RSI indicator, signals, and performance metrics.
""", language="python")
    
    config = get_rsi_config(period=14, overbought=70, oversold=30)
    
    strategy_config = {
        "base_token": "WETH",
        "quote_token": "USDC",
        "protocol": "uniswap_v3",
        "chain": "arbitrum",
    }
    
    # Get latest RSI value from history
    latest_rsi = st.session_state.ta_rsi_data[-1][1] if st.session_state.ta_rsi_data else 50
    
    # Convert price history to DataFrame
    price_df = pd.DataFrame(st.session_state.ta_price_history, columns=["time", "price"])
    
    # Convert signals to DataFrames
    buy_df = pd.DataFrame(st.session_state.ta_buy_signals, columns=["time", "price"]) if st.session_state.ta_buy_signals else None
    sell_df = pd.DataFrame(st.session_state.ta_sell_signals, columns=["time", "price"]) if st.session_state.ta_sell_signals else None
    
    session_state = {
        "rsi_value": latest_rsi,
        "rsi": latest_rsi,  # Alternative key
        "price_history": price_df,  # DataFrame for charts
        "rsi_data": st.session_state.ta_rsi_data,  # List of (timestamp, rsi) tuples
        "buy_signals": buy_df,  # DataFrame with buy signals
        "sell_signals": sell_df,  # DataFrame with sell signals
        "base_balance": Decimal("0.5"),
        "quote_balance": Decimal("1700"),
        "base_price": Decimal("3400"),
        "total_pnl": Decimal("1250.50"),
        "total_trades": 42,
        "win_rate": Decimal("65.5"),
    }
    
    render_ta_dashboard(
        strategy_id="demo_ta_strategy",
        strategy_config=strategy_config,
        session_state=session_state,
        config=config,
    )

elif page == "🏦 Lending Dashboard":
    st.header("🏦 Lending Dashboard Template")
    st.info("✨ This entire dashboard is rendered by calling `render_lending_dashboard()` - no custom Streamlit code!")
    
    with st.expander("📝 See the code that renders this dashboard"):
        st.code("""
from almanak.framework.dashboard.templates import (
    get_aave_v3_config,
    render_lending_dashboard,
)

config = get_aave_v3_config(collateral_token="WETH", borrow_token="USDC")
render_lending_dashboard(strategy_id, strategy_config, session_state, config)
# Complete lending dashboard with health factor, LTV, collateral breakdown.
""", language="python")
    
    config = get_aave_v3_config(
        collateral_token="WETH",
        borrow_token="USDC",
        chain="arbitrum",
    )
    
    strategy_config = {
        "protocol": "aave_v3",
        "collateral_token": "WETH",
        "borrow_token": "USDC",
        "chain": "arbitrum",
    }
    
    session_state = {
        "health_factor": st.session_state.lending_health_factor,
        "ltv": st.session_state.lending_ltv,
        "collateral_assets": st.session_state.lending_collateral_assets,
        "collateral_amount": Decimal("10.0"),
        "collateral_value_usd": Decimal("34000"),
        "borrowed_amount": Decimal("20000"),
        "borrowed_value_usd": Decimal("20000"),
        "supply_apy": Decimal("0.03"),
        "borrow_apy": Decimal("0.05"),
    }
    
    render_lending_dashboard(
        strategy_id="demo_lending_strategy",
        strategy_config=strategy_config,
        session_state=session_state,
        config=config,
    )

elif page == "⚡ Perp Dashboard":
    st.header("⚡ Perp Dashboard Template")
    st.info("✨ This entire dashboard is rendered by calling `render_perp_dashboard()` - no custom Streamlit code!")
    
    with st.expander("📝 See the code that renders this dashboard"):
        st.code("""
from almanak.framework.dashboard.templates import (
    PerpDashboardConfig,
    render_perp_dashboard,
)

config = PerpDashboardConfig(protocol="gmx_v2", market="ETH/USD")
render_perp_dashboard(strategy_id, strategy_config, session_state, config)
# Complete perp dashboard with position, funding rates, liquidation levels.
""", language="python")
    
    config = PerpDashboardConfig(
        protocol="gmx_v2",
        market="ETH/USD",
        collateral_token="WETH",
        chain="arbitrum",
    )
    
    strategy_config = {
        "protocol": "gmx_v2",
        "market": "ETH/USD",
        "chain": "arbitrum",
    }
    
    session_state = {
        "has_position": True,
        "is_long": st.session_state.perp_is_long,
        "entry_price": st.session_state.perp_entry_price,
        "current_price": st.session_state.perp_current_price,
        "liquidation_price": st.session_state.perp_liquidation_price,
        "size_usd": st.session_state.perp_size_usd,
        "leverage": st.session_state.perp_leverage,
        "collateral_usd": st.session_state.perp_collateral_usd,
        "unrealized_pnl": st.session_state.perp_unrealized_pnl,
        "funding_history": [
            (datetime.now(UTC) - timedelta(hours=i * 8), 0.0001 * (i % 10 - 5) / 100)
            for i in range(21)
        ],
    }
    
    render_perp_dashboard(
        strategy_id="demo_perp_strategy",
        strategy_config=strategy_config,
        session_state=session_state,
        config=config,
    )

elif page == "🔮 Prediction Dashboard":
    st.header("🔮 Prediction Dashboard Template")
    st.info("✨ This entire dashboard is rendered by calling `render_prediction_dashboard()` - no custom Streamlit code!")
    
    with st.expander("📝 See the code that renders this dashboard"):
        st.code("""
from almanak.framework.dashboard.templates import (
    PredictionDashboardConfig,
    render_prediction_dashboard,
)

config = PredictionDashboardConfig(protocol="polymarket")
render_prediction_dashboard(strategy_id, strategy_config, session_state, config)
# Complete prediction dashboard with positions, probabilities, arbitrage analysis.
""", language="python")
    
    config = PredictionDashboardConfig(
        protocol="polymarket",
        chain="polygon",
    )
    
    strategy_config = {
        "protocol": "polymarket",
        "chain": "polygon",
    }
    
    session_state = {
        "market_question": st.session_state.prediction_market_question,
        "market_id": "demo_market_001",
        "yes_shares": st.session_state.prediction_yes_shares,
        "no_shares": st.session_state.prediction_no_shares,
        "yes_price": st.session_state.prediction_yes_price,
        "no_price": st.session_state.prediction_no_price,
        "cost_basis": st.session_state.prediction_cost_basis,
        "probability_history": [
            (datetime.now(UTC) - timedelta(days=30 - i), 0.4 + i * 0.01, 0.6 - i * 0.01)
            for i in range(30)
        ],
        "tracked_markets": [
            {"question": "ETH > $5000?", "yes_price": 0.35, "position": "YES"},
            {"question": "BTC > $100K?", "yes_price": 0.60, "position": "NO"},
        ],
    }
    
    render_prediction_dashboard(
        strategy_id="demo_prediction_strategy",
        strategy_config=strategy_config,
        session_state=session_state,
        config=config,
    )

elif page == "💼 Portfolio Plots":
    st.header("💼 Portfolio Plots")
    st.info("✨ These plots use SDK plot functions directly - still no custom Streamlit plot code!")
    st.markdown("""
    **Note**: These are standalone plot functions. For full dashboards, use the templates above.
    """)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Portfolio Value Over Time")
        value_history = [
            (datetime.now(UTC) - timedelta(days=30 - i), 10000 + i * 50 + (i % 10) * 20)
            for i in range(30)
        ]
        fig = plot_portfolio_value_over_time(
            value_data=value_history,
            show_drawdown=True,
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("PnL Waterfall")
        fig = plot_pnl_waterfall(
            pnl_components={
                "Trading PnL": 5000,
                "Fees Earned": 2000,
                "Gas Costs": -500,
                "IL": -1200,
            },
            title="Strategy PnL Breakdown",
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Asset Allocation")
    fig = plot_asset_allocation(
        assets={"ETH": 45, "BTC": 30, "USDC": 25},
        show_percentages=True,
    )
    st.plotly_chart(fig, use_container_width=True)

# =============================================================================
# FOOTER
# =============================================================================

st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: #666;'>
    <small>Almanak SDK Dashboard Showcase | 
    <a href='https://github.com/almanak/sdk' target='_blank'>Documentation</a></small>
    </div>
    """,
    unsafe_allow_html=True,
)
