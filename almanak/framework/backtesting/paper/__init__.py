"""Paper Trading Module.

This module provides real-time paper trading capabilities using Anvil forks.
Paper trading executes actual transactions on a local fork of mainnet,
allowing strategies to be validated with real DeFi protocol interactions
before deployment with real capital.

Key Components:
    - PaperTrader: Main paper trading engine orchestrating fork-based execution
    - RollingForkManager: Manages Anvil fork lifecycle and wallet funding
    - PaperPortfolioTracker: Tracks paper trading portfolio state
    - PaperTraderConfig: Configuration for paper trading sessions
    - PaperTrade: Record of a paper trade execution
    - PaperTradingSummary: Summary of paper trading session

Usage:
    from almanak.framework.backtesting.paper import (
        PaperTrader,
        PaperTraderConfig,
        RollingForkManager,
        PaperPortfolioTracker,
    )

    # Create components
    fork_manager = RollingForkManager(
        rpc_url="https://arb1.arbitrum.io/rpc",
        chain="arbitrum",
    )

    portfolio_tracker = PaperPortfolioTracker(
        initial_balances={"USDC": Decimal("10000")},
        initial_capital_usd=Decimal("10000"),
    )

    # Create paper trader
    trader = PaperTrader(
        fork_manager=fork_manager,
        portfolio_tracker=portfolio_tracker,
        config=PaperTraderConfig(tick_interval_seconds=60),
    )

    # Run paper trading session
    result = await trader.run(my_strategy, duration_seconds=3600)
    print(f"PnL: ${result.metrics.net_pnl_usd}")

    # Or start/stop manually
    await trader.start(my_strategy)
    # ... later ...
    await trader.stop()
"""

from almanak.framework.backtesting.paper.background import (
    BackgroundPaperTrader,
    BackgroundStatus,
    PaperTraderState,
    PIDFile,
    TradeHistoryWriter,
)
from almanak.framework.backtesting.paper.config import PaperTraderConfig
from almanak.framework.backtesting.paper.engine import (
    ERC20_DECIMALS_CALL_TIMEOUT,
    NATIVE_ETH_ADDRESS,
    TOKEN_DECIMALS,
    PaperTradeableStrategy,
    PaperTradeEventCallback,
    PaperTradeEventType,
    PaperTrader,
    create_market_snapshot_from_fork,
    get_token_decimals,
    get_token_decimals_with_fallback,
)
from almanak.framework.backtesting.paper.fork_manager import RollingForkManager
from almanak.framework.backtesting.paper.models import (
    PaperTrade,
    PaperTradeError,
    PaperTradeErrorType,
    PaperTradingSummary,
)
from almanak.framework.backtesting.paper.portfolio_tracker import PaperPortfolioTracker
from almanak.framework.backtesting.paper.position_queries import (
    # Aave V3
    AAVE_V3_POOL_DATA_PROVIDER,
    AAVE_V3_TOKEN_DECIMALS,
    AAVE_V3_TOKENS,
    # GMX V2
    GMX_V2_COLLATERAL_TOKENS,
    GMX_V2_DATA_STORE,
    GMX_V2_MARKETS,
    GMX_V2_READER,
    # Uniswap V3
    UNISWAP_V3_POSITION_MANAGER,
    AaveV3LendingPosition,
    GMXv2Position,
    UniswapV3Position,
    query_aave_positions,
    query_aave_positions_sync,
    query_gmx_positions,
    query_gmx_positions_sync,
    query_uniswap_v3_positions,
    query_uniswap_v3_positions_sync,
)
from almanak.framework.backtesting.paper.position_reconciler import (
    DiscrepancyType,
    PositionDiscrepancy,
    PositionReconciler,
    PositionType,
    TrackedPosition,
    compare_positions,
)

__all__ = [
    # Background process management
    "BackgroundPaperTrader",
    "BackgroundStatus",
    "PaperTraderState",
    "PIDFile",
    "TradeHistoryWriter",
    # Main engine
    "PaperTrader",
    "PaperTradeableStrategy",
    "PaperTradeEventType",
    "PaperTradeEventCallback",
    "create_market_snapshot_from_fork",
    # Token decimals
    "TOKEN_DECIMALS",
    "NATIVE_ETH_ADDRESS",
    "ERC20_DECIMALS_CALL_TIMEOUT",
    "get_token_decimals",
    "get_token_decimals_with_fallback",
    # Configuration
    "PaperTraderConfig",
    # Fork management
    "RollingForkManager",
    # Portfolio tracking
    "PaperPortfolioTracker",
    # Trade models
    "PaperTrade",
    "PaperTradeError",
    "PaperTradeErrorType",
    "PaperTradingSummary",
    # Position queries - Uniswap V3
    "UniswapV3Position",
    "query_uniswap_v3_positions",
    "query_uniswap_v3_positions_sync",
    "UNISWAP_V3_POSITION_MANAGER",
    # Position queries - GMX V2
    "GMXv2Position",
    "query_gmx_positions",
    "query_gmx_positions_sync",
    "GMX_V2_READER",
    "GMX_V2_DATA_STORE",
    "GMX_V2_MARKETS",
    "GMX_V2_COLLATERAL_TOKENS",
    # Position queries - Aave V3
    "AaveV3LendingPosition",
    "query_aave_positions",
    "query_aave_positions_sync",
    "AAVE_V3_POOL_DATA_PROVIDER",
    "AAVE_V3_TOKENS",
    "AAVE_V3_TOKEN_DECIMALS",
    # Position reconciliation
    "PositionReconciler",
    "TrackedPosition",
    "PositionDiscrepancy",
    "PositionType",
    "DiscrepancyType",
    "compare_positions",
]
