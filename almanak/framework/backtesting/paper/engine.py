"""PaperTrader Engine - Fork-based execution simulation for strategy testing.

This module provides the main PaperTrader engine that orchestrates paper trading
execution using Anvil forks. Unlike PnL backtesting which uses historical data,
PaperTrader executes real transactions on local Anvil forks for accurate simulation.

The PaperTrader:
1. Manages rolling Anvil forks via RollingForkManager
2. Tracks portfolio state via PaperPortfolioTracker
3. Executes intents on the fork using the real execution pipeline
4. Records trades as PaperTrade objects with receipts
5. Calculates comprehensive backtest metrics

Key Differences from PnL Backtesting:
    - Executes REAL transactions on Anvil forks (not simulated)
    - Uses actual protocol contracts and pricing
    - Captures real gas costs, slippage, and fees
    - Supports accurate testing of complex DeFi interactions
    - Can test with live market state (recent fork block)

Examples:
    Basic usage for strategy testing:

        from almanak.framework.backtesting.paper import PaperTrader, PaperTraderConfig
        from almanak.framework.anvil.fork_manager import RollingForkManager
        from almanak.framework.backtesting.paper.portfolio_tracker import PaperPortfolioTracker

        # Create components
        fork_manager = RollingForkManager(rpc_url="https://arb-mainnet.g.alchemy.com/v2/...")
        portfolio_tracker = PaperPortfolioTracker(
            initial_balances={"USDC": Decimal("10000")},
            initial_capital_usd=Decimal("10000"),
        )

        # Create paper trader
        trader = PaperTrader(
            fork_manager=fork_manager,
            portfolio_tracker=portfolio_tracker,
            config=PaperTraderConfig(
                chain="arbitrum",
                rpc_url="https://arb-mainnet.g.alchemy.com/v2/...",
                strategy_id="my_strategy",
            ),
        )

        # Run paper trading session
        result = await trader.run(strategy, duration_seconds=3600)
        print(result.summary())

    Strict mode for accurate valuations (no hardcoded fallback prices):

        config = PaperTraderConfig(
            chain="arbitrum",
            rpc_url="https://arb-mainnet.g.alchemy.com/v2/...",
            strategy_id="production_ready",
            price_source="auto",  # Chainlink -> TWAP -> CoinGecko fallback
            strict_price_mode=True,  # Fail if no real price available
        )
        trader = PaperTrader(
            fork_manager=fork_manager,
            portfolio_tracker=portfolio_tracker,
            config=config,
        )
"""

import asyncio
import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from almanak.framework.data.indicators.rsi import RSICalculator

from almanak.framework.anvil.fork_manager import RollingForkManager
from almanak.framework.backtesting.models import (
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
    EquityPoint,
    IntentType,
    TradeRecord,
)
from almanak.framework.backtesting.paper.config import PaperTraderConfig
from almanak.framework.backtesting.paper.models import (
    PaperTrade,
    PaperTradeError,
    PaperTradeErrorType,
    PaperTradingSummary,
)
from almanak.framework.backtesting.paper.portfolio_tracker import PaperPortfolioTracker
from almanak.framework.backtesting.paper.token_registry import (
    get_token_symbol_with_fallback,
)
from almanak.framework.backtesting.pnl.error_handling import (
    BacktestErrorConfig,
    BacktestErrorHandler,
)
from almanak.framework.backtesting.pnl.providers.chainlink import (
    ChainlinkDataProvider,
    ChainlinkStaleDataError,
)
from almanak.framework.backtesting.pnl.receipt_utils import (
    extract_token_flows as extract_receipt_token_flows,
)
from almanak.framework.data.interfaces import AllDataSourcesFailed
from almanak.framework.data.market_snapshot import MarketSnapshot
from almanak.framework.data.price.dex_twap import DEXTWAPDataProvider, LowLiquidityWarning
from almanak.framework.execution.orchestrator import (
    ExecutionContext,
    ExecutionOrchestrator,
    ExecutionResult,
)
from almanak.framework.models.reproduction_bundle import ActionBundle, TransactionReceipt
from almanak.gateway.data.price import CoinGeckoPriceSource, PriceAggregator

logger = logging.getLogger(__name__)


# =============================================================================
# Token Decimal Registry
# =============================================================================

# Chain IDs (EIP-155)
CHAIN_ID_ETHEREUM = 1
CHAIN_ID_ARBITRUM = 42161
CHAIN_ID_BASE = 8453

# Reverse mapping from chain_id (int) -> chain name (str) for TokenResolver calls
_CHAIN_ID_TO_NAME: dict[int, str] = {
    1: "ethereum",
    42161: "arbitrum",
    10: "optimism",
    8453: "base",
    43114: "avalanche",
    137: "polygon",
    56: "bsc",
    146: "sonic",
    9745: "plasma",
    81457: "blast",
    5000: "mantle",
    80094: "berachain",
}


def _get_resolver():
    """Lazy import and return the TokenResolver singleton.

    Uses lazy import to avoid circular dependencies and import-time overhead.
    Returns None if TokenResolver is not available.
    """
    try:
        from almanak.framework.data.tokens.resolver import get_token_resolver

        return get_token_resolver()
    except Exception:
        logger.debug("TokenResolver not available, using local TOKEN_DECIMALS only")
        return None


# Token decimals registry: (chain_id, lowercase_address) -> decimals
# Addresses are stored lowercase for case-insensitive matching
TOKEN_DECIMALS: dict[tuple[int, str], int] = {
    # =========================================================================
    # Native ETH (sentinel address)
    # =========================================================================
    # Ethereum
    (CHAIN_ID_ETHEREUM, "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"): 18,
    # Arbitrum
    (CHAIN_ID_ARBITRUM, "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"): 18,
    # Base
    (CHAIN_ID_BASE, "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"): 18,
    # =========================================================================
    # WETH (18 decimals)
    # =========================================================================
    # Ethereum
    (CHAIN_ID_ETHEREUM, "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"): 18,
    # Arbitrum
    (CHAIN_ID_ARBITRUM, "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"): 18,
    # Base
    (CHAIN_ID_BASE, "0x4200000000000000000000000000000000000006"): 18,
    # =========================================================================
    # USDC (6 decimals)
    # =========================================================================
    # Ethereum
    (CHAIN_ID_ETHEREUM, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"): 6,
    # Arbitrum (native USDC)
    (CHAIN_ID_ARBITRUM, "0xaf88d065e77c8cc2239327c5edb3a432268e5831"): 6,
    # Arbitrum (bridged USDC.e)
    (CHAIN_ID_ARBITRUM, "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8"): 6,
    # Base
    (CHAIN_ID_BASE, "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913"): 6,
    # =========================================================================
    # USDT (6 decimals)
    # =========================================================================
    # Ethereum
    (CHAIN_ID_ETHEREUM, "0xdac17f958d2ee523a2206206994597c13d831ec7"): 6,
    # Arbitrum
    (CHAIN_ID_ARBITRUM, "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9"): 6,
    # =========================================================================
    # DAI (18 decimals)
    # =========================================================================
    # Ethereum
    (CHAIN_ID_ETHEREUM, "0x6b175474e89094c44da98b954eedeac495271d0f"): 18,
    # Arbitrum
    (CHAIN_ID_ARBITRUM, "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1"): 18,
    # Base
    (CHAIN_ID_BASE, "0x50c5725949a6f0c72e6c4a641f24049a917db0cb"): 18,
    # =========================================================================
    # WBTC (8 decimals)
    # =========================================================================
    # Ethereum
    (CHAIN_ID_ETHEREUM, "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"): 8,
    # Arbitrum
    (CHAIN_ID_ARBITRUM, "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f"): 8,
    # =========================================================================
    # ARB (18 decimals)
    # =========================================================================
    # Ethereum
    (CHAIN_ID_ETHEREUM, "0xb50721bcf8d664c30412cfbc6cf7a15145234ad1"): 18,
    # Arbitrum
    (CHAIN_ID_ARBITRUM, "0x912ce59144191c1204e64559fe8253a0e49e6548"): 18,
    # =========================================================================
    # LINK (18 decimals)
    # =========================================================================
    # Ethereum
    (CHAIN_ID_ETHEREUM, "0x514910771af9ca656af840dff83e8264ecf986ca"): 18,
    # Arbitrum
    (CHAIN_ID_ARBITRUM, "0xf97f4df75117a78c1a5a0dbb814af92458539fb4"): 18,
    # Base
    (CHAIN_ID_BASE, "0x88fb150bdc53a65fe94dea0c9ba0a6daf8c6e196"): 18,
    # =========================================================================
    # UNI (18 decimals)
    # =========================================================================
    # Ethereum
    (CHAIN_ID_ETHEREUM, "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"): 18,
    # Arbitrum
    (CHAIN_ID_ARBITRUM, "0xfa7f8980b0f1e64a2062791cc3b0871572f1f7f0"): 18,
    # Base
    (CHAIN_ID_BASE, "0xc3de830ea07524a0761646a6a4e4be0e114a3c83"): 18,
    # =========================================================================
    # AAVE (18 decimals)
    # =========================================================================
    # Ethereum
    (CHAIN_ID_ETHEREUM, "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9"): 18,
    # Arbitrum
    (CHAIN_ID_ARBITRUM, "0xba5ddd1f9d7f570dc94a51479a000e3bce967196"): 18,
    # Base
    (CHAIN_ID_BASE, "0x18c11fd286c5ec11c3b683caa813b77f5163a122"): 18,
    # =========================================================================
    # GMX (18 decimals) - Arbitrum only
    # =========================================================================
    (CHAIN_ID_ARBITRUM, "0xfc5a1a6eb076a2c7ad06ed22c90d7e710e35ad0a"): 18,
    # =========================================================================
    # CRV (18 decimals)
    # =========================================================================
    # Ethereum
    (CHAIN_ID_ETHEREUM, "0xd533a949740bb3306d119cc777fa900ba034cd52"): 18,
    # Arbitrum
    (CHAIN_ID_ARBITRUM, "0x11cdb42b0eb46d95f990bedd4695a6e3fa034978"): 18,
    # Base
    (CHAIN_ID_BASE, "0x8ee73c484a26e0a5df2ee2a4960b789967dd0415"): 18,
}


def get_token_decimals(chain_id: int, token_address: str) -> int | None:
    """Get the number of decimals for a token on a specific chain.

    Delegates to TokenResolver for unified resolution, falls back to
    the local TOKEN_DECIMALS registry if resolver is unavailable.
    Returns None if the token is not found.

    For dynamic lookup with ERC20 fallback, use get_token_decimals_with_fallback().

    Args:
        chain_id: EIP-155 chain ID (e.g., 1 for Ethereum, 42161 for Arbitrum, 8453 for Base)
        token_address: Token contract address (case-insensitive)

    Returns:
        Number of decimals for the token, or None if not in registry

    Example:
        >>> get_token_decimals(1, "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
        6  # USDC on Ethereum
        >>> get_token_decimals(42161, "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1")
        18  # WETH on Arbitrum
        >>> get_token_decimals(1, "0x1234...")
        None  # Unknown token
    """
    normalized_address = token_address.lower()

    # Try TokenResolver first
    chain_name = _CHAIN_ID_TO_NAME.get(chain_id)
    if chain_name:
        resolver = _get_resolver()
        if resolver:
            try:
                return resolver.get_decimals(chain_name, normalized_address)
            except Exception:
                pass  # Fall through to local registry

    # Fallback to local TOKEN_DECIMALS
    return TOKEN_DECIMALS.get((chain_id, normalized_address))


# Native ETH sentinel address (used in ERC-4626, Uniswap, etc.)
NATIVE_ETH_ADDRESS = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

# ERC20 decimals() function selector: keccak256("decimals()")[0:4]
ERC20_DECIMALS_SELECTOR = "0x313ce567"

# Default timeout for ERC20 decimals() calls (seconds)
ERC20_DECIMALS_CALL_TIMEOUT = 2.0


async def _fetch_erc20_decimals(rpc_url: str, token_address: str) -> int | None:
    """Fetch decimals from an ERC20 token contract via eth_call.

    Makes a JSON-RPC eth_call to the token's decimals() function.

    Args:
        rpc_url: RPC endpoint URL (e.g., Anvil fork URL)
        token_address: ERC20 token contract address

    Returns:
        Number of decimals (0-255), or None if call fails

    Note:
        Uses 2-second timeout to avoid blocking on unresponsive RPCs.
    """
    import aiohttp

    payload = {
        "jsonrpc": "2.0",
        "method": "eth_call",
        "params": [
            {"to": token_address, "data": ERC20_DECIMALS_SELECTOR},
            "latest",
        ],
        "id": 1,
    }

    try:
        timeout = aiohttp.ClientTimeout(total=ERC20_DECIMALS_CALL_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(rpc_url, json=payload) as response:
                result_data: dict[str, Any] = await response.json()

                if "error" in result_data:
                    logger.debug(f"ERC20 decimals() call failed for {token_address}: {result_data['error']}")
                    return None

                result = result_data.get("result")
                if result and result != "0x":
                    # decimals() returns uint8, parse as hex
                    decimals = int(result, 16)
                    # Validate: ERC20 decimals are typically 0-18, max 255
                    if 0 <= decimals <= 255:
                        return decimals
                    else:
                        logger.warning(f"Invalid decimals value {decimals} for {token_address}")
                        return None

                return None

    except TimeoutError:
        logger.warning(f"Timeout fetching decimals for {token_address} (>{ERC20_DECIMALS_CALL_TIMEOUT}s)")
        return None
    except Exception as e:
        logger.debug(f"Error fetching decimals for {token_address}: {e}")
        return None


async def get_token_decimals_with_fallback(
    chain_id: int,
    token_address: str,
    rpc_url: str | None = None,
) -> int:
    """Get token decimals with TokenResolver and ERC20 fallback for unknown tokens.

    Resolution order:
    1. TokenResolver (unified cache/registry/gateway resolution)
    2. Local TOKEN_DECIMALS registry (fallback)
    3. ERC20 decimals() on-chain query (requires RPC URL)
    4. Default to 18 with warning (last resort)

    Handles native ETH (sentinel address 0xeee...eee) which always has 18 decimals.

    Args:
        chain_id: EIP-155 chain ID (e.g., 1 for Ethereum, 42161 for Arbitrum)
        token_address: Token contract address (case-insensitive)
        rpc_url: RPC endpoint for ERC20 fallback queries (e.g., Anvil fork URL)

    Returns:
        Number of decimals for the token. Returns 18 as default if:
        - Token is native ETH
        - RPC URL not provided and token not in registry
        - ERC20 call fails

    Example:
        >>> await get_token_decimals_with_fallback(42161, "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "http://localhost:8545")
        6  # USDC on Arbitrum (from registry)

        >>> await get_token_decimals_with_fallback(42161, "0xNEWTOKEN...", "http://localhost:8545")
        8  # Fetched via ERC20 call and cached
    """
    normalized_address = token_address.lower()

    # 1. Check if it's native ETH (sentinel address)
    if normalized_address == NATIVE_ETH_ADDRESS:
        return 18

    # 2. Try TokenResolver first (unified resolution)
    chain_name = _CHAIN_ID_TO_NAME.get(chain_id)
    if chain_name:
        resolver = _get_resolver()
        if resolver:
            try:
                return resolver.get_decimals(chain_name, normalized_address)
            except Exception:
                pass  # Fall through to local registry and ERC20 fallback

    # 3. Check local TOKEN_DECIMALS registry
    registry_result = TOKEN_DECIMALS.get((chain_id, normalized_address))
    if registry_result is not None:
        return registry_result

    # 4. If no RPC URL provided, default to 18 with warning
    if rpc_url is None:
        logger.warning(
            f"Token {token_address[:10]}... not in registry and no RPC URL provided. Defaulting to 18 decimals."
        )
        return 18

    # 5. Query ERC20 decimals() with timeout
    logger.debug(f"Querying ERC20 decimals() for unknown token {token_address[:10]}...")
    decimals = await _fetch_erc20_decimals(rpc_url, token_address)

    if decimals is not None:
        # Cache result in local registry for future lookups
        TOKEN_DECIMALS[(chain_id, normalized_address)] = decimals
        logger.info(f"Cached decimals for {token_address[:10]}... on chain {chain_id}: {decimals}")
        return decimals

    # 6. Fallback: default to 18 with warning
    logger.warning(f"Could not fetch decimals for {token_address[:10]}... Defaulting to 18 decimals.")
    return 18


# =============================================================================
# Protocol for Paper-Tradeable Strategies
# =============================================================================


@runtime_checkable
class PaperTradeableStrategy(Protocol):
    """Protocol defining the interface for strategies that can be paper traded.

    Strategies must implement:
    - strategy_id: Unique identifier for the strategy
    - decide(market): Method that returns an intent based on market data
    - compile_intent(intent): Method to compile intent to ActionBundle

    The decide method can return:
    - An Intent object (SwapIntent, LPIntent, etc.)
    - None (equivalent to HOLD)
    - A DecideResult (for IntentStrategy compatibility)
    """

    @property
    def strategy_id(self) -> str:
        """Return the unique identifier for this strategy."""
        ...

    def decide(self, market: MarketSnapshot) -> Any:
        """Make a trading decision based on current market state.

        Args:
            market: MarketSnapshot containing current prices, balances, indicators

        Returns:
            An Intent object, None (hold), or DecideResult
        """
        ...


# =============================================================================
# Event Types for Paper Trading
# =============================================================================


class PaperTradeEventType:
    """Event types emitted during paper trading."""

    SESSION_STARTED = "session_started"
    SESSION_ENDED = "session_ended"
    TICK_STARTED = "tick_started"
    TICK_ENDED = "tick_ended"
    FORK_REFRESHED = "fork_refreshed"
    INTENT_DECIDED = "intent_decided"
    TRADE_EXECUTING = "trade_executing"
    TRADE_COMPLETED = "trade_completed"
    TRADE_FAILED = "trade_failed"
    PORTFOLIO_UPDATED = "portfolio_updated"
    ERROR = "error"


# Event callback type
PaperTradeEventCallback = Callable[[str, dict[str, Any]], None]


# =============================================================================
# Market Snapshot Factory for Fork State
# =============================================================================


async def create_market_snapshot_from_fork(
    fork_manager: RollingForkManager,
    chain: str,
    wallet_address: str,
    portfolio_tracker: PaperPortfolioTracker | None = None,
    token_prices: dict[str, Decimal] | None = None,
    price_oracle: Any | None = None,
    rsi_calculator: "RSICalculator | None" = None,
) -> MarketSnapshot:
    """Create a MarketSnapshot from the current fork state.

    This function bridges the fork's on-chain state to the MarketSnapshot
    format expected by strategies' decide() methods.

    Args:
        fork_manager: RollingForkManager with active fork
        chain: Chain identifier (e.g., "arbitrum")
        wallet_address: Wallet address for balance queries
        portfolio_tracker: Optional portfolio tracker for balance data
        token_prices: Optional dict of token symbol to USD price for valuation
        price_oracle: Optional PriceOracle/PriceAggregator for market.price() calls
        rsi_calculator: Optional RSICalculator for indicator support (RSI, MACD, BB, ATR)

    Returns:
        MarketSnapshot populated with fork-based data
    """
    # Create snapshot with current timestamp
    snapshot = MarketSnapshot(
        chain=chain,
        wallet_address=wallet_address,
        timestamp=datetime.now(UTC),
        price_oracle=price_oracle,
        rsi_calculator=rsi_calculator,
    )

    # Add metadata about fork state
    if fork_manager.is_running:
        snapshot._fork_block = fork_manager.current_block  # type: ignore[attr-defined]
        snapshot._fork_rpc_url = fork_manager.get_rpc_url()  # type: ignore[attr-defined]

    # If we have a portfolio tracker, use its balances
    if portfolio_tracker:
        # Stablecoins known to be $1
        stables = {"USDC", "USDT", "DAI", "FRAX", "LUSD", "BUSD", "USD", "USDC.E"}

        for token, amount in portfolio_tracker.current_balances.items():
            # Get price from provided prices dict, or use fallback logic
            token_upper = token.upper()
            if token_prices and token_upper in token_prices:
                price = token_prices[token_upper]
            elif token_upper in stables:
                price = Decimal("1")
            else:
                # Fallback for unknown tokens without price data
                price = Decimal("1")

            balance_usd = amount * price

            # Create a simple dict for balance data (TokenBalance may not exist)
            balance_data = {
                "symbol": token,
                "balance": amount,
                "balance_usd": balance_usd,
            }
            # Store as private attribute since set_balance may not exist
            if not hasattr(snapshot, "_balances"):
                snapshot._balances = {}  # type: ignore[attr-defined]
            snapshot._balances[token] = balance_data  # type: ignore[attr-defined]

    return snapshot


# =============================================================================
# PaperTrader Engine
# =============================================================================


@dataclass
class PaperTrader:
    """Main paper trading engine for fork-based strategy simulation.

    The PaperTrader executes strategy decisions on local Anvil forks,
    providing accurate simulation of real DeFi execution. It:

    1. Manages fork lifecycle via RollingForkManager
    2. Calls strategy.decide() at configured intervals
    3. Compiles intents to ActionBundles
    4. Executes transactions on the fork via ExecutionOrchestrator
    5. Tracks portfolio state and records trades
    6. Calculates comprehensive performance metrics

    Attributes:
        fork_manager: RollingForkManager for Anvil fork lifecycle
        portfolio_tracker: PaperPortfolioTracker for state tracking
        config: PaperTraderConfig with execution parameters
        event_callback: Optional callback for trading events

    Example:
        trader = PaperTrader(
            fork_manager=fork_manager,
            portfolio_tracker=portfolio_tracker,
            config=PaperTraderConfig(tick_interval_seconds=60),
        )

        # Run for 1 hour
        result = await trader.run(my_strategy, duration_seconds=3600)

        # Or run indefinitely until stopped
        await trader.start(my_strategy)
        # ... later ...
        await trader.stop()
    """

    fork_manager: RollingForkManager
    portfolio_tracker: PaperPortfolioTracker
    config: PaperTraderConfig
    event_callback: PaperTradeEventCallback | None = None

    # Internal state
    _running: bool = field(default=False, init=False, repr=False)
    _current_strategy: PaperTradeableStrategy | None = field(default=None, init=False, repr=False)
    _orchestrator: ExecutionOrchestrator | None = field(default=None, init=False, repr=False)
    _trades: list[PaperTrade] = field(default_factory=list, init=False, repr=False)
    _errors: list[PaperTradeError] = field(default_factory=list, init=False, repr=False)
    _equity_curve: list[EquityPoint] = field(default_factory=list, init=False, repr=False)
    _session_start: datetime | None = field(default=None, init=False, repr=False)
    _tick_count: int = field(default=0, init=False, repr=False)
    _price_aggregator: PriceAggregator | None = field(default=None, init=False, repr=False)
    _price_cache: dict[str, Decimal] = field(default_factory=dict, init=False, repr=False)
    _price_source_order: list[str] = field(default_factory=list, init=False, repr=False)
    _backtest_id: str | None = field(default=None, init=False, repr=False)
    _chainlink_provider: ChainlinkDataProvider | None = field(default=None, init=False, repr=False)
    _twap_provider: DEXTWAPDataProvider | None = field(default=None, init=False, repr=False)
    _error_handler: BacktestErrorHandler | None = field(default=None, init=False, repr=False)
    _used_hardcoded_fallback: bool = field(default=False, init=False, repr=False)
    _fallback_usage: dict[str, int] = field(default_factory=dict, init=False, repr=False)
    _rsi_calculator: "RSICalculator | None" = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if self.config.tick_interval_seconds <= 0:
            raise ValueError("tick_interval_seconds must be positive")

        # Initialize fallback usage tracking
        self._fallback_usage = {
            "hardcoded_price": 0,
            "default_gas_price": 0,
            "default_usd_amount": 0,
        }

        # Initialize price provider based on config
        self._init_price_provider()

        # Initialize indicator calculators (RSI, MACD, BB, ATR all derive from OHLCV)
        self._init_indicator_calculators()

    def _track_fallback(self, fallback_type: str) -> None:
        """Track usage of a fallback value.

        Args:
            fallback_type: Type of fallback used (e.g., "hardcoded_price")
        """
        if fallback_type in self._fallback_usage:
            self._fallback_usage[fallback_type] += 1
        else:
            self._fallback_usage[fallback_type] = 1

    async def run(
        self,
        strategy: PaperTradeableStrategy,
        duration_seconds: float | None = None,
        max_ticks: int | None = None,
    ) -> BacktestResult:
        """Run a paper trading session for the specified duration.

        This is the main entry point for paper trading. It:
        1. Initializes the fork and orchestrator
        2. Runs the trading loop for the specified duration
        3. Calculates and returns metrics

        Args:
            strategy: Strategy to paper trade
            duration_seconds: Maximum duration in seconds (None = config default)
            max_ticks: Maximum number of ticks (None = no limit)

        Returns:
            BacktestResult with comprehensive metrics and trades

        Raises:
            RuntimeError: If already running
        """
        if self._running:
            raise RuntimeError("PaperTrader is already running")

        run_started_at = datetime.now(UTC)
        self._session_start = run_started_at
        self._running = True
        self._current_strategy = strategy
        self._trades = []
        self._errors = []
        self._equity_curve = []
        self._tick_count = 0

        # Generate unique backtest_id for correlation across all log messages
        self._backtest_id = str(uuid.uuid4())

        # Initialize error handler for consistent error handling
        self._error_handler = BacktestErrorHandler(BacktestErrorConfig())

        # Determine effective duration (max_duration_seconds may be None for indefinite)
        effective_duration: float = (
            duration_seconds
            if duration_seconds is not None
            else (
                self.config.max_duration_seconds or 3600.0  # Default to 1 hour if not specified
            )
        )

        logger.info(
            f"[{self._backtest_id}] Starting paper trading session for {strategy.strategy_id} "
            f"(duration={effective_duration}s, interval={self.config.tick_interval_seconds}s)"
        )

        self._emit_event(
            PaperTradeEventType.SESSION_STARTED,
            {
                "strategy_id": strategy.strategy_id,
                "duration_seconds": effective_duration,
                "tick_interval_seconds": self.config.tick_interval_seconds,
            },
        )

        error: str | None = None

        try:
            # Initialize fork
            await self._initialize_fork()

            # Initialize orchestrator with fork RPC
            await self._initialize_orchestrator()

            # Record initial equity point
            await self._record_equity_point()

            # Run trading loop
            end_time = run_started_at + timedelta(seconds=effective_duration)

            while self._running:
                # Check time limit
                now = datetime.now(UTC)
                if now >= end_time:
                    logger.info(f"[{self._backtest_id}] Duration limit reached, stopping")
                    break

                # Check tick limit
                if max_ticks is not None and self._tick_count >= max_ticks:
                    logger.info(f"[{self._backtest_id}] Max ticks ({max_ticks}) reached, stopping")
                    break

                # Execute tick
                await self._execute_tick(strategy)
                self._tick_count += 1

                # Sleep until next tick
                await asyncio.sleep(self.config.tick_interval_seconds)

                # Check if fork needs refresh
                if await self._should_refresh_fork():
                    await self._refresh_fork()

        except asyncio.CancelledError:
            logger.info(f"[{self._backtest_id}] Paper trading session cancelled")
            error = "Session cancelled"
        except Exception as e:
            # Use error handler for consistent classification
            if self._error_handler:
                handler_result = self._error_handler.handle_error(e, context="paper_trading_session")
                if handler_result.should_stop:
                    logger.error(f"[{self._backtest_id}] Fatal error in paper trading session: {e}")
                else:
                    logger.warning(f"[{self._backtest_id}] Non-critical error in paper trading session: {e}")
            else:
                logger.exception(f"[{self._backtest_id}] Paper trading session failed: {e}")
            error = str(e)
        finally:
            self._running = False
            self._current_strategy = None

            # Cleanup
            await self._cleanup()

        run_ended_at = datetime.now(UTC)

        self._emit_event(
            PaperTradeEventType.SESSION_ENDED,
            {
                "strategy_id": strategy.strategy_id,
                "tick_count": self._tick_count,
                "trade_count": len(self._trades),
                "error": error,
            },
        )

        # Calculate metrics
        metrics = self._calculate_metrics()

        # Convert PaperTrades to TradeRecords for BacktestResult
        trade_records: list[TradeRecord] = []
        for trade in self._trades:
            # Build tokens list from token flows
            tokens = list(trade.tokens_in.keys()) + list(trade.tokens_out.keys())

            record = TradeRecord(
                timestamp=trade.timestamp,
                intent_type=IntentType(trade.intent_type) if trade.intent_type else IntentType.UNKNOWN,
                executed_price=Decimal("0"),  # Price embedded in execution
                fee_usd=Decimal("0"),  # Fees embedded in execution
                slippage_usd=Decimal("0"),
                gas_cost_usd=trade.gas_cost_usd,
                pnl_usd=trade.net_token_flow_usd,  # Pre-gas PnL: TradeRecord.net_pnl_usd subtracts gas itself
                success=True,  # All trades in _trades are successful
                amount_usd=Decimal(trade.metadata.get("amount_usd", "0")),
                protocol=trade.protocol,
                tokens=tokens,
                tx_hash=trade.tx_hash,
                metadata=trade.metadata,
            )
            trade_records.append(record)

        # Get final portfolio value from portfolio tracker PnL calculation
        # The tracker needs current prices - we use initial balances as baseline
        final_value = self._calculate_portfolio_value()

        # Get error summary from error handler
        error_summary = {}
        if self._error_handler:
            error_summary = self._error_handler.get_error_summary()

        logger.info(
            f"[{self._backtest_id}] Paper trading completed for {strategy.strategy_id}: "
            f"ticks={self._tick_count}, trades={len(self._trades)}, "
            f"PnL=${metrics.net_pnl_usd:,.2f}"
        )

        # Calculate initial capital from config's initial balances
        initial_capital = self._calculate_initial_capital()

        # Build config dict with error summary
        config_dict = self.config.to_dict()
        if error_summary:
            config_dict["error_summary"] = error_summary

        # Build compliance violations list
        compliance_violations: list[str] = []
        fallback_usage = self._fallback_usage.copy()

        # Check if any fallbacks were used
        if fallback_usage.get("hardcoded_price", 0) > 0:
            count = fallback_usage["hardcoded_price"]
            compliance_violations.append(
                f"Hardcoded price fallback used {count} time(s). "
                "Set strict_price_mode=True for institutional-grade backtests."
            )
        if fallback_usage.get("default_gas_price", 0) > 0:
            count = fallback_usage["default_gas_price"]
            compliance_violations.append(f"Default gas price fallback used {count} time(s).")
        if fallback_usage.get("default_usd_amount", 0) > 0:
            count = fallback_usage["default_usd_amount"]
            compliance_violations.append(f"Default USD amount fallback used {count} time(s).")
        if fallback_usage.get("zero_output_placeholder", 0) > 0:
            count = fallback_usage["zero_output_placeholder"]
            compliance_violations.append(
                f"Zero output placeholder used {count} time(s) due to missing receipt data. "
                "PnL calculations may be inaccurate."
            )

        # Determine institutional compliance
        institutional_compliance = len(compliance_violations) == 0

        return BacktestResult(
            engine=BacktestEngine.PAPER,
            strategy_id=strategy.strategy_id,
            start_time=run_started_at,
            end_time=run_ended_at,
            metrics=metrics,
            trades=trade_records,
            equity_curve=self._equity_curve,
            initial_capital_usd=initial_capital,
            final_capital_usd=final_value,
            chain=self.config.chain,
            run_started_at=run_started_at,
            run_ended_at=run_ended_at,
            run_duration_seconds=(run_ended_at - run_started_at).total_seconds(),
            config=config_dict,
            error=error,
            backtest_id=self._backtest_id,
            fallback_usage=fallback_usage,
            institutional_compliance=institutional_compliance,
            compliance_violations=compliance_violations,
        )

    async def start(self, strategy: PaperTradeableStrategy) -> None:
        """Start continuous paper trading until stop() is called.

        This method runs paper trading indefinitely. Call stop() to end
        the session gracefully.

        Args:
            strategy: Strategy to paper trade

        Raises:
            RuntimeError: If already running
        """
        if self._running:
            raise RuntimeError("PaperTrader is already running")

        # Run with no duration limit (will run until stop() is called)
        asyncio.create_task(self.run(strategy, duration_seconds=float("inf")))

    async def stop(self) -> None:
        """Stop the current paper trading session.

        Signals the trading loop to exit gracefully. The current tick
        will complete before stopping.
        """
        if self._running:
            logger.info(f"[{self._backtest_id}] Stopping paper trading session...")
            self._running = False

    def is_running(self) -> bool:
        """Check if paper trading is currently active.

        Returns:
            True if a session is running
        """
        return self._running

    async def tick(self) -> PaperTrade | None:
        """Execute one trading cycle (tick) manually.

        This method allows manual tick execution for testing or custom
        integration. It performs one complete trading cycle:

        1. Optionally resets fork to latest block (based on config)
        2. Creates MarketSnapshot from current fork state
        3. Calls strategy.decide(snapshot) to get intent
        4. If intent returned (non-HOLD), executes via orchestrator on fork
        5. Records trade result in portfolio_tracker
        6. Handles and records errors gracefully

        Prerequisites:
            - PaperTrader must be initialized (call start() or run() first)
            - A strategy must be set via _current_strategy

        Returns:
            PaperTrade if a trade was executed successfully, None otherwise
            (including HOLD decisions, errors, or no strategy set)

        Example:
            # Manual tick control
            trader = PaperTrader(fork_manager, portfolio_tracker, config)
            await trader._initialize_fork()
            await trader._initialize_orchestrator()
            trader._current_strategy = my_strategy
            trader._running = True

            # Execute single tick
            trade = await trader.tick()
            if trade:
                print(f"Trade executed: {trade.tx_hash}")
        """
        if not self._running:
            logger.warning(f"[{self._backtest_id}] tick() called but PaperTrader is not running")
            return None

        if not self._current_strategy:
            logger.warning(f"[{self._backtest_id}] tick() called but no strategy is set")
            return None

        # Check if fork needs refresh before tick
        if await self._should_refresh_fork():
            await self._refresh_fork()

        # Execute the tick
        return await self._execute_tick(self._current_strategy)

    async def run_loop(
        self,
        strategy: PaperTradeableStrategy,
        max_ticks: int | None = None,
    ) -> PaperTradingSummary:
        """Run a paper trading session with a simple tick loop.

        This method implements the classic paper trading loop pattern:
        1. Initialize fork and orchestrator
        2. Loop: call tick(), sleep for tick_interval_seconds
        3. Stop when max_ticks reached or _running becomes False
        4. Cleanup in finally block

        Unlike run(), which returns a comprehensive BacktestResult, this method
        returns a simpler PaperTradingSummary focused on trade statistics.

        Args:
            strategy: Strategy to paper trade
            max_ticks: Maximum number of ticks to run (None = use config.max_ticks,
                      if that's also None, runs until stop() is called)

        Returns:
            PaperTradingSummary with session statistics and trade details

        Raises:
            RuntimeError: If already running

        Example:
            trader = PaperTrader(fork_manager, portfolio_tracker, config)
            summary = await trader.run_loop(my_strategy, max_ticks=100)
            print(summary.summary())
        """
        if self._running:
            raise RuntimeError("PaperTrader is already running")

        # Determine effective max_ticks
        effective_max_ticks: int | None = max_ticks if max_ticks is not None else self.config.max_ticks

        # Initialize session state
        session_start = datetime.now(UTC)
        self._session_start = session_start
        self._running = True
        self._current_strategy = strategy
        self._trades = []
        self._errors = []
        self._equity_curve = []
        self._tick_count = 0

        # Generate unique backtest_id for correlation across all log messages
        self._backtest_id = str(uuid.uuid4())

        # Initialize error handler for consistent error handling
        self._error_handler = BacktestErrorHandler(BacktestErrorConfig())

        logger.info(
            f"[{self._backtest_id}] Starting paper trading loop for {strategy.strategy_id} "
            f"(max_ticks={effective_max_ticks}, interval={self.config.tick_interval_seconds}s)"
        )

        self._emit_event(
            PaperTradeEventType.SESSION_STARTED,
            {
                "strategy_id": strategy.strategy_id,
                "max_ticks": effective_max_ticks,
                "tick_interval_seconds": self.config.tick_interval_seconds,
            },
        )

        try:
            # Initialize fork
            await self._initialize_fork()

            # Initialize orchestrator with fork RPC
            await self._initialize_orchestrator()

            # Record initial equity point
            await self._record_equity_point()

            # Main tick loop
            while self._running:
                # Check tick limit
                if effective_max_ticks is not None and self._tick_count >= effective_max_ticks:
                    logger.info(f"[{self._backtest_id}] Max ticks ({effective_max_ticks}) reached, stopping")
                    break

                # Execute tick (includes fork refresh check)
                await self.tick()
                self._tick_count += 1

                # Sleep until next tick (only if we'll continue)
                if self._running and (effective_max_ticks is None or self._tick_count < effective_max_ticks):
                    await asyncio.sleep(self.config.tick_interval_seconds)

        except asyncio.CancelledError:
            logger.info(f"[{self._backtest_id}] Paper trading loop cancelled")
        except Exception as e:
            # Use error handler for consistent classification
            if self._error_handler:
                handler_result = self._error_handler.handle_error(e, context="paper_trading_loop")
                if handler_result.should_stop:
                    logger.error(f"[{self._backtest_id}] Fatal error in paper trading loop: {e}")
                else:
                    logger.warning(f"[{self._backtest_id}] Non-critical error in paper trading loop: {e}")
            else:
                logger.exception(f"[{self._backtest_id}] Paper trading loop failed: {e}")
            # Record the error
            error = PaperTradeError(
                timestamp=datetime.now(UTC),
                intent={},
                error_type=PaperTradeErrorType.INTERNAL_ERROR,
                error_message=f"Loop error: {e}",
                block_number=self.fork_manager.current_block if self.fork_manager.is_running else None,
                metadata={"exception_type": type(e).__name__},
            )
            self._errors.append(error)
        finally:
            # Signal stop and cleanup
            await self.stop()
            await self._cleanup()

        session_end = datetime.now(UTC)
        duration = session_end - session_start

        self._emit_event(
            PaperTradeEventType.SESSION_ENDED,
            {
                "strategy_id": strategy.strategy_id,
                "tick_count": self._tick_count,
                "trade_count": len(self._trades),
                "error_count": len(self._errors),
            },
        )

        # Build error summary
        error_summary: dict[str, int] = {}
        for error in self._errors:
            error_type_str = error.error_type.value
            error_summary[error_type_str] = error_summary.get(error_type_str, 0) + 1

        # Calculate total gas
        total_gas_used = sum(t.gas_used for t in self._trades)
        total_gas_cost_usd = sum((t.gas_cost_usd for t in self._trades), Decimal("0"))

        # Create summary
        summary = PaperTradingSummary(
            strategy_id=strategy.strategy_id,
            start_time=session_start,
            duration=duration,
            total_trades=len(self._trades) + len(self._errors),
            successful_trades=len(self._trades),
            failed_trades=len(self._errors),
            chain=self.config.chain,
            initial_balances=dict(self.portfolio_tracker.initial_balances),
            final_balances=dict(self.portfolio_tracker.current_balances),
            total_gas_used=total_gas_used,
            total_gas_cost_usd=total_gas_cost_usd,
            pnl_usd=self._calculate_pnl_usd(),
            error_summary=error_summary,
            trades=list(self._trades),
            errors=list(self._errors),
        )

        logger.info(
            f"[{self._backtest_id}] Paper trading loop completed for {strategy.strategy_id}: "
            f"ticks={self._tick_count}, trades={len(self._trades)}, "
            f"errors={len(self._errors)}"
        )

        return summary

    def _calculate_pnl_usd(self) -> Decimal | None:
        """Calculate PnL in USD from portfolio changes.

        Returns:
            Estimated PnL or None if calculation not possible
        """
        try:
            initial_value = self._calculate_initial_capital()
            final_value = self._calculate_portfolio_value()
            return final_value - initial_value
        except Exception:
            return None

    # =========================================================================
    # Internal Methods
    # =========================================================================

    async def _initialize_fork(self) -> None:
        """Initialize the Anvil fork for paper trading."""
        logger.info(f"[{self._backtest_id}] Initializing Anvil fork for chain={self.config.chain}")

        # Start fork at latest block
        await self.fork_manager.start()

        if self.fork_manager.is_running:
            logger.info(f"[{self._backtest_id}] Fork initialized at block {self.fork_manager.current_block}")

            # Initialize portfolio tracker session with config balances
            initial_balances = self.config.get_initial_balances()
            self.portfolio_tracker.start_session(
                initial_balances=initial_balances,
                chain=self.config.chain,
            )

            # Fund the on-chain wallet with initial balances (first startup)
            await self._sync_wallet_to_fork(use_initial=True)

    async def _sync_wallet_to_fork(self, *, use_initial: bool = False) -> None:
        """Fund the on-chain wallet to match tracked portfolio balances.

        Uses the portfolio tracker's current balances so that after a fork
        reset the on-chain wallet matches the strategy's actual position.
        Falls back to config initial balances on first init (before the
        tracker has been started) or when use_initial=True.

        Args:
            use_initial: Force using config initial balances (for first startup).
        """
        # Use the hardcoded Anvil account #0 address
        # TODO: Support custom private key via PaperTraderConfig
        wallet_address = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

        # Use tracker's current balances if available, otherwise config initial
        if not use_initial and self.portfolio_tracker.current_balances:
            balances = dict(self.portfolio_tracker.current_balances)
        else:
            balances = self.config.get_initial_balances()

        # Fund ETH
        eth_amount = balances.pop("ETH", None)
        if eth_amount and eth_amount > 0:
            success = await self.fork_manager.fund_wallet(wallet_address, eth_amount)
            if not success:
                logger.warning(f"[{self._backtest_id}] Failed to fund wallet with {eth_amount} ETH")

        # Fund ERC-20 tokens
        if balances:
            success = await self.fork_manager.fund_tokens(wallet_address, balances)
            if not success:
                logger.warning(f"[{self._backtest_id}] Failed to fund some ERC-20 tokens")

    async def _initialize_orchestrator(self) -> None:
        """Initialize the execution orchestrator with fork connection."""
        from almanak.framework.execution.signer.local import LocalKeySigner
        from almanak.framework.execution.simulator.direct import DirectSimulator
        from almanak.framework.execution.submitter.public import PublicMempoolSubmitter

        # Get fork RPC URL
        fork_rpc = self.fork_manager.get_rpc_url()

        # Create signer with test private key (for fork only)
        # Note: This uses a deterministic test key for Anvil (first default Anvil account)
        # TODO: Support custom private key via PaperTraderConfig for non-default Anvil wallets
        test_private_key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
        signer = LocalKeySigner(private_key=test_private_key)

        # Create submitter connected to fork
        submitter = PublicMempoolSubmitter(rpc_url=fork_rpc)

        # Create simulator
        simulator = DirectSimulator()

        # Create orchestrator with default settings
        self._orchestrator = ExecutionOrchestrator(
            signer=signer,
            submitter=submitter,
            simulator=simulator,
            chain=self.config.chain,
            rpc_url=fork_rpc,
        )

        logger.debug(f"[{self._backtest_id}] Orchestrator initialized with fork RPC: {fork_rpc}")

    async def _cleanup(self) -> None:
        """Cleanup resources after paper trading session."""
        # Stop the fork
        try:
            await self.fork_manager.stop()
        except Exception as e:
            logger.warning(f"[{self._backtest_id}] Error stopping fork: {e}")

        self._orchestrator = None

    async def _execute_tick(self, strategy: PaperTradeableStrategy) -> PaperTrade | None:
        """Execute a single trading tick.

        This method:
        1. Gets current fork state
        2. Creates MarketSnapshot from fork
        3. Calls strategy.decide()
        4. Executes intent if non-HOLD
        5. Records trade and updates portfolio

        Args:
            strategy: Strategy to execute

        Returns:
            PaperTrade if a trade was executed successfully, None otherwise
        """
        tick_start = datetime.now(UTC)
        trade_result: PaperTrade | None = None

        self._emit_event(
            PaperTradeEventType.TICK_STARTED,
            {"tick_number": self._tick_count},
        )

        try:
            # Check fork is running
            if not self.fork_manager.is_running:
                logger.warning(f"[{self._backtest_id}] Fork not running, skipping tick")
                return None

            # Fetch prices for portfolio tokens (cached for IntentCompiler use)
            token_prices = await self._get_portfolio_prices()
            self._cached_prices = token_prices

            # Create market snapshot from fork (with price oracle so strategies can call market.price())
            wallet_address = self._orchestrator.signer.address if self._orchestrator else ""
            snapshot = await create_market_snapshot_from_fork(
                fork_manager=self.fork_manager,
                chain=self.config.chain,
                wallet_address=wallet_address,
                portfolio_tracker=self.portfolio_tracker,
                token_prices=token_prices,
                price_oracle=self._price_aggregator,
                rsi_calculator=self._rsi_calculator,
            )

            # Call strategy decide
            try:
                decide_result = strategy.decide(snapshot)
                if self._error_handler:
                    self._error_handler.record_success()
            except Exception as e:
                if self._error_handler:
                    result = self._error_handler.handle_error(e, context=f"strategy_decide:tick_{self._tick_count}")
                    if result.should_stop:
                        logger.error(f"[{self._backtest_id}] Fatal error in strategy decide(): {e}")
                        raise
                else:
                    logger.warning(f"[{self._backtest_id}] Strategy decide() raised exception: {e}")
                decide_result = None

            # Extract intent
            intent = self._extract_intent(decide_result)

            self._emit_event(
                PaperTradeEventType.INTENT_DECIDED,
                {
                    "intent_type": self._get_intent_type(intent).value if intent else "HOLD",
                    "tick_number": self._tick_count,
                },
            )

            # Execute if not HOLD
            if intent is not None and not self._is_hold_intent(intent):
                trade_result = await self._execute_intent(intent, strategy, snapshot)

            # Record equity point
            await self._record_equity_point()

        except Exception as e:
            # Use error handler for consistent classification
            if self._error_handler:
                result = self._error_handler.handle_error(e, context=f"tick_execution:tick_{self._tick_count}")
                if result.should_stop:
                    logger.error(f"[{self._backtest_id}] Fatal error during tick {self._tick_count}: {e}")
                    # Record as PaperTradeError for tracking
                    error = PaperTradeError(
                        timestamp=datetime.now(UTC),
                        intent={},
                        error_type=PaperTradeErrorType.INTERNAL_ERROR,
                        error_message=str(e),
                        block_number=self.fork_manager.current_block if self.fork_manager.is_running else None,
                        metadata={
                            "exception_type": type(e).__name__,
                            "tick_number": self._tick_count,
                        },
                    )
                    self._errors.append(error)
                else:
                    logger.warning(f"[{self._backtest_id}] Non-critical error during tick {self._tick_count}: {e}")
            else:
                logger.exception(f"[{self._backtest_id}] Error during tick {self._tick_count}: {e}")
            self._emit_event(
                PaperTradeEventType.ERROR,
                {"error": str(e), "tick_number": self._tick_count},
            )

        tick_end = datetime.now(UTC)
        tick_duration = (tick_end - tick_start).total_seconds()

        self._emit_event(
            PaperTradeEventType.TICK_ENDED,
            {
                "tick_number": self._tick_count,
                "duration_seconds": tick_duration,
            },
        )

        return trade_result

    async def _execute_intent(
        self,
        intent: Any,
        strategy: PaperTradeableStrategy,
        snapshot: MarketSnapshot,
    ) -> PaperTrade | None:
        """Execute an intent on the fork.

        Args:
            intent: Intent to execute
            strategy: Strategy that generated the intent
            snapshot: Market snapshot used for decision

        Returns:
            PaperTrade record if successful, None if failed
        """
        if not self._orchestrator:
            logger.error(f"[{self._backtest_id}] No orchestrator available for execution")
            return None

        execution_start = datetime.now(UTC)
        intent_type = self._get_intent_type(intent)

        self._emit_event(
            PaperTradeEventType.TRADE_EXECUTING,
            {"intent_type": intent_type.value},
        )

        # Serialize intent for storage
        intent_dict = self._serialize_intent(intent)

        try:
            # Compile intent to ActionBundle
            action_bundle = self._compile_intent(intent)

            if not action_bundle:
                logger.warning(f"[{self._backtest_id}] Failed to compile intent: {intent}")
                error = PaperTradeError(
                    timestamp=execution_start,
                    intent=intent_dict,
                    error_type=PaperTradeErrorType.INTENT_INVALID,
                    error_message="Failed to compile intent to ActionBundle",
                    block_number=self.fork_manager.current_block,
                    metadata={"intent_type": intent_type.value},
                )
                self._errors.append(error)
                return None

            # Create execution context
            context = ExecutionContext(
                strategy_id=strategy.strategy_id,
                chain=self.config.chain,
                wallet_address=self._orchestrator.signer.address,
                simulation_enabled=True,  # Always simulate on fork
            )

            # Execute on fork
            result = await self._orchestrator.execute(action_bundle, context)

            # Calculate execution time
            execution_end = datetime.now(UTC)
            execution_time_ms = int((execution_end - execution_start).total_seconds() * 1000)

            if result.success:
                # Calculate costs
                gas_cost_usd = self._calculate_gas_cost_usd(result)

                # Get transaction details and receipt
                tx_hash = ""
                block_number = self.fork_manager.current_block or 0
                gas_used = 0
                receipt: TransactionReceipt | None = None
                if result.transaction_results:
                    first_result = result.transaction_results[0]
                    tx_hash = first_result.tx_hash or ""
                    if first_result.receipt:
                        receipt = first_result.receipt  # type: ignore[assignment]
                        block_number = receipt.block_number or block_number  # type: ignore[union-attr]
                        gas_used = receipt.gas_used or 0  # type: ignore[union-attr]

                # Get wallet address for receipt parsing
                wallet_address = self._orchestrator.signer.address if self._orchestrator else ""

                # Get token flows from receipt (if available) or intent (fallback)
                tokens_in, tokens_out = await self._extract_token_flows(
                    intent, receipt=receipt, wallet_address=wallet_address
                )

                # Calculate slippage tracking values
                expected_amount_out = self._get_expected_amount_out(intent)
                actual_amount_out = self._get_actual_amount_out(tokens_in, intent)
                actual_slippage_bps = self._calculate_slippage_bps(expected_amount_out, actual_amount_out)

                # Collect token prices at execution time for PnL calculation
                token_prices_usd: dict[str, Decimal] = {}
                all_tokens = set(tokens_in.keys()) | set(tokens_out.keys())
                for token in all_tokens:
                    token_prices_usd[token.upper()] = self._get_token_price_sync(token)

                # Create successful trade
                trade = PaperTrade(
                    timestamp=execution_start,
                    block_number=block_number,
                    intent=intent_dict,
                    tx_hash=tx_hash,
                    gas_used=gas_used,
                    gas_cost_usd=gas_cost_usd,
                    tokens_in=tokens_in,
                    tokens_out=tokens_out,
                    protocol=self._get_intent_protocol(intent),
                    intent_type=intent_type.value,
                    execution_time_ms=execution_time_ms,
                    eth_price_usd=self._get_token_price_sync("ETH"),
                    metadata={
                        "correlation_id": result.correlation_id,
                        "amount_usd": str(self._get_intent_amount_usd(intent)),
                    },
                    expected_amount_out=expected_amount_out,
                    actual_amount_out=actual_amount_out,
                    actual_slippage_bps=actual_slippage_bps,
                    token_prices_usd=token_prices_usd,
                )

                self._trades.append(trade)

                # Update portfolio tracker
                self.portfolio_tracker.record_trade(trade)

                self._emit_event(
                    PaperTradeEventType.TRADE_COMPLETED,
                    {
                        "intent_type": intent_type.value,
                        "tx_hash": trade.tx_hash,
                        "gas_used": trade.gas_used,
                    },
                )

                return trade

            else:
                # Determine error type from result
                error_type = PaperTradeErrorType.INTERNAL_ERROR
                if result.error_phase:
                    phase_name = result.error_phase.value.upper()
                    if "SIMULATION" in phase_name:
                        error_type = PaperTradeErrorType.SIMULATION_FAILED
                    elif "SIGN" in phase_name:
                        error_type = PaperTradeErrorType.INTERNAL_ERROR
                    elif "SUBMIT" in phase_name:
                        error_type = PaperTradeErrorType.RPC_ERROR

                error = PaperTradeError(
                    timestamp=execution_start,
                    intent=intent_dict,
                    error_type=error_type,
                    error_message=result.error or "Unknown error",
                    block_number=self.fork_manager.current_block,
                    metadata={
                        "phase": result.phase.value,
                        "intent_type": intent_type.value,
                    },
                )

                self._errors.append(error)

                self._emit_event(
                    PaperTradeEventType.TRADE_FAILED,
                    {
                        "intent_type": intent_type.value,
                        "error": result.error,
                        "phase": result.phase.value,
                    },
                )

                return None

        except Exception as e:
            # Use error handler for consistent classification
            if self._error_handler:
                handler_result = self._error_handler.handle_error(e, context=f"intent_execution:{intent_type.value}")
                if handler_result.should_stop:
                    logger.error(f"[{self._backtest_id}] Fatal error executing intent: {e}")
                elif handler_result.should_retry:
                    logger.warning(f"[{self._backtest_id}] Recoverable error executing intent (retry possible): {e}")
                else:
                    logger.warning(f"[{self._backtest_id}] Non-critical error executing intent: {e}")
            else:
                logger.exception(f"[{self._backtest_id}] Error executing intent: {e}")

            error = PaperTradeError(
                timestamp=execution_start,
                intent=intent_dict,
                error_type=PaperTradeErrorType.INTERNAL_ERROR,
                error_message=str(e),
                block_number=self.fork_manager.current_block,
                metadata={
                    "exception_type": type(e).__name__,
                    "intent_type": intent_type.value,
                },
            )

            self._errors.append(error)

            self._emit_event(
                PaperTradeEventType.TRADE_FAILED,
                {
                    "intent_type": intent_type.value,
                    "error": str(e),
                    "phase": "EXCEPTION",
                },
            )

            return None

    async def _record_equity_point(self) -> None:
        """Record current portfolio value as equity point.

        Note: This method refreshes the price cache before calculating portfolio
        value to ensure newly acquired tokens have prices and to avoid using
        stale prices across ticks.
        """
        # Refresh prices for all portfolio tokens to ensure cache is up-to-date
        # This prevents issues with:
        # 1. Empty cache on initial equity point
        # 2. New tokens acquired during trades not having prices
        # 3. Stale prices when strict_price_mode is enabled
        await self._get_portfolio_prices()

        now = datetime.now(UTC)
        value = self._calculate_portfolio_value()

        point = EquityPoint(
            timestamp=now,
            value_usd=value,
        )

        self._equity_curve.append(point)

        self._emit_event(
            PaperTradeEventType.PORTFOLIO_UPDATED,
            {
                "value_usd": str(value),
                "timestamp": now.isoformat(),
            },
        )

    async def _should_refresh_fork(self) -> bool:
        """Check if the fork should be refreshed.

        Returns True if:
        - Fork has been reset in this tick (reset_fork_every_tick is True)
        - Fork has become stale (not running)

        Returns:
            True if fork should be refreshed
        """
        # Check if we should reset each tick
        if not self.config.reset_fork_every_tick:
            return False

        if not self.fork_manager.is_running:
            return True

        return True  # Reset every tick if configured

    async def _refresh_fork(self) -> None:
        """Refresh the Anvil fork to a more recent block.

        This resets the fork to the latest block while preserving portfolio state.
        """
        logger.info(f"[{self._backtest_id}] Refreshing Anvil fork to latest block...")

        self._emit_event(
            PaperTradeEventType.FORK_REFRESHED,
            {"reason": "reset_each_tick"},
        )

        # Reset fork to latest block
        await self.fork_manager.reset_to_latest()

        # Reinitialize orchestrator with refreshed fork
        await self._initialize_orchestrator()

        # Re-fund wallet after fork reset (balances are wiped on reset)
        await self._sync_wallet_to_fork()

        if self.fork_manager.is_running:
            logger.info(f"[{self._backtest_id}] Fork refreshed to block {self.fork_manager.current_block}")

    def _emit_event(self, event_type: str, data: dict[str, Any]) -> None:
        """Emit a paper trading event.

        Args:
            event_type: Type of event
            data: Event data
        """
        if self.event_callback:
            try:
                self.event_callback(event_type, data)
            except Exception as e:
                logger.warning(f"[{self._backtest_id}] Event callback failed: {e}")

    # =========================================================================
    # Intent Processing Helpers
    # =========================================================================

    def _extract_intent(self, decide_result: Any) -> Any:
        """Extract the intent from a decide() result."""
        if decide_result is None:
            return None

        # Check if it's a DecideResult with an intent attribute
        if hasattr(decide_result, "intent"):
            return decide_result.intent

        # Check if it's a DecideResult tuple-like (intent, context)
        if isinstance(decide_result, tuple) and len(decide_result) >= 1:
            return decide_result[0]

        # Otherwise, assume it's an intent directly
        return decide_result

    def _is_hold_intent(self, intent: Any) -> bool:
        """Check if an intent is a HOLD intent."""
        if intent is None:
            return True

        # Check intent_type attribute
        if hasattr(intent, "intent_type"):
            intent_type = intent.intent_type
            if hasattr(intent_type, "value"):
                return intent_type.value == "HOLD"
            return str(intent_type) == "HOLD"

        # Check class name
        if hasattr(intent, "__class__"):
            if intent.__class__.__name__ == "HoldIntent":
                return True

        return False

    def _get_intent_type(self, intent: Any) -> IntentType:
        """Extract IntentType from an intent object."""
        if intent is None:
            return IntentType.HOLD

        # Check for intent_type attribute
        if hasattr(intent, "intent_type"):
            intent_type_value = intent.intent_type
            if isinstance(intent_type_value, IntentType):
                return intent_type_value
            if hasattr(intent_type_value, "value"):
                try:
                    return IntentType(intent_type_value.value)
                except ValueError:
                    pass
            try:
                return IntentType(str(intent_type_value))
            except ValueError:
                pass

        # Check class name for common intent types
        class_name = intent.__class__.__name__.upper()
        type_mappings = {
            "SWAP": IntentType.SWAP,
            "LPOPEN": IntentType.LP_OPEN,
            "LP_OPEN": IntentType.LP_OPEN,
            "LPCLOSE": IntentType.LP_CLOSE,
            "LP_CLOSE": IntentType.LP_CLOSE,
            "PERPOPEN": IntentType.PERP_OPEN,
            "PERP_OPEN": IntentType.PERP_OPEN,
            "PERPCLOSE": IntentType.PERP_CLOSE,
            "PERP_CLOSE": IntentType.PERP_CLOSE,
            "SUPPLY": IntentType.SUPPLY,
            "WITHDRAW": IntentType.WITHDRAW,
            "BORROW": IntentType.BORROW,
            "REPAY": IntentType.REPAY,
            "BRIDGE": IntentType.BRIDGE,
            "HOLD": IntentType.HOLD,
        }

        for key, intent_type in type_mappings.items():
            if key in class_name:
                return intent_type

        return IntentType.UNKNOWN

    def _compile_intent(self, intent: Any) -> ActionBundle | None:
        """Compile an intent to an ActionBundle.

        Args:
            intent: Intent to compile

        Returns:
            ActionBundle or None if compilation fails
        """
        # Check if intent has a compile method
        if hasattr(intent, "compile"):
            try:
                return intent.compile()
            except Exception as e:
                logger.warning(f"[{self._backtest_id}] Intent compile() failed: {e}")

        # Try using IntentCompiler with current prices
        try:
            from almanak.framework.intents import IntentCompiler

            # Build price oracle dict from cached portfolio prices
            price_dict = getattr(self, "_cached_prices", None)

            wallet_address = self._orchestrator.signer.address if self._orchestrator else ""
            compiler = IntentCompiler(
                chain=self.config.chain,
                wallet_address=wallet_address,
                price_oracle=price_dict,
                rpc_url=self.fork_manager.get_rpc_url() if self.fork_manager.is_running else None,
            )
            result = compiler.compile(intent)
            if result.status.value == "SUCCESS":
                return result.action_bundle
        except Exception as e:
            logger.warning(f"[{self._backtest_id}] IntentCompiler failed: {e}")

        return None

    def _get_intent_amount_usd(self, intent: Any) -> Decimal:
        """Extract USD amount from an intent."""
        # Check for direct USD amount
        for attr in ["amount_usd", "notional_usd", "value_usd", "collateral_usd"]:
            if hasattr(intent, attr):
                value = getattr(intent, attr)
                if value is not None:
                    return Decimal(str(value))

        return Decimal("0")

    def _calculate_gas_cost_usd(self, result: ExecutionResult) -> Decimal:
        """Calculate gas cost in USD from execution result.

        Uses the price provider to get the current ETH price.
        """
        if not result.total_gas_cost_wei:
            return Decimal("0")

        # Get ETH price from price provider (uses cache/fallback)
        eth_price = self._get_token_price_sync("ETH")

        # Convert wei to ETH
        gas_cost_eth = Decimal(result.total_gas_cost_wei) / Decimal(10**18)

        return gas_cost_eth * eth_price

    def _extract_fee_usd(self, action_bundle: ActionBundle) -> Decimal:
        """Extract expected fee from action bundle."""
        if action_bundle.metadata:
            fee = action_bundle.metadata.get("expected_fee_usd")
            if fee is not None:
                return Decimal(str(fee))
        return Decimal("0")

    def _serialize_intent(self, intent: Any) -> dict[str, Any]:
        """Serialize an intent to a dictionary for storage.

        Args:
            intent: Intent object to serialize

        Returns:
            Dictionary representation of the intent
        """
        if intent is None:
            return {}

        # Check if intent has a to_dict method
        if hasattr(intent, "to_dict"):
            try:
                return intent.to_dict()
            except Exception:
                pass

        # Check if intent has __dict__
        if hasattr(intent, "__dict__"):
            try:
                result = {}
                for key, value in intent.__dict__.items():
                    if not key.startswith("_"):
                        # Convert Decimals and other types to strings
                        if isinstance(value, Decimal):
                            result[key] = str(value)
                        elif hasattr(value, "value"):  # Enum
                            result[key] = value.value
                        else:
                            result[key] = value
                return result
            except Exception:
                pass

        # Fallback to string representation
        return {"repr": str(intent)}

    async def _extract_token_flows(
        self,
        intent: Any,
        receipt: TransactionReceipt | None = None,
        wallet_address: str = "",
    ) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
        """Extract token inflows and outflows from transaction receipt or intent.

        When a receipt is provided, parses actual ERC-20 Transfer events to get
        real token flows. Falls back to intent-based estimation when no receipt
        is available.

        Uses get_token_decimals_with_fallback() to correctly handle tokens with
        non-standard decimals (e.g., USDC/USDT with 6 decimals).

        Args:
            intent: Intent object
            receipt: Transaction receipt with logs (optional)
            wallet_address: Wallet address for receipt parsing

        Returns:
            Tuple of (tokens_in, tokens_out) dictionaries with token symbols as keys
        """
        tokens_in: dict[str, Decimal] = {}
        tokens_out: dict[str, Decimal] = {}

        # Try to extract actual token flows from receipt
        if receipt is not None and wallet_address:
            receipt_dict = receipt.to_dict()
            flows = extract_receipt_token_flows(receipt_dict, wallet_address)

            # Get chain_id and RPC URL for decimal lookups
            chain_id = self.fork_manager.chain_id
            rpc_url = self.fork_manager.get_rpc_url() if self.fork_manager.is_running else None

            # Convert from smallest unit to Decimal with correct token decimals
            # Use symbol mapping for human-readable portfolio keys (US-065c)
            for token_addr, amount in flows.tokens_in.items():
                decimals = await get_token_decimals_with_fallback(chain_id, token_addr, rpc_url)
                symbol = await get_token_symbol_with_fallback(chain_id, token_addr, rpc_url)
                tokens_in[symbol] = Decimal(str(amount)) / Decimal(10**decimals)

            for token_addr, amount in flows.tokens_out.items():
                decimals = await get_token_decimals_with_fallback(chain_id, token_addr, rpc_url)
                symbol = await get_token_symbol_with_fallback(chain_id, token_addr, rpc_url)
                tokens_out[symbol] = Decimal(str(amount)) / Decimal(10**decimals)

            # If we got flows from receipt, return them
            if tokens_in or tokens_out:
                logger.debug(
                    f"[{self._backtest_id}] Extracted token flows from receipt: {len(tokens_in)} tokens in, {len(tokens_out)} tokens out"
                )
                return tokens_in, tokens_out

        # Fallback: Extract expected flows from intent attributes
        intent_type = self._get_intent_type(intent)

        if intent_type == IntentType.SWAP:
            # Extract from_token and to_token
            from_token = getattr(intent, "from_token", None)
            to_token = getattr(intent, "to_token", None)
            intent_amount = getattr(intent, "amount", None) or getattr(intent, "amount_in", None)

            if from_token and intent_amount:
                tokens_out[str(from_token).upper()] = Decimal(str(intent_amount))
            if to_token:
                # Try to get expected amount from intent attributes
                expected_out = self._get_expected_amount_out(intent)
                if expected_out is not None:
                    tokens_in[str(to_token).upper()] = expected_out
                else:
                    # No receipt and no expected amount - use zero with warning
                    logger.warning(
                        f"[{self._backtest_id}] Cannot determine swap output amount for {to_token} "
                        "without receipt. Using zero placeholder - this may affect PnL accuracy."
                    )
                    self._track_fallback("zero_output_placeholder")
                    tokens_in[str(to_token).upper()] = Decimal("0")

        elif intent_type in (IntentType.SUPPLY, IntentType.REPAY):
            token = getattr(intent, "token", None) or getattr(intent, "asset", None)
            intent_amount = getattr(intent, "amount", None)
            if token and intent_amount:
                tokens_out[str(token).upper()] = Decimal(str(intent_amount))

        elif intent_type in (IntentType.WITHDRAW, IntentType.BORROW):
            token = getattr(intent, "token", None) or getattr(intent, "asset", None)
            intent_amount = getattr(intent, "amount", None)
            if token and intent_amount:
                tokens_in[str(token).upper()] = Decimal(str(intent_amount))

        elif intent_type == IntentType.LP_OPEN:
            token0 = getattr(intent, "token0", None) or getattr(intent, "token_a", None)
            token1 = getattr(intent, "token1", None) or getattr(intent, "token_b", None)
            amount0 = getattr(intent, "amount0", None)
            amount1 = getattr(intent, "amount1", None)
            if token0 and amount0:
                tokens_out[str(token0).upper()] = Decimal(str(amount0))
            if token1 and amount1:
                tokens_out[str(token1).upper()] = Decimal(str(amount1))

        elif intent_type == IntentType.LP_CLOSE:
            token0 = getattr(intent, "token0", None) or getattr(intent, "token_a", None)
            token1 = getattr(intent, "token1", None) or getattr(intent, "token_b", None)
            if token0 or token1:
                # LP_CLOSE without receipt - we can't know actual output amounts
                logger.warning(
                    f"[{self._backtest_id}] Cannot determine LP close output amounts "
                    "without receipt. Using zero placeholder - this may affect PnL accuracy."
                )
                self._track_fallback("zero_output_placeholder")
            if token0:
                tokens_in[str(token0).upper()] = Decimal("0")
            if token1:
                tokens_in[str(token1).upper()] = Decimal("0")

        return tokens_in, tokens_out

    def _get_expected_amount_out(self, intent: Any) -> Decimal | None:
        """Extract expected output amount from an intent.

        Looks for common attributes that indicate expected output:
        - expected_amount_out, amount_out_min, min_amount_out (swap)
        - expected_amount, quote_amount (general)

        Args:
            intent: Intent object

        Returns:
            Expected output amount as Decimal, or None if not found
        """
        for attr in [
            "expected_amount_out",
            "amount_out_min",
            "min_amount_out",
            "expected_amount",
            "quote_amount",
            "amount_out",
        ]:
            if hasattr(intent, attr):
                value = getattr(intent, attr)
                if value is not None:
                    try:
                        return Decimal(str(value))
                    except Exception:
                        pass
        return None

    def _get_actual_amount_out(self, tokens_in: dict[str, Decimal], intent: Any) -> Decimal | None:
        """Get actual output amount from parsed token flows.

        For swaps, returns the amount of the target token received.
        For other intent types, returns the first token in amount.

        Args:
            tokens_in: Dictionary of tokens received {token: amount}
            intent: Intent object (to determine target token)

        Returns:
            Actual output amount as Decimal, or None if not determinable
        """
        if not tokens_in:
            return None

        # For swaps, try to find the target token
        intent_type = self._get_intent_type(intent)
        if intent_type == IntentType.SWAP:
            to_token = getattr(intent, "to_token", None)
            if to_token:
                to_token_upper = str(to_token).upper()
                # Check both symbol and address forms
                for token_key, amount in tokens_in.items():
                    if token_key.upper() == to_token_upper or to_token_upper in token_key.upper():
                        return amount

        # Fallback: return sum of all tokens in
        if tokens_in:
            return sum(tokens_in.values(), Decimal("0"))

        return None

    def _calculate_slippage_bps(self, expected: Decimal | None, actual: Decimal | None) -> int | None:
        """Calculate slippage in basis points (bps).

        Slippage = (expected - actual) / expected * 10000

        Positive slippage means received less than expected (negative for user).
        Negative slippage means received more than expected (positive for user).

        Args:
            expected: Expected output amount
            actual: Actual output amount

        Returns:
            Slippage in basis points, or None if cannot be calculated
        """
        if expected is None or actual is None:
            return None

        if expected == Decimal("0"):
            # Cannot calculate slippage from zero expected
            return None

        slippage = (expected - actual) / expected * Decimal("10000")
        return int(slippage)

    def _get_intent_protocol(self, intent: Any) -> str:
        """Extract the protocol from an intent object.

        Args:
            intent: Intent object

        Returns:
            Protocol name string
        """
        # Check for protocol attribute
        for attr in ["protocol", "protocol_name", "connector", "adapter"]:
            if hasattr(intent, attr):
                value = getattr(intent, attr)
                if value and isinstance(value, str):
                    return value.lower()

        # Infer from class name
        class_name = intent.__class__.__name__.lower()
        if "uniswap" in class_name:
            return "uniswap_v3"
        if "gmx" in class_name:
            return "gmx"
        if "aave" in class_name:
            return "aave_v3"
        if "hyperliquid" in class_name:
            return "hyperliquid"
        if "across" in class_name or "stargate" in class_name:
            return "bridge"

        return "default"

    # =========================================================================
    # Price Provider Helpers
    # =========================================================================

    def _init_price_provider(self) -> None:
        """Initialize price provider based on config.price_source setting.

        Implements a fallback chain for price sourcing:
        - 'auto': Tries Chainlink -> TWAP -> CoinGecko in order
        - 'coingecko': Use CoinGecko API for market prices
        - 'chainlink': Use Chainlink oracles for on-chain prices
        - 'twap': Use TWAP from DEX pools for on-chain prices

        When 'auto' is selected, the system attempts to fetch prices from
        each provider in priority order, falling back to the next if one
        fails or returns stale data.

        The price provider is used for portfolio valuation and gas cost
        calculations throughout the paper trading session.
        """
        price_source = self.config.price_source

        # Define fallback order based on price_source setting
        # For 'auto', use full fallback chain: Chainlink -> TWAP -> CoinGecko
        # For specific sources, just use that source
        if price_source == "auto":
            self._price_source_order = ["chainlink", "twap", "coingecko"]
            logger.info(
                "[%s] Price source 'auto' selected, using fallback chain: Chainlink -> TWAP -> CoinGecko",
                self._backtest_id,
            )
        else:
            self._price_source_order = [price_source]
            logger.info(
                "[%s] Price source '%s' selected as primary provider",
                self._backtest_id,
                price_source,
            )

        # Initialize CoinGecko provider (always available as fallback)
        try:
            coingecko_source = CoinGeckoPriceSource()
            self._price_aggregator = PriceAggregator(sources=[coingecko_source])
            logger.info(
                "[%s] Initialized CoinGecko price provider",
                self._backtest_id,
            )
        except Exception as e:
            # Use error handler for consistent classification
            if self._error_handler:
                self._error_handler.handle_error(e, context="init_coingecko_provider")
            logger.warning(
                "[%s] Failed to initialize CoinGecko provider: %s",
                self._backtest_id,
                str(e),
            )
            self._price_aggregator = None

        # Initialize Chainlink provider if needed
        # Note: Chainlink requires RPC access for on-chain queries, which is
        # available after fork initialization via self.config.rpc_url
        if "chainlink" in self._price_source_order:
            try:
                # Map chain name to Chainlink-supported chain identifier
                chain_mapping = {
                    "ethereum": "ethereum",
                    "arbitrum": "arbitrum",
                    "base": "base",
                    "optimism": "optimism",
                    "polygon": "polygon",
                    "avalanche": "avalanche",
                }
                chainlink_chain = chain_mapping.get(self.config.chain)
                if chainlink_chain:
                    self._chainlink_provider = ChainlinkDataProvider(
                        chain=chainlink_chain,
                        rpc_url=self.config.rpc_url,
                        cache_ttl_seconds=60,
                    )
                    logger.info(
                        "[%s] Initialized Chainlink price provider for chain=%s",
                        self._backtest_id,
                        chainlink_chain,
                    )
                else:
                    logger.warning(
                        "[%s] Chain '%s' not supported by Chainlink provider, will skip to next",
                        self._backtest_id,
                        self.config.chain,
                    )
            except Exception as e:
                # Use error handler for consistent classification
                if self._error_handler:
                    self._error_handler.handle_error(e, context="init_chainlink_provider")
                logger.warning(
                    "[%s] Failed to initialize Chainlink provider: %s",
                    self._backtest_id,
                    str(e),
                )

        # Initialize TWAP provider if needed
        if "twap" in self._price_source_order:
            try:
                # Map chain name to TWAP-supported chain identifier
                twap_chain_mapping = {
                    "ethereum": "ethereum",
                    "arbitrum": "arbitrum",
                    "base": "base",
                    "optimism": "optimism",
                    "polygon": "polygon",
                    "avalanche": "avalanche",
                }
                twap_chain = twap_chain_mapping.get(self.config.chain)
                if twap_chain:
                    self._twap_provider = DEXTWAPDataProvider(
                        chain=twap_chain,
                        rpc_url=self.config.rpc_url,
                        twap_window_seconds=300,  # 5 minute TWAP window
                        cache_ttl_seconds=60,
                    )
                    logger.info(
                        "[%s] Initialized DEX TWAP price provider for chain=%s (window=300s)",
                        self._backtest_id,
                        twap_chain,
                    )
                else:
                    logger.warning(
                        "[%s] Chain '%s' not supported by TWAP provider, will skip to next",
                        self._backtest_id,
                        self.config.chain,
                    )
            except Exception as e:
                # Use error handler for consistent classification
                if self._error_handler:
                    self._error_handler.handle_error(e, context="init_twap_provider")
                logger.warning(
                    "[%s] Failed to initialize TWAP provider: %s",
                    self._backtest_id,
                    str(e),
                )

    def _init_indicator_calculators(self) -> None:
        """Initialize indicator calculators (RSI, MACD, BB, ATR) using Binance OHLCV.

        Creates an RSICalculator backed by BinanceOHLCVProvider. The RSI calculator
        also exposes its OHLCV provider, which MarketSnapshot uses lazily for
        MACD, Bollinger Bands, ATR, SMA, and EMA calculations.
        """
        try:
            from almanak.framework.data.indicators.rsi import RSICalculator as RSICalc
            from almanak.framework.data.ohlcv.binance_provider import BinanceOHLCVProvider

            ohlcv_provider = BinanceOHLCVProvider(cache_ttl=120)
            self._rsi_calculator = RSICalc(ohlcv_provider=ohlcv_provider)
            logger.info(
                "[%s] Initialized indicator calculators (RSI, MACD, BB, ATR) via Binance OHLCV",
                self._backtest_id,
            )
        except Exception as e:
            logger.warning(
                "[%s] Failed to initialize indicator calculators: %s. "
                "Strategies using market.rsi()/macd()/bollinger_bands() will raise ValueError.",
                self._backtest_id,
                str(e),
            )
            self._rsi_calculator = None

    async def _get_token_price(self, token: str) -> Decimal:
        """Get token price in USD using the configured fallback chain.

        Implements a fallback chain for price sourcing. When price_source='auto',
        tries providers in order: Chainlink -> TWAP -> CoinGecko.

        Each provider in the chain is tried in sequence. If one fails or returns
        stale data, the next provider is attempted. Logs which provider was used
        for observability.

        Falls back to hardcoded prices if all providers fail (unless strict_price_mode=True).

        Args:
            token: Token symbol (e.g., 'ETH', 'WETH', 'USDC')

        Returns:
            Token price in USD as Decimal
        """
        token_upper = token.upper()

        # Check cache first
        if token_upper in self._price_cache:
            return self._price_cache[token_upper]

        # Stablecoins always return $1
        stables = {"USDC", "USDT", "DAI", "FRAX", "LUSD", "BUSD", "USD", "USDC.E"}
        if token_upper in stables:
            self._price_cache[token_upper] = Decimal("1")
            logger.debug("[%s] Price for %s: $1.00 (stablecoin)", self._backtest_id, token_upper)
            return Decimal("1")

        # Map token symbols to supported ones
        lookup_token = token_upper
        if lookup_token == "WETH":
            lookup_token = "ETH"

        # Try each provider in the fallback chain order
        provider_used: str | None = None
        price: Decimal | None = None

        for source in self._price_source_order:
            try:
                if source == "chainlink":
                    # Try Chainlink provider if available
                    if self._chainlink_provider is None:
                        logger.debug(
                            "[%s] Chainlink provider not initialized for %s, trying next",
                            self._backtest_id,
                            token_upper,
                        )
                        continue

                    try:
                        chainlink_price = await self._chainlink_provider.get_latest_price(
                            lookup_token, raise_on_stale=False
                        )
                        if chainlink_price is not None:
                            price = chainlink_price
                            provider_used = "chainlink"
                            logger.debug(
                                "[%s] Chainlink price for %s: $%s",
                                self._backtest_id,
                                token_upper,
                                price,
                            )
                            break
                        else:
                            logger.debug(
                                "[%s] Chainlink returned stale or unavailable data for %s, trying next",
                                self._backtest_id,
                                token_upper,
                            )
                            continue
                    except ChainlinkStaleDataError:
                        logger.debug(
                            "[%s] Chainlink data stale for %s, trying next provider",
                            self._backtest_id,
                            token_upper,
                        )
                        continue
                    except ValueError as e:
                        # Token not supported by Chainlink (no feed available)
                        logger.debug(
                            "[%s] Chainlink: %s, trying next provider",
                            self._backtest_id,
                            str(e),
                        )
                        continue

                elif source == "twap":
                    # Try TWAP provider if available
                    if self._twap_provider is None:
                        logger.debug(
                            "[%s] TWAP provider not initialized for %s, trying next",
                            self._backtest_id,
                            token_upper,
                        )
                        continue

                    try:
                        twap_result = await self._twap_provider.calculate_twap(
                            lookup_token, raise_on_low_liquidity=False
                        )
                        if twap_result is not None:
                            price = twap_result.price
                            if twap_result.is_low_liquidity:
                                provider_used = "twap (low_liquidity)"
                            else:
                                provider_used = "twap"
                            logger.debug(
                                "[%s] TWAP price for %s: $%s (tick=%d, window=%ds)",
                                self._backtest_id,
                                token_upper,
                                price,
                                twap_result.tick,
                                twap_result.window_seconds,
                            )
                            break
                        else:
                            logger.debug(
                                "[%s] TWAP calculation failed for %s, trying next provider",
                                self._backtest_id,
                                token_upper,
                            )
                            continue
                    except LowLiquidityWarning:
                        logger.debug(
                            "[%s] TWAP pool low liquidity for %s, trying next provider",
                            self._backtest_id,
                            token_upper,
                        )
                        continue
                    except ValueError as e:
                        # Token not supported by TWAP (no pool available)
                        logger.debug(
                            "[%s] TWAP: %s, trying next provider",
                            self._backtest_id,
                            str(e),
                        )
                        continue

                elif source == "coingecko":
                    if self._price_aggregator:
                        result = await self._price_aggregator.get_aggregated_price(lookup_token, "USD")
                        # Check if data is stale
                        if result.stale:
                            logger.warning(
                                "[%s] CoinGecko returned stale data for %s (confidence: %.2f)",
                                self._backtest_id,
                                token_upper,
                                result.confidence,
                            )
                            # Still use it if it's the last option
                            if source == self._price_source_order[-1]:
                                price = result.price
                                provider_used = "coingecko (stale)"
                            else:
                                continue
                        else:
                            price = result.price
                            provider_used = "coingecko"
                        break
            except AllDataSourcesFailed as e:
                # Use error handler for consistent classification
                if self._error_handler:
                    self._error_handler.handle_error(e, context=f"price_fetch:{source}:{token_upper}")
                logger.warning(
                    "[%s] Price provider '%s' failed for %s: %s",
                    self._backtest_id,
                    source,
                    token_upper,
                    str(e),
                )
                continue
            except Exception as e:
                # Use error handler for consistent classification
                if self._error_handler:
                    self._error_handler.handle_error(e, context=f"price_fetch:{source}:{token_upper}")
                logger.warning(
                    "[%s] Unexpected error from '%s' price provider for %s: %s",
                    self._backtest_id,
                    source,
                    token_upper,
                    str(e),
                )
                continue

        # If we got a price from a provider, cache and return it
        if price is not None and provider_used is not None:
            self._price_cache[token_upper] = price
            logger.info(
                "[%s] Price for %s: $%s (provider: %s)",
                self._backtest_id,
                token_upper,
                price,
                provider_used,
            )
            return price

        # All providers failed - check if strict price mode is enabled
        if self.config.strict_price_mode:
            # Strict mode: fail instead of using arbitrary prices
            error_msg = (
                f"All price providers failed for {token_upper} on chain={self.config.chain} "
                f"(chain_id={self.config.chain_id}) and strict_price_mode is enabled. "
                f"Providers attempted: chainlink, twap, coingecko. "
                f"Set strict_price_mode=False or ensure price providers can serve this token."
            )
            logger.error("[%s] %s", self._backtest_id, error_msg)
            raise ValueError(error_msg)

        # Fallback prices for common tokens when all providers fail
        logger.warning(
            "[%s] All price providers failed for %s on chain=%s, using hardcoded fallback. "
            "Set strict_price_mode=True for institutional-grade backtests.",
            self._backtest_id,
            token_upper,
            self.config.chain,
        )
        fallback_prices: dict[str, Decimal] = {
            "ETH": Decimal("3000"),
            "WETH": Decimal("3000"),
            "BTC": Decimal("60000"),
            "WBTC": Decimal("60000"),
            "ARB": Decimal("1"),
            "OP": Decimal("2"),
            "AVAX": Decimal("35"),
            "WAVAX": Decimal("35"),
            "LINK": Decimal("15"),
            "UNI": Decimal("10"),
        }

        price = fallback_prices.get(token_upper, Decimal("1"))
        self._price_cache[token_upper] = price
        logger.warning(
            "[%s] Price for %s: $%s (provider: hardcoded_fallback) - This may produce inaccurate backtest results",
            self._backtest_id,
            token_upper,
            price,
        )
        # Track that we used a hardcoded fallback for compliance reporting
        self._used_hardcoded_fallback = True
        self._track_fallback("hardcoded_price")
        return price

    def _get_token_price_sync(self, token: str) -> Decimal:
        """Get token price synchronously (for non-async contexts).

        Uses cached price if available (populated by async _get_token_price
        which implements the fallback chain), otherwise returns hardcoded fallback.
        This is useful for methods that cannot be async.

        Note: For best results, ensure _get_token_price has been called first
        to populate the cache with prices from the fallback chain.

        Args:
            token: Token symbol (e.g., 'ETH', 'WETH', 'USDC')

        Returns:
            Token price in USD as Decimal

        Raises:
            ValueError: If price not in cache, not a stablecoin, and
                strict_price_mode is True.
        """
        token_upper = token.upper()

        # Check cache first (populated by async _get_token_price with fallback chain)
        if token_upper in self._price_cache:
            return self._price_cache[token_upper]

        # Stablecoins always return $1
        stables = {"USDC", "USDT", "DAI", "FRAX", "LUSD", "BUSD", "USD", "USDC.E"}
        if token_upper in stables:
            return Decimal("1")

        # Check if strict price mode is enabled
        if self.config.strict_price_mode:
            # Strict mode: fail instead of using arbitrary prices
            error_msg = (
                f"Price for {token_upper} not in cache on chain={self.config.chain} "
                f"(chain_id={self.config.chain_id}) and strict_price_mode is enabled. "
                f"Ensure _get_token_price is called first to populate the cache, "
                f"or set strict_price_mode=False."
            )
            logger.error("[%s] %s", self._backtest_id, error_msg)
            raise ValueError(error_msg)

        # Fallback prices for common tokens (sync fallback when cache not populated)
        logger.warning(
            "[%s] Using hardcoded fallback price for %s on chain=%s in sync context. "
            "Set strict_price_mode=True for institutional-grade backtests.",
            self._backtest_id,
            token_upper,
            self.config.chain,
        )
        fallback_prices: dict[str, Decimal] = {
            "ETH": Decimal("3000"),
            "WETH": Decimal("3000"),
            "BTC": Decimal("60000"),
            "WBTC": Decimal("60000"),
            "ARB": Decimal("1"),
            "OP": Decimal("2"),
            "AVAX": Decimal("35"),
            "WAVAX": Decimal("35"),
            "LINK": Decimal("15"),
            "UNI": Decimal("10"),
        }

        price = fallback_prices.get(token_upper, Decimal("1"))
        # Track that we used a hardcoded fallback for compliance reporting
        self._used_hardcoded_fallback = True
        self._track_fallback("hardcoded_price")
        return price

    def _clear_price_cache(self) -> None:
        """Clear the price cache to force fresh fetches."""
        self._price_cache.clear()

    async def _get_portfolio_prices(self) -> dict[str, Decimal]:
        """Fetch prices for all tokens in the portfolio.

        Returns:
            Dict mapping token symbols to their USD prices
        """
        prices: dict[str, Decimal] = {}

        # Get all unique tokens from portfolio
        tokens_to_price = set()
        tokens_to_price.add("ETH")
        tokens_to_price.add("WETH")

        for token in self.portfolio_tracker.current_balances:
            tokens_to_price.add(token.upper())

        for token in self.config.initial_tokens:
            tokens_to_price.add(token.upper())

        # Fetch prices for each token
        for token in tokens_to_price:
            price = await self._get_token_price(token)
            prices[token] = price

        return prices

    # =========================================================================
    # Portfolio Value Helpers
    # =========================================================================

    def _calculate_initial_capital(self) -> Decimal:
        """Calculate initial capital from config balances.

        Uses cached token prices from the price provider.
        Falls back to sync price getter which uses cached or fallback values.

        Returns:
            Estimated initial capital in USD
        """
        initial = Decimal("0")

        # ETH value using price provider
        eth_price = self._get_token_price_sync("ETH")
        initial += self.config.initial_eth * eth_price

        # Token values from price provider
        for token, amount in self.config.initial_tokens.items():
            price = self._get_token_price_sync(token)
            initial += amount * price

        return initial

    def _calculate_portfolio_value(self) -> Decimal:
        """Calculate current portfolio value from tracker.

        Uses the portfolio tracker's current balances and the price provider.

        Returns:
            Current portfolio value in USD
        """
        total = Decimal("0")

        for token, amount in self.portfolio_tracker.current_balances.items():
            price = self._get_token_price_sync(token)
            total += amount * price

        return total

    # =========================================================================
    # Metrics Calculation
    # =========================================================================

    def _calculate_metrics(self) -> BacktestMetrics:
        """Calculate comprehensive backtest metrics.

        Returns:
            BacktestMetrics with all calculated performance metrics
        """
        if not self._equity_curve:
            return BacktestMetrics()

        # Extract equity values
        initial_capital = self._calculate_initial_capital()
        equity_values = [p.value_usd for p in self._equity_curve]
        initial_value = equity_values[0] if equity_values else initial_capital
        final_value = equity_values[-1] if equity_values else initial_capital

        # Total PnL
        total_pnl = final_value - initial_value

        # Execution costs from trades (all _trades are successful executions)
        total_fees = Decimal("0")  # Fees tracked in metadata if needed
        total_slippage = Decimal("0")  # Slippage would be calculated from receipts
        total_gas = sum((t.gas_cost_usd for t in self._trades), Decimal("0"))

        # Net PnL
        net_pnl = total_pnl

        # Total return
        total_return = Decimal("0")
        if initial_value > Decimal("0"):
            total_return = (final_value - initial_value) / initial_value

        # Calculate returns for risk metrics
        returns = self._calculate_returns(equity_values)

        # Volatility and Sharpe ratio (simplified)
        volatility = self._calculate_volatility(returns)
        sharpe = self._calculate_sharpe_ratio(returns, volatility)

        # Max drawdown
        max_drawdown = self._calculate_max_drawdown(equity_values)

        # Trade statistics using per-trade PnL from net_pnl_usd (includes gas costs)
        total_trades_count = len(self._trades)

        # Calculate win rate and profit factor from per-trade PnL (including gas)
        gross_profit = Decimal("0")
        gross_loss = Decimal("0")
        winning_trades_count = 0
        losing_trades_count = 0

        for trade in self._trades:
            trade_pnl = trade.net_pnl_usd
            if trade_pnl > Decimal("0"):
                gross_profit += trade_pnl
                winning_trades_count += 1
            elif trade_pnl < Decimal("0"):
                gross_loss += abs(trade_pnl)
                losing_trades_count += 1
            # Zero PnL trades are neutral - don't count as win or loss

        # Win rate = winning trades / total trades with non-zero PnL
        trades_with_pnl = winning_trades_count + losing_trades_count
        win_rate = Decimal("0")
        if trades_with_pnl > 0:
            win_rate = Decimal(winning_trades_count) / Decimal(trades_with_pnl)

        # Profit factor = gross profit / gross loss
        profit_factor = Decimal("0")
        if gross_loss > Decimal("0"):
            profit_factor = gross_profit / gross_loss

        return BacktestMetrics(
            total_pnl_usd=total_pnl,
            net_pnl_usd=net_pnl,
            sharpe_ratio=sharpe,
            max_drawdown_pct=max_drawdown,
            win_rate=win_rate,
            total_trades=total_trades_count,
            profit_factor=profit_factor,
            total_return_pct=total_return,
            total_fees_usd=total_fees,
            total_slippage_usd=total_slippage,
            total_gas_usd=total_gas,
            winning_trades=winning_trades_count,
            losing_trades=losing_trades_count,
            volatility=volatility,
        )

    def _calculate_returns(self, values: list[Decimal]) -> list[Decimal]:
        """Calculate period-over-period returns."""
        if len(values) < 2:
            return []

        returns: list[Decimal] = []
        for i in range(1, len(values)):
            if values[i - 1] > Decimal("0"):
                ret = (values[i] - values[i - 1]) / values[i - 1]
                returns.append(ret)
        return returns

    def _calculate_volatility(self, returns: list[Decimal]) -> Decimal:
        """Calculate volatility (standard deviation) of returns."""
        if len(returns) < 2:
            return Decimal("0")

        n = Decimal(str(len(returns)))
        mean = sum(returns, Decimal("0")) / n
        squared_diffs = sum((r - mean) ** 2 for r in returns)
        variance = squared_diffs / (n - Decimal("1"))

        return self._decimal_sqrt(variance)

    def _calculate_sharpe_ratio(
        self,
        returns: list[Decimal],
        volatility: Decimal,
    ) -> Decimal:
        """Calculate Sharpe ratio."""
        if volatility == Decimal("0") or not returns:
            return Decimal("0")

        n = Decimal(str(len(returns)))
        mean_return = sum(returns, Decimal("0")) / n

        # Annualize (assuming hourly returns for paper trading)
        annualized_return = mean_return * Decimal("8760")  # Hours per year
        annualized_vol = volatility * self._decimal_sqrt(Decimal("8760"))

        if annualized_vol == Decimal("0"):
            return Decimal("0")

        return annualized_return / annualized_vol

    def _calculate_max_drawdown(self, values: list[Decimal]) -> Decimal:
        """Calculate maximum drawdown."""
        if len(values) < 2:
            return Decimal("0")

        max_drawdown = Decimal("0")
        peak = values[0]

        for value in values:
            if value > peak:
                peak = value
            elif peak > Decimal("0"):
                drawdown = (peak - value) / peak
                if drawdown > max_drawdown:
                    max_drawdown = drawdown

        return max_drawdown

    def _decimal_sqrt(self, n: Decimal) -> Decimal:
        """Calculate square root of a Decimal using Newton's method."""
        if n < Decimal("0"):
            raise ValueError("Cannot compute sqrt of negative number")
        if n == Decimal("0"):
            return Decimal("0")

        x = n
        for _ in range(50):
            x_new = (x + n / x) / Decimal("2")
            if abs(x_new - x) < Decimal("1e-28"):
                break
            x = x_new
        return x


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "PaperTrader",
    "PaperTradeableStrategy",
    "PaperTradeEventType",
    "PaperTradeEventCallback",
    "create_market_snapshot_from_fork",
]
