#!/usr/bin/env python3
"""
===============================================================================
ALMANAK SDK - Data Module Showcase Demo
===============================================================================

This demo showcases ALL data fetching capabilities of the Almanak SDK.
It demonstrates how to fetch prices, balances, technical indicators,
funding rates, lending rates, and more.

REQUIREMENTS:
    1. Gateway running: `almanak gateway`
    2. Anvil fork running (for balance queries): `anvil --fork-url <RPC_URL>`

USAGE:
    # Full demo (requires gateway + anvil)
    python examples/demo_data_module.py

    # Indicators only (uses CoinGecko, no gateway needed)
    python examples/demo_data_module.py --indicators-only

    # Skip slow operations
    python examples/demo_data_module.py --quick

ENVIRONMENT:
    - ALCHEMY_API_KEY: Required for RPC operations
    - ALMANAK_PRIVATE_KEY: Optional, for wallet-specific queries

===============================================================================
"""

import argparse
import asyncio
import functools
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION
# =============================================================================


@dataclass
class DemoConfig:
    """Configuration for the demo."""

    # Chains to test
    chains: list[str]
    # Tokens to fetch prices for
    tokens: list[str]
    # Wallet address for balance queries
    wallet_address: str
    # Run quick mode (skip slow operations)
    quick_mode: bool
    # Indicators only (no gateway required)
    indicators_only: bool


DEFAULT_CONFIG = DemoConfig(
    chains=["arbitrum", "ethereum", "base"],
    tokens=["ETH", "WETH", "USDC", "USDT", "ARB", "BTC", "LINK"],
    wallet_address="0x742d35Cc6634C0532925a3b844Bc9e7595f9fE41",  # Example address
    quick_mode=False,
    indicators_only=False,
)


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================


def print_header(title: str) -> None:
    """Print a formatted section header."""
    width = 70
    print("\n" + "=" * width)
    print(f" {title}")
    print("=" * width)


def print_subheader(title: str) -> None:
    """Print a formatted subsection header."""
    print(f"\n--- {title} ---")


def timed_operation(operation_name: str):
    """Decorator to time and log operations."""

    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                result = await func(*args, **kwargs)
                elapsed = (time.perf_counter() - start) * 1000
                print(f"  [OK] {operation_name} completed in {elapsed:.1f}ms")
                return result
            except Exception as e:
                elapsed = (time.perf_counter() - start) * 1000
                print(f"  [FAIL] {operation_name} failed in {elapsed:.1f}ms: {e}")
                return None

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                elapsed = (time.perf_counter() - start) * 1000
                print(f"  [OK] {operation_name} completed in {elapsed:.1f}ms")
                return result
            except Exception as e:
                elapsed = (time.perf_counter() - start) * 1000
                print(f"  [FAIL] {operation_name} failed in {elapsed:.1f}ms: {e}")
                return None

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator


def format_decimal(value: Decimal | float | None, decimals: int = 4) -> str:
    """Format a decimal value for display."""
    if value is None:
        return "N/A"
    if isinstance(value, Decimal):
        return f"{float(value):,.{decimals}f}"
    return f"{value:,.{decimals}f}"


def format_usd(value: Decimal | float | None) -> str:
    """Format a USD value for display."""
    if value is None:
        return "N/A"
    if isinstance(value, Decimal):
        value = float(value)
    return f"${value:,.2f}"


# =============================================================================
# DEMO SECTIONS
# =============================================================================


class DataModuleDemo:
    """Comprehensive demo of the Almanak data module."""

    def __init__(self, config: DemoConfig):
        self.config = config
        self.results: dict[str, Any] = {}

    async def run(self) -> None:
        """Run the complete demo."""
        print_header("ALMANAK SDK - DATA MODULE SHOWCASE")
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Mode: {'Indicators Only' if self.config.indicators_only else 'Full Demo'}")
        print(f"Quick Mode: {self.config.quick_mode}")

        total_start = time.perf_counter()

        # Always run indicators (no gateway needed)
        await self.demo_technical_indicators()

        if not self.config.indicators_only:
            # These require gateway to be running
            await self.demo_token_prices()
            await self.demo_wallet_balances()
            await self.demo_ohlcv_data()
            await self.demo_funding_rates()
            await self.demo_lending_rates()
            await self.demo_pool_info()
            if not self.config.quick_mode:
                await self.demo_prediction_markets()

        # Summary
        total_elapsed = time.perf_counter() - total_start
        self.print_summary(total_elapsed)

    # -------------------------------------------------------------------------
    # 1. TOKEN PRICES
    # -------------------------------------------------------------------------

    async def demo_token_prices(self) -> None:
        """Demonstrate fetching token prices from multiple sources."""
        print_header("1. TOKEN PRICES")
        print("Fetching prices from CoinGecko via Gateway...")

        try:
            from almanak.framework.data.indicators import CoinGeckoOHLCVProvider

            provider = CoinGeckoOHLCVProvider()
            try:
                prices = {}
                for token in self.config.tokens:
                    start = time.perf_counter()
                    try:
                        # CoinGecko provider gives OHLCV, we take latest close as price
                        ohlcv = await provider.get_ohlcv(token, "USD", timeframe="1h", limit=1)
                        if ohlcv:
                            price = float(ohlcv[-1].close)
                            prices[token] = price
                            elapsed = (time.perf_counter() - start) * 1000
                            print(f"  {token:6s}: {format_usd(price):>12s}  ({elapsed:.0f}ms)")
                    except Exception as e:
                        print(f"  {token:6s}: ERROR - {e}")

                self.results["prices"] = prices
            finally:
                await provider.close()

        except ImportError as e:
            print(f"  [SKIP] CoinGecko provider not available: {e}")

    # -------------------------------------------------------------------------
    # 2. WALLET BALANCES
    # -------------------------------------------------------------------------

    async def demo_wallet_balances(self) -> None:
        """Demonstrate fetching wallet balances."""
        print_header("2. WALLET BALANCES")
        print(f"Wallet: {self.config.wallet_address[:10]}...{self.config.wallet_address[-6:]}")
        print("Requires: Gateway + Anvil fork running")

        try:
            from almanak.framework.data.balance import GatewayBalanceProvider
            from almanak.framework.gateway_client import GatewayClient

            # Connect to gateway and create provider
            async with GatewayClient() as client:
                provider = GatewayBalanceProvider(
                    client=client,
                    wallet_address=self.config.wallet_address,
                )

                balances = {}
                tokens_to_check = ["ETH", "WETH", "USDC", "USDT"]

                for token in tokens_to_check:
                    start = time.perf_counter()
                    try:
                        result = await provider.get_balance(token)
                        balances[token] = result
                        elapsed = (time.perf_counter() - start) * 1000
                        print(f"  {token:6s}: {format_decimal(result.balance, 6):>14s} ({elapsed:.0f}ms)")
                    except Exception as e:
                        print(f"  {token:6s}: ERROR - {e}")

                self.results["balances"] = balances

        except ImportError as e:
            print(f"  [SKIP] Balance provider not available: {e}")
        except Exception as e:
            print(f"  [SKIP] Could not connect to gateway: {e}")
            print("  Hint: Make sure 'almanak gateway' is running")

    # -------------------------------------------------------------------------
    # 3. TECHNICAL INDICATORS (RSI, MACD, Bollinger Bands, etc.)
    # -------------------------------------------------------------------------

    async def demo_technical_indicators(self) -> None:
        """Demonstrate all technical indicators."""
        print_header("3. TECHNICAL ANALYSIS INDICATORS")
        print("Data source: CoinGecko OHLCV (no gateway required)")

        try:
            from almanak.framework.data.indicators import (
                ATRCalculator,
                BollingerBandsCalculator,
                CoinGeckoOHLCVProvider,
                MACDCalculator,
                MovingAverageCalculator,
                RSICalculator,
                StochasticCalculator,
            )

            # Create OHLCV provider
            ohlcv_provider = CoinGeckoOHLCVProvider()

            try:
                # Test tokens
                test_tokens = ["ETH", "WBTC"]  # Using WBTC instead of BTC (BTC not in token registry)

                for token in test_tokens:
                    print_subheader(f"{token} Indicators")

                    # ----- RSI -----
                    print("\n  RSI (Relative Strength Index):")
                    rsi_calc = RSICalculator(ohlcv_provider=ohlcv_provider)
                    for timeframe in ["1h", "4h"]:
                        start = time.perf_counter()
                        try:
                            rsi = await rsi_calc.calculate_rsi(token, period=14, timeframe=timeframe)
                            elapsed = (time.perf_counter() - start) * 1000
                            signal = "OVERSOLD" if rsi < 30 else "OVERBOUGHT" if rsi > 70 else "NEUTRAL"
                            print(f"    RSI(14, {timeframe}): {rsi:6.2f}  [{signal}]  ({elapsed:.0f}ms)")
                        except Exception as e:
                            print(f"    RSI(14, {timeframe}): ERROR - {e}")

                    # ----- Moving Averages -----
                    print("\n  Moving Averages:")
                    ma_calc = MovingAverageCalculator(ohlcv_provider=ohlcv_provider)

                    # SMA
                    for period in [20, 50]:
                        start = time.perf_counter()
                        try:
                            sma = await ma_calc.sma(token, period=period, timeframe="1h")
                            elapsed = (time.perf_counter() - start) * 1000
                            print(f"    SMA({period}):  {format_usd(sma):>12s}  ({elapsed:.0f}ms)")
                        except Exception as e:
                            print(f"    SMA({period}):  ERROR - {e}")

                    # EMA
                    for period in [12, 26]:
                        start = time.perf_counter()
                        try:
                            ema = await ma_calc.ema(token, period=period, timeframe="1h")
                            elapsed = (time.perf_counter() - start) * 1000
                            print(f"    EMA({period}):  {format_usd(ema):>12s}  ({elapsed:.0f}ms)")
                        except Exception as e:
                            print(f"    EMA({period}):  ERROR - {e}")

                    # ----- Bollinger Bands -----
                    print("\n  Bollinger Bands (20, 2.0):")
                    bb_calc = BollingerBandsCalculator(ohlcv_provider=ohlcv_provider)
                    start = time.perf_counter()
                    try:
                        bb = await bb_calc.calculate_bollinger_bands(token, period=20, std_dev=2.0, timeframe="1h")
                        elapsed = (time.perf_counter() - start) * 1000
                        print(f"    Upper Band:   {format_usd(bb.upper_band)}")
                        print(f"    Middle Band:  {format_usd(bb.middle_band)}")
                        print(f"    Lower Band:   {format_usd(bb.lower_band)}")
                        print(f"    Bandwidth:    {bb.bandwidth:.4f}")
                        print(f"    %B:           {bb.percent_b:.4f}")
                        position = "ABOVE UPPER" if bb.percent_b > 1 else "BELOW LOWER" if bb.percent_b < 0 else "IN BANDS"
                        print(f"    Position:     [{position}]  ({elapsed:.0f}ms)")
                    except Exception as e:
                        print(f"    ERROR - {e}")

                    # ----- MACD -----
                    print("\n  MACD (12, 26, 9):")
                    macd_calc = MACDCalculator(ohlcv_provider=ohlcv_provider)
                    start = time.perf_counter()
                    try:
                        macd = await macd_calc.calculate_macd(token, timeframe="1h")
                        elapsed = (time.perf_counter() - start) * 1000
                        signal = "BULLISH" if macd.histogram > 0 else "BEARISH"
                        print(f"    MACD Line:    {macd.macd_line:>12.4f}")
                        print(f"    Signal Line:  {macd.signal_line:>12.4f}")
                        print(f"    Histogram:    {macd.histogram:>12.4f}  [{signal}]  ({elapsed:.0f}ms)")
                    except Exception as e:
                        print(f"    ERROR - {e}")

                    # ----- Stochastic -----
                    print("\n  Stochastic Oscillator (14, 3):")
                    stoch_calc = StochasticCalculator(ohlcv_provider=ohlcv_provider)
                    start = time.perf_counter()
                    try:
                        stoch = await stoch_calc.calculate_stochastic(token, k_period=14, d_period=3, timeframe="1h")
                        elapsed = (time.perf_counter() - start) * 1000
                        signal = "OVERSOLD" if stoch.k_value < 20 else "OVERBOUGHT" if stoch.k_value > 80 else "NEUTRAL"
                        print(f"    %K:           {stoch.k_value:>12.2f}")
                        print(f"    %D:           {stoch.d_value:>12.2f}  [{signal}]  ({elapsed:.0f}ms)")
                    except Exception as e:
                        print(f"    ERROR - {e}")

                    # ----- ATR -----
                    print("\n  ATR (Average True Range, 14):")
                    atr_calc = ATRCalculator(ohlcv_provider=ohlcv_provider)
                    start = time.perf_counter()
                    try:
                        atr = await atr_calc.calculate_atr(token, period=14, timeframe="1h")
                        elapsed = (time.perf_counter() - start) * 1000
                        print(f"    ATR(14):      {format_usd(atr):>12s}  ({elapsed:.0f}ms)")
                    except Exception as e:
                        print(f"    ERROR - {e}")

                self.results["indicators"] = {"completed": True}
            finally:
                # Clean up HTTP session
                await ohlcv_provider.close()

        except ImportError as e:
            print(f"  [SKIP] Indicators not available: {e}")

    # -------------------------------------------------------------------------
    # 4. OHLCV (Candlestick) DATA
    # -------------------------------------------------------------------------

    async def demo_ohlcv_data(self) -> None:
        """Demonstrate fetching OHLCV candlestick data."""
        print_header("4. OHLCV CANDLESTICK DATA")
        print("Fetching historical candlestick data...")

        try:
            from almanak.framework.data.ohlcv import BinanceOHLCVProvider

            provider = BinanceOHLCVProvider()
            try:
                test_cases = [
                    ("ETH", "1h", 5),
                    ("BTC", "4h", 5),
                ]

                for token, timeframe, limit in test_cases:
                    print_subheader(f"{token} - {timeframe} candles (last {limit})")
                    start = time.perf_counter()
                    try:
                        candles = await provider.get_ohlcv(token, "USD", timeframe=timeframe, limit=limit)
                        elapsed = (time.perf_counter() - start) * 1000

                        print(f"  {'Timestamp':<20} {'Open':>12} {'High':>12} {'Low':>12} {'Close':>12}")
                        print("  " + "-" * 72)
                        for candle in candles[-5:]:
                            ts = candle.timestamp.strftime("%Y-%m-%d %H:%M")
                            print(
                                f"  {ts:<20} {format_usd(candle.open):>12} "
                                f"{format_usd(candle.high):>12} {format_usd(candle.low):>12} "
                                f"{format_usd(candle.close):>12}"
                            )
                        print(f"  Fetched {len(candles)} candles in {elapsed:.0f}ms")
                    except Exception as e:
                        print(f"  ERROR - {e}")

                self.results["ohlcv"] = {"completed": True}
            finally:
                await provider.close()

        except ImportError as e:
            print(f"  [SKIP] OHLCV provider not available: {e}")

    # -------------------------------------------------------------------------
    # 5. FUNDING RATES (Perpetuals)
    # -------------------------------------------------------------------------

    async def demo_funding_rates(self) -> None:
        """Demonstrate fetching perp funding rates."""
        print_header("5. PERPETUAL FUNDING RATES")
        print("Venues: GMX V2, Hyperliquid")

        try:
            from almanak.framework.data.funding import (
                SUPPORTED_MARKETS,
                FundingRateProvider,
                Venue,
            )

            provider = FundingRateProvider()
            try:
                markets = ["ETH-USD", "BTC-USD"]
                venues = [Venue.GMX_V2, Venue.HYPERLIQUID]

                print_subheader("Current Funding Rates")
                print(f"  {'Market':<12} {'Venue':<12} {'Rate (8h)':>12} {'APY':>12}")
                print("  " + "-" * 52)

                for market in markets:
                    for venue in venues:
                        if market in SUPPORTED_MARKETS.get(venue, []):
                            start = time.perf_counter()
                            try:
                                rate = await provider.get_funding_rate(venue, market)
                                elapsed = (time.perf_counter() - start) * 1000
                                rate_8h = f"{rate.rate_percent_8h:+.4f}%"
                                apy = f"{rate.rate_percent_8h * 3 * 365:+.2f}%"
                                print(f"  {market:<12} {venue.value:<12} {rate_8h:>12} {apy:>12}  ({elapsed:.0f}ms)")
                            except Exception as e:
                                print(f"  {market:<12} {venue.value:<12} ERROR: {e}")

                # Funding rate spread
                print_subheader("Cross-Venue Spread Analysis")
                try:
                    spread = await provider.get_funding_rate_spread("ETH-USD", Venue.GMX_V2, Venue.HYPERLIQUID)
                    print("  ETH-USD GMX vs Hyperliquid:")
                    print(f"    Spread (8h):    {spread.spread_percent_8h:+.4f}%")
                    print(f"    Is Profitable:  {spread.is_profitable}")
                    if spread.recommended_direction:
                        print(f"    Recommended:    {spread.recommended_direction}")
                except Exception as e:
                    print(f"  Spread calculation error: {e}")

                self.results["funding_rates"] = {"completed": True}
            finally:
                await provider.close()

        except ImportError as e:
            print(f"  [SKIP] Funding rate provider not available: {e}")

    # -------------------------------------------------------------------------
    # 6. LENDING RATES
    # -------------------------------------------------------------------------

    async def demo_lending_rates(self) -> None:
        """Demonstrate fetching lending protocol rates."""
        print_header("6. LENDING PROTOCOL RATES")
        print("Protocols: Aave V3, Morpho, Compound V3")

        try:
            from almanak.framework.data.rates import (
                SUPPORTED_PROTOCOLS,
                SUPPORTED_TOKENS,
            )

            print(f"  Supported protocols: {', '.join(SUPPORTED_PROTOCOLS)}")
            print(f"  Supported tokens: {', '.join(list(SUPPORTED_TOKENS)[:5])}...")

            # Note: RateMonitor requires gateway connection
            print("\n  [INFO] Lending rates require gateway connection")
            print("  Run 'almanak gateway' to fetch live rates")

            # Show supported configuration
            print_subheader("Available Rate Queries")
            print("  Supply rates: Get APY for supplying assets")
            print("  Borrow rates: Get APY for borrowing assets")
            print("  Best rates: Compare across protocols")

            self.results["lending_rates"] = {"completed": True}

        except ImportError as e:
            print(f"  [SKIP] Rate monitor not available: {e}")

    # -------------------------------------------------------------------------
    # 7. DEX POOL INFO
    # -------------------------------------------------------------------------

    async def demo_pool_info(self) -> None:
        """Demonstrate fetching DEX pool information."""
        print_header("7. DEX POOL INFORMATION")
        print("Supported: Uniswap V2, Uniswap V3, SushiSwap")

        try:
            from almanak.framework.data.defi.pools import (
                VALID_DEX_TYPES,
            )

            print(f"  Supported DEX types: {', '.join(VALID_DEX_TYPES)}")

            # Example pool addresses
            example_pools = {
                "USDC/WETH 0.05%": "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
                "USDC/WETH 0.3%": "0x8ad599c3A0ff1De082011EFDDc58f1908eb6e6D8",
            }

            print_subheader("Pool Data Structure")
            print("  PoolReserves contains:")
            print("    - pool_address: Contract address")
            print("    - dex: Protocol type (uniswap_v2, uniswap_v3, sushiswap)")
            print("    - token0, token1: Pool tokens with metadata")
            print("    - reserve0, reserve1: Token reserves")
            print("    - fee_tier: Pool fee in basis points")
            print("    - sqrt_price_x96: V3 price (Q64.96 format)")
            print("    - tick: V3 current tick")
            print("    - liquidity: V3 in-range liquidity")
            print("    - tvl_usd: Total value locked")

            print_subheader("Example Pool Addresses (Ethereum)")
            for name, address in example_pools.items():
                print(f"  {name}: {address[:20]}...")

            self.results["pool_info"] = {"completed": True}

        except ImportError as e:
            print(f"  [SKIP] Pool reader not available: {e}")

    # -------------------------------------------------------------------------
    # 8. PREDICTION MARKETS
    # -------------------------------------------------------------------------

    async def demo_prediction_markets(self) -> None:
        """Demonstrate fetching prediction market data."""
        print_header("8. PREDICTION MARKET DATA (Polymarket)")
        print("Provider: Polymarket CLOB")

        try:
            # Import to verify availability (documented in output)
            import almanak.framework.data.prediction_provider

            print_subheader("PredictionMarket Data Structure")
            print("  - market_id: Internal market ID")
            print("  - condition_id: CTF condition ID")
            print("  - question: Market question text")
            print("  - yes_price, no_price: Current outcome prices (0-1)")
            print("  - yes_token_id, no_token_id: CLOB token IDs")
            print("  - spread: Bid-ask spread")
            print("  - volume_24h: 24-hour trading volume")
            print("  - liquidity: Current liquidity")
            print("  - is_active, is_resolved: Market status")

            print_subheader("Available Operations")
            print("  - get_market(market_id): Fetch single market")
            print("  - get_markets(filters): Search markets")
            print("  - get_positions(wallet): Get wallet positions")
            print("  - get_orderbook(token_id): Get market depth")
            print("  - get_related_markets(market_id): Find correlated markets")

            print("\n  [INFO] Live Polymarket data requires API credentials")
            print("  Set POLYMARKET_API_KEY for real market data")

            self.results["prediction_markets"] = {"completed": True}

        except ImportError as e:
            print(f"  [SKIP] Prediction provider not available: {e}")

    # -------------------------------------------------------------------------
    # SUMMARY
    # -------------------------------------------------------------------------

    def print_summary(self, total_time: float) -> None:
        """Print demo summary."""
        print_header("DEMO SUMMARY")

        completed = sum(1 for v in self.results.values() if v is not None)
        total = 8 if not self.config.indicators_only else 1

        print(f"  Sections completed: {completed}/{total}")
        print(f"  Total time: {total_time:.2f}s")

        print_subheader("Data Module Capabilities")
        print("""
  The Almanak Data Module provides:

  1. PRICE DATA
     - Multi-source aggregation (CoinGecko, Chainlink, DEX)
     - Stale data detection and fallbacks
     - Stablecoin pricing modes (market, pegged, hybrid)

  2. BALANCE QUERIES
     - ERC-20 and native token balances
     - Automatic decimal conversion
     - Cache invalidation after transactions

  3. TECHNICAL INDICATORS
     - RSI (Relative Strength Index)
     - SMA/EMA (Moving Averages)
     - Bollinger Bands
     - MACD (Moving Average Convergence Divergence)
     - Stochastic Oscillator
     - ATR (Average True Range)

  4. OHLCV DATA
     - Multiple timeframes (1m, 5m, 15m, 1h, 4h, 1d)
     - Binance and CoinGecko providers
     - SQLite caching for efficiency

  5. FUNDING RATES
     - GMX V2, Hyperliquid venues
     - Cross-venue spread analysis
     - Historical rate data

  6. LENDING RATES
     - Aave V3, Morpho, Compound V3
     - Supply and borrow APY
     - Best rate comparison

  7. DEX POOL DATA
     - Uniswap V2/V3, SushiSwap
     - Reserve data and TVL
     - V3 tick and liquidity data

  8. PREDICTION MARKETS
     - Polymarket integration
     - Position tracking
     - Orderbook data
""")

        print_subheader("Next Steps")
        print("  - Run with --indicators-only for quick TA demo")
        print("  - Start gateway for full functionality: almanak gateway")
        print("  - Check strategies/demo/ for usage examples")
        print("  - Read blueprints/01-data-layer.md for architecture")


# =============================================================================
# MAIN
# =============================================================================


def parse_args() -> DemoConfig:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Almanak SDK Data Module Showcase Demo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python examples/demo_data_module.py                    # Full demo
  python examples/demo_data_module.py --indicators-only  # TA indicators only
  python examples/demo_data_module.py --quick            # Skip slow operations
        """,
    )
    parser.add_argument(
        "--indicators-only",
        action="store_true",
        help="Only run technical indicators (no gateway required)",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Quick mode - skip slow operations",
    )
    parser.add_argument(
        "--wallet",
        type=str,
        default=DEFAULT_CONFIG.wallet_address,
        help="Wallet address for balance queries",
    )

    args = parser.parse_args()

    return DemoConfig(
        chains=DEFAULT_CONFIG.chains,
        tokens=DEFAULT_CONFIG.tokens,
        wallet_address=args.wallet,
        quick_mode=args.quick,
        indicators_only=args.indicators_only,
    )


async def main() -> None:
    """Main entry point."""
    config = parse_args()
    demo = DataModuleDemo(config)
    await demo.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\nDemo failed with error: {e}")
        sys.exit(1)
