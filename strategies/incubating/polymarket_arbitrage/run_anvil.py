#!/usr/bin/env python3
"""
===============================================================================
TUTORIAL: Running Polymarket Arbitrage Strategy (Dry Run)
===============================================================================

This script demonstrates how to test a Polymarket arbitrage strategy with
mock market prices. Since Polymarket uses a CLOB for order execution,
we can't fully test on Anvil like we do with on-chain protocols.

Instead, this script:
1. Creates a strategy instance with test configuration
2. Mocks market prices to create arbitrage opportunities
3. Calls strategy.decide() to verify intent generation
4. Does NOT execute real trades (dry-run mode)

USAGE:
------
    python strategies/demo/polymarket_arbitrage/run_anvil.py

    # With custom options:
    python strategies/demo/polymarket_arbitrage/run_anvil.py --scenario overpriced
    python strategies/demo/polymarket_arbitrage/run_anvil.py --scenario underpriced
    python strategies/demo/polymarket_arbitrage/run_anvil.py --scenario fair

===============================================================================
"""

import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
from dotenv import load_dotenv  # noqa: E402

load_dotenv(project_root / ".env")


# =============================================================================
# MOCK MARKET SNAPSHOT
# =============================================================================


@dataclass
class MockBalance:
    """Mock balance for testing."""

    symbol: str
    balance: Decimal
    balance_usd: Decimal
    address: str = ""


class MockMarketSnapshot:
    """Mock market snapshot for testing arbitrage strategies."""

    def __init__(
        self,
        chain: str = "polygon",
        wallet_address: str = "0x742d35Cc6634C0532925a3b844Bc9e7595f8aA77",
    ):
        self.chain = chain
        self.wallet_address = wallet_address
        self._prices: dict[tuple[str, str], Decimal] = {}
        self._balances: dict[str, MockBalance] = {}

    def set_prediction_price(self, market_id: str, outcome: str, price: Decimal) -> None:
        """Set mock price for a market outcome."""
        self._prices[(market_id, outcome)] = price

    def prediction_price(self, market_id: str, outcome: str) -> Decimal | None:
        """Get prediction market price for an outcome."""
        return self._prices.get((market_id, outcome))

    def set_balance(self, token: str, balance: MockBalance) -> None:
        """Set mock balance for a token."""
        self._balances[token] = balance

    def balance(self, token: str) -> MockBalance:
        """Get token balance."""
        if token not in self._balances:
            raise KeyError(f"Balance not set for {token}")
        return self._balances[token]


# =============================================================================
# TEST SCENARIOS
# =============================================================================


def create_scenario(scenario: str) -> dict[str, Decimal]:
    """Create price scenarios for testing.

    For a pair of mutually exclusive markets (A happens, A doesn't happen),
    the sum of YES prices should equal 1.00.

    Scenarios:
    - fair: Prices sum to 1.00 (no arbitrage)
    - overpriced: Prices sum > 1.00 (sell opportunity)
    - underpriced: Prices sum < 1.00 (buy opportunity)
    """
    scenarios = {
        "fair": {
            "market-a-yes": Decimal("0.60"),
            "market-a-no": Decimal("0.40"),
        },
        "overpriced": {
            # Sum = 1.05 (5% overpriced)
            "market-a-yes": Decimal("0.62"),
            "market-a-no": Decimal("0.43"),
        },
        "underpriced": {
            # Sum = 0.95 (5% underpriced)
            "market-a-yes": Decimal("0.55"),
            "market-a-no": Decimal("0.40"),
        },
        "large_arb": {
            # Sum = 1.10 (10% overpriced - big opportunity)
            "market-a-yes": Decimal("0.65"),
            "market-a-no": Decimal("0.45"),
        },
    }
    return scenarios.get(scenario, scenarios["fair"])


# =============================================================================
# RUN STRATEGY
# =============================================================================


def run_strategy_dry_run(scenario: str = "overpriced"):
    """
    Run the PolymarketArbitrageStrategy in dry-run mode.

    This demonstrates the complete decision flow:
    1. Create strategy instance with config
    2. Create market snapshot with mock prices
    3. Call strategy.decide() to get intent
    4. Print the resulting intent (no execution)

    Args:
        scenario: "fair", "overpriced", "underpriced", or "large_arb"
    """
    print(f"\n{'=' * 60}")
    print(f"RUNNING POLYMARKET ARBITRAGE (dry-run, scenario: {scenario})")
    print(f"{'=' * 60}")

    from almanak.framework.models.hot_reload_config import HotReloadableConfig

    # Import our strategy
    from strategies.demo.polymarket_arbitrage import PolymarketArbitrageStrategy

    # =========================================================================
    # STEP 1: Create Strategy Instance
    # =========================================================================
    print("\n--- Step 1: Create Strategy ---")

    # Market pair to monitor (mutually exclusive outcomes)
    market_pair = ["market-a-yes", "market-a-no"]

    # Create configuration
    config = HotReloadableConfig(
        trade_size_usd=Decimal("10"),
        max_slippage=Decimal("0.01"),
    )
    # Add strategy-specific config
    config.market_pair = market_pair
    config.min_arb_pct = Decimal("0.02")  # 2% minimum arbitrage
    config.trade_size_usd = Decimal("10")
    config.max_exposure_usd = Decimal("100")
    config.order_type = "market"

    # Test wallet
    test_wallet = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

    strategy = PolymarketArbitrageStrategy(
        config=config,
        chain="polygon",
        wallet_address=test_wallet,
    )

    print(f"Strategy: {strategy.STRATEGY_NAME}")
    print(f"Market Pair: {market_pair}")
    print(f"Min Arb: {config.min_arb_pct:.1%}")
    print(f"Trade Size: ${config.trade_size_usd}")

    # =========================================================================
    # STEP 2: Create Market Snapshot
    # =========================================================================
    print("\n--- Step 2: Create Market Snapshot ---")

    market = MockMarketSnapshot(
        chain="polygon",
        wallet_address=test_wallet,
    )

    # Set mock prices based on scenario
    prices = create_scenario(scenario)
    for market_id, price in prices.items():
        market.set_prediction_price(market_id, "YES", price)
        market.set_prediction_price(market_id, "NO", Decimal("1") - price)

    # Set mock USDC balance
    market.set_balance(
        "USDC",
        MockBalance(
            symbol="USDC",
            balance=Decimal("1000"),
            balance_usd=Decimal("1000"),
        ),
    )

    # Print prices
    price_sum = sum(prices.values())
    print(f"\nMarket Prices (scenario: {scenario}):")
    for market_id, price in prices.items():
        print(f"  {market_id} YES: ${price:.3f}")

    print(f"\nPrice Sum: ${price_sum:.4f}")
    print("Fair Sum: $1.0000")
    print(f"Arbitrage: {abs(price_sum - 1):.2%}")
    print("USDC Balance: $1000.00")

    # =========================================================================
    # STEP 3: Get Intent from Strategy
    # =========================================================================
    print("\n--- Step 3: Strategy Decision ---")

    intent = strategy.decide(market)

    if intent is None:
        print("Strategy returned None (hold)")
        return

    print(f"Intent Type: {intent.intent_type.value}")

    if intent.intent_type.value == "HOLD":
        reason = getattr(intent, "reason", "No reason provided")
        print(f"Reason: {reason}")
        print("\nStrategy decided to HOLD - no arbitrage opportunity detected")
        return

    # Print intent details
    if hasattr(intent, "market_id"):
        print(f"Market ID: {intent.market_id}")
    if hasattr(intent, "outcome"):
        print(f"Outcome: {intent.outcome}")
    if hasattr(intent, "amount_usd"):
        print(f"Amount USD: ${intent.amount_usd}")
    if hasattr(intent, "order_type"):
        print(f"Order Type: {intent.order_type}")
    if hasattr(intent, "max_price") and intent.max_price:
        print(f"Max Price: ${intent.max_price}")

    # =========================================================================
    # STEP 4: Explain the Arbitrage
    # =========================================================================
    print(f"\n{'=' * 60}")
    print("ARBITRAGE EXPLANATION")
    print(f"{'=' * 60}")

    if price_sum > 1:
        print(f"\nPrice sum (${price_sum:.4f}) > $1.00 = Markets are OVERPRICED")
        print(f"Profit opportunity: {(price_sum - 1):.2%}")
        print("\nStrategy: Buy NO on the most overpriced market")
        print("  - NO shares will increase in value as prices correct")
        print("  - If market stays overpriced, NO still pays out eventually")
    elif price_sum < 1:
        print(f"\nPrice sum (${price_sum:.4f}) < $1.00 = Markets are UNDERPRICED")
        print(f"Profit opportunity: {(1 - price_sum):.2%}")
        print("\nStrategy: Buy YES on the most underpriced market")
        print("  - YES shares will increase in value as prices correct")
        print("  - Buying underpriced probability is +EV")
    else:
        print("\nPrices are fairly priced (sum = $1.00)")
        print("No arbitrage opportunity")

    # =========================================================================
    # STEP 5: Summary
    # =========================================================================
    print(f"\n{'=' * 60}")
    print("DRY-RUN COMPLETE")
    print(f"{'=' * 60}")
    print("\nIn production, this intent would:")
    print(f"  1. Build a CLOB order for {intent.outcome} shares on {intent.market_id}")
    print("  2. Sign the order with your wallet")
    print("  3. Submit to Polymarket CLOB API")
    print("  4. Wait for order to be filled")
    print("  5. Monitor for price convergence or market resolution")

    print(f"\n{'=' * 60}")
    print("POLYMARKET ARBITRAGE DEMO COMPLETE")
    print(f"{'=' * 60}\n")


# =============================================================================
# MAIN
# =============================================================================


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Run PolymarketArbitrageStrategy in dry-run mode")
    parser.add_argument(
        "--scenario",
        choices=["fair", "overpriced", "underpriced", "large_arb"],
        default="overpriced",
        help="Price scenario to simulate (default: overpriced)",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("ALMANAK DEMO - POLYMARKET ARBITRAGE (DRY-RUN)")
    print("=" * 60)
    print("\nThis test runs the PolymarketArbitrageStrategy in dry-run mode:")
    print("  1. Strategy.decide() -> returns Intent")
    print("  2. NO actual trades are executed")
    print("  3. This is for testing arbitrage detection logic only")
    print(f"\nPrice scenario: {args.scenario.upper()}")
    print("")

    try:
        run_strategy_dry_run(scenario=args.scenario)
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
