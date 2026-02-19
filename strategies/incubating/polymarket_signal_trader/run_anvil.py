#!/usr/bin/env python3
"""
===============================================================================
TUTORIAL: Running Polymarket Signal Trader Strategy (Dry Run)
===============================================================================

This script demonstrates how to test a Polymarket strategy with mock signals.
Since Polymarket uses a CLOB (Central Limit Order Book) for order execution,
we can't fully test on Anvil like we do with on-chain protocols.

Instead, this script:
1. Creates a strategy instance with test configuration
2. Mocks external signals to trigger trading decisions
3. Calls strategy.decide() to verify intent generation
4. Does NOT execute real trades (dry-run mode)

For real testing:
- Use paper trading mode on Polymarket
- Test with small amounts first
- Verify API credentials work

USAGE:
------
    python strategies/demo/polymarket_signal_trader/run_anvil.py

    # With custom options:
    python strategies/demo/polymarket_signal_trader/run_anvil.py --signal bullish
    python strategies/demo/polymarket_signal_trader/run_anvil.py --signal bearish

===============================================================================
"""

import sys
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

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
    """Mock market snapshot for testing prediction strategies."""

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

    def prediction_position(self, market_id: str) -> dict[str, Any] | None:
        """Get current position in a market (mock returns None)."""
        return None


# =============================================================================
# MOCK SIGNAL PROVIDER
# =============================================================================


def create_mock_signal(direction: str, confidence: float = 0.8):
    """Create a mock signal for testing."""
    from almanak.framework.connectors.polymarket.signals import (
        SignalDirection,
        SignalResult,
    )

    direction_map = {
        "bullish": SignalDirection.BULLISH,
        "bearish": SignalDirection.BEARISH,
        "neutral": SignalDirection.NEUTRAL,
    }

    return SignalResult(
        direction=direction_map.get(direction.lower(), SignalDirection.NEUTRAL),
        confidence=confidence,
        source="mock_test",
        metadata={"test": True},
    )


# =============================================================================
# RUN STRATEGY
# =============================================================================


def run_strategy_dry_run(force_signal: str = "bullish"):
    """
    Run the PolymarketSignalTraderStrategy in dry-run mode.

    This demonstrates the complete decision flow:
    1. Create strategy instance with config
    2. Create market snapshot with mock data
    3. Patch signal provider to return forced signal
    4. Call strategy.decide() to get intent
    5. Print the resulting intent (no execution)

    Args:
        force_signal: "bullish", "bearish", or "neutral"
    """
    print(f"\n{'=' * 60}")
    print(f"RUNNING POLYMARKET SIGNAL TRADER (dry-run, signal: {force_signal})")
    print(f"{'=' * 60}")

    from almanak.framework.models.hot_reload_config import HotReloadableConfig

    # Import our strategy
    from strategies.demo.polymarket_signal_trader import PolymarketSignalTraderStrategy

    # =========================================================================
    # STEP 1: Create Strategy Instance
    # =========================================================================
    print("\n--- Step 1: Create Strategy ---")

    # Create configuration
    config = HotReloadableConfig(
        trade_size_usd=Decimal("10"),
        max_slippage=Decimal("0.01"),
    )
    # Add strategy-specific config
    config.market_id = "will-bitcoin-exceed-100000-by-2025"
    config.trade_size_usd = Decimal("10")
    config.min_confidence = Decimal("0.6")
    config.min_edge = Decimal("0.03")
    config.order_type = "market"
    config.stop_loss_pct = Decimal("0.20")
    config.take_profit_pct = Decimal("0.30")
    config.exit_before_resolution_hours = 24

    # Test wallet (Anvil default)
    test_wallet = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

    strategy = PolymarketSignalTraderStrategy(
        config=config,
        chain="polygon",
        wallet_address=test_wallet,
    )

    print(f"Strategy: {strategy.STRATEGY_NAME}")
    print(f"Market: {config.market_id}")
    print(f"Trade Size: ${config.trade_size_usd}")

    # =========================================================================
    # STEP 2: Create Market Snapshot
    # =========================================================================
    print("\n--- Step 2: Create Market Snapshot ---")

    market = MockMarketSnapshot(
        chain="polygon",
        wallet_address=test_wallet,
    )

    # Set mock market price (YES at 65%, NO at 35%)
    yes_price = Decimal("0.65")
    market.set_prediction_price(config.market_id, "YES", yes_price)
    market.set_prediction_price(config.market_id, "NO", Decimal("1") - yes_price)

    # Set mock USDC balance
    market.set_balance(
        "USDC",
        MockBalance(
            symbol="USDC",
            balance=Decimal("1000"),
            balance_usd=Decimal("1000"),
        ),
    )

    print(f"Market YES Price: ${yes_price:.3f}")
    print(f"Market NO Price: ${1 - yes_price:.3f}")
    print("USDC Balance: $1000.00")

    # =========================================================================
    # STEP 3: Patch Signal Provider
    # =========================================================================
    print("\n--- Step 3: Mock Signal ---")

    mock_signal = create_mock_signal(force_signal, confidence=0.75)
    print(f"Signal Direction: {mock_signal.direction.value}")
    print(f"Signal Confidence: {mock_signal.confidence:.2f}")

    # Patch the strategy's signal method
    strategy._get_aggregated_signal = lambda market_id: mock_signal

    # =========================================================================
    # STEP 4: Get Intent from Strategy
    # =========================================================================
    print("\n--- Step 4: Strategy Decision ---")

    intent = strategy.decide(market)

    if intent is None:
        print("Strategy returned None (hold)")
        return

    print(f"Intent Type: {intent.intent_type.value}")

    if intent.intent_type.value == "HOLD":
        reason = getattr(intent, "reason", "No reason provided")
        print(f"Reason: {reason}")
        print("\nStrategy decided to HOLD - no trade would be executed")
        return

    # Print intent details
    if hasattr(intent, "market_id"):
        print(f"Market ID: {intent.market_id}")
    if hasattr(intent, "outcome"):
        print(f"Outcome: {intent.outcome}")
    if hasattr(intent, "amount_usd"):
        print(f"Amount USD: ${intent.amount_usd}")
    if hasattr(intent, "shares"):
        print(f"Shares: {intent.shares}")
    if hasattr(intent, "order_type"):
        print(f"Order Type: {intent.order_type}")
    if hasattr(intent, "max_price") and intent.max_price:
        print(f"Max Price: ${intent.max_price}")
    if hasattr(intent, "exit_conditions") and intent.exit_conditions:
        ec = intent.exit_conditions
        print("Exit Conditions:")
        if ec.stop_loss_price:
            print(f"  - Stop Loss: ${ec.stop_loss_price}")
        if ec.take_profit_price:
            print(f"  - Take Profit: ${ec.take_profit_price}")
        if ec.exit_before_resolution_hours:
            print(f"  - Exit Before Resolution: {ec.exit_before_resolution_hours}h")

    # =========================================================================
    # STEP 5: Summary
    # =========================================================================
    print(f"\n{'=' * 60}")
    print("DRY-RUN COMPLETE")
    print(f"{'=' * 60}")
    print("\nIn production, this intent would:")
    if intent.intent_type.value == "PREDICTION_BUY":
        print(f"  1. Build a CLOB order for {intent.outcome} shares")
        print("  2. Sign the order with your wallet")
        print("  3. Submit to Polymarket CLOB API")
        print("  4. Wait for order to be filled")
        print("  5. Set up position monitoring with exit conditions")
    elif intent.intent_type.value == "PREDICTION_SELL":
        print(f"  1. Build a CLOB sell order for {intent.outcome} shares")
        print("  2. Sign the order with your wallet")
        print("  3. Submit to Polymarket CLOB API")
        print("  4. Wait for order to be filled")

    print(f"\n{'=' * 60}")
    print("POLYMARKET SIGNAL TRADER DEMO COMPLETE")
    print(f"{'=' * 60}\n")


# =============================================================================
# MAIN
# =============================================================================


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Run PolymarketSignalTraderStrategy in dry-run mode")
    parser.add_argument(
        "--signal",
        choices=["bullish", "bearish", "neutral"],
        default="bullish",
        help="Force a specific signal direction (default: bullish)",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("ALMANAK DEMO - POLYMARKET SIGNAL TRADER (DRY-RUN)")
    print("=" * 60)
    print("\nThis test runs the PolymarketSignalTraderStrategy in dry-run mode:")
    print("  1. Strategy.decide() -> returns Intent")
    print("  2. NO actual trades are executed")
    print("  3. This is for testing signal -> intent logic only")
    print(f"\nForced signal: {args.signal.upper()}")
    print("")

    try:
        run_strategy_dry_run(force_signal=args.signal)
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
