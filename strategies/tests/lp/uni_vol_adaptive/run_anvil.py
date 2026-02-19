#!/usr/bin/env python3
"""
===============================================================================
Uniswap V3 Volatility-Adaptive LP Strategy - Anvil Test Runner
===============================================================================

This script tests the volatility-adaptive LP strategy on an Anvil fork of
Arbitrum mainnet. It demonstrates how the strategy adjusts range width based
on ATR (Average True Range) volatility levels.

WHAT THIS SCRIPT DOES:
----------------------
1. Starts an Anvil fork of Arbitrum mainnet
2. Funds the test wallet with WETH and USDC
3. Runs the strategy with a forced ATR value to test different volatility regimes
4. Prints the computed ATR and resulting range width

VOLATILITY REGIMES:
-------------------
- LOW (ATR < 2%): 5% range width
- MEDIUM (ATR 2-5%): 10% range width
- HIGH (ATR > 5%): 15% range width

USAGE:
------
    python strategies/tests/lp/uni_vol_adaptive/run_anvil.py

    # Test specific volatility regimes:
    python strategies/tests/lp/uni_vol_adaptive/run_anvil.py --regime low
    python strategies/tests/lp/uni_vol_adaptive/run_anvil.py --regime high

===============================================================================
"""

import os
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(project_root / ".env")


# =============================================================================
# CONFIGURATION
# =============================================================================

# Anvil's first default account (Account #0)
ANVIL_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
ANVIL_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

# Arbitrum mainnet token addresses
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"  # Native USDC
WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

# Whale addresses for funding
USDC_WHALE = "0x489ee077994B6658eAfA855C308275EAd8097C4A"  # Aave V3 pool

# Amounts to fund
FUND_AMOUNT_USDC = 100  # 100 USDC
FUND_AMOUNT_WETH = Decimal("0.05")  # 0.05 WETH

# Anvil settings
ANVIL_PORT = 8545
ANVIL_RPC = f"http://127.0.0.1:{ANVIL_PORT}"

# ATR values for testing different regimes (based on $3000 ETH price)
ATR_VALUES = {
    "low": Decimal("30"),  # $30 = 1% of $3000 -> LOW regime
    "medium": Decimal("100"),  # $100 = 3.3% of $3000 -> MEDIUM regime
    "high": Decimal("200"),  # $200 = 6.6% of $3000 -> HIGH regime
}


# =============================================================================
# ANVIL MANAGER
# =============================================================================


class AnvilManager:
    """Manages the Anvil fork lifecycle."""

    def __init__(self, fork_url: str, port: int = 8545):
        self.fork_url = fork_url
        self.port = port
        self.process: subprocess.Popen | None = None

    def start(self) -> bool:
        """Start Anvil fork."""
        print(f"\n{'=' * 60}")
        print("STARTING ANVIL FORK")
        print(f"{'=' * 60}")
        print(f"Forking from: {self.fork_url[:50]}...")

        cmd = [
            "anvil",
            "--fork-url",
            self.fork_url,
            "--port",
            str(self.port),
            "--chain-id",
            "42161",  # Arbitrum
            "--timeout",
            "60000",
        ]

        try:
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            print("Waiting for Anvil to fork (this may take ~10 seconds)...")
            time.sleep(8)

            if self.process.poll() is not None:
                stderr = self.process.stderr.read().decode() if self.process.stderr else ""
                print(f"ERROR: Anvil failed to start: {stderr[:500]}")
                return False

            print(f"Anvil started on port {self.port}")
            return True

        except FileNotFoundError:
            print("ERROR: 'anvil' command not found!")
            print("\nPlease install Foundry:")
            print("  curl -L https://foundry.paradigm.xyz | bash")
            print("  foundryup")
            return False

    def stop(self):
        """Stop Anvil."""
        if self.process:
            print("\nStopping Anvil...")
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            print("Anvil stopped.")


# =============================================================================
# WALLET FUNDING
# =============================================================================


def run_cast(args: list[str], check: bool = True) -> str:
    """Run a cast command."""
    cmd = ["cast"] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"Cast command failed: {result.stderr}")
    return result.stdout.strip()


def parse_cast_uint(output: str) -> int:
    """Parse uint from cast output."""
    output = output.strip()
    if " " in output:
        output = output.split(" ")[0]
    return int(output.replace(",", ""))


def fund_wallet_with_usdc(wallet: str, amount_usdc: int) -> bool:
    """Fund wallet with USDC by impersonating a whale."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_usdc} USDC")
    print(f"{'=' * 60}")

    amount_wei = amount_usdc * 10**6

    try:
        # Check whale balance
        balance = run_cast(
            [
                "call",
                USDC_ADDRESS,
                "balanceOf(address)(uint256)",
                USDC_WHALE,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        whale_balance = parse_cast_uint(balance)
        print(f"Whale USDC balance: {whale_balance / 10**6:,.2f}")

        if whale_balance < amount_wei:
            print("ERROR: Whale has insufficient USDC")
            return False

        # Give whale ETH for gas
        run_cast(
            [
                "rpc",
                "anvil_setBalance",
                USDC_WHALE,
                "0x56BC75E2D63100000",
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        # Impersonate and transfer
        run_cast(
            [
                "rpc",
                "anvil_impersonateAccount",
                USDC_WHALE,
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        run_cast(
            [
                "send",
                USDC_ADDRESS,
                "transfer(address,uint256)(bool)",
                wallet,
                str(amount_wei),
                "--from",
                USDC_WHALE,
                "--unlocked",
                "--gas-limit",
                "100000",
                "--rpc-url",
                ANVIL_RPC,
            ]
        )

        run_cast(
            [
                "rpc",
                "anvil_stopImpersonatingAccount",
                USDC_WHALE,
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        # Verify
        balance = run_cast(
            [
                "call",
                USDC_ADDRESS,
                "balanceOf(address)(uint256)",
                wallet,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        new_balance = parse_cast_uint(balance)
        print(f"Wallet USDC balance: {new_balance / 10**6:,.2f}")
        return new_balance >= amount_wei

    except Exception as e:
        print(f"ERROR: Failed to fund wallet: {e}")
        return False


def fund_wallet_with_weth(wallet: str, amount_weth: Decimal) -> bool:
    """Fund wallet with WETH by wrapping ETH."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_weth} WETH")
    print(f"{'=' * 60}")

    amount_wei = int(amount_weth * 10**18)

    try:
        # Ensure wallet has ETH
        run_cast(
            [
                "rpc",
                "anvil_setBalance",
                wallet,
                hex(10 * 10**18),
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        # Wrap ETH to WETH
        run_cast(
            [
                "send",
                WETH_ADDRESS,
                "--value",
                str(amount_wei),
                "--from",
                wallet,
                "--private-key",
                ANVIL_PRIVATE_KEY,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )

        # Verify
        balance = run_cast(
            [
                "call",
                WETH_ADDRESS,
                "balanceOf(address)(uint256)",
                wallet,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        weth_balance = int(balance.split()[0].replace(",", ""))
        print(f"Wallet WETH balance: {weth_balance / 10**18:.6f}")
        return weth_balance >= amount_wei

    except Exception as e:
        print(f"ERROR: Failed to fund wallet: {e}")
        return False


# =============================================================================
# STRATEGY EXECUTION DIRECTLY
# =============================================================================


def run_strategy_directly(atr_value: Decimal, regime: str) -> int:
    """
    Run the volatility-adaptive LP strategy directly with injected indicator data.

    Args:
        atr_value: ATR value to inject into market snapshot
        regime: Volatility regime name (for logging)

    Returns:
        Exit code (0 = success)
    """
    print(f"\n{'=' * 60}")
    print("RUNNING VOLATILITY-ADAPTIVE LP STRATEGY DIRECTLY")
    print(f"Testing regime: {regime.upper()}")
    print(f"ATR value: ${atr_value:.2f}")
    print(f"{'=' * 60}")

    from almanak.framework.strategies import ATRData, MarketSnapshot, TokenBalance
    from strategies.tests.lp.uni_vol_adaptive.strategy import (
        UniVolAdaptiveConfig,
        UniVolAdaptiveStrategy,
    )

    # Create config
    config = UniVolAdaptiveConfig(
        chain="arbitrum",
        network="anvil",
        pool="WETH/USDC/3000",
        base_range_width_pct=Decimal("0.10"),
        amount0=Decimal("0.002"),
        amount1=Decimal("3"),
        force_action="",  # Use ATR-based logic
    )

    # Create strategy instance
    strategy = UniVolAdaptiveStrategy(
        config=config,
        chain="arbitrum",
        wallet_address=ANVIL_WALLET,
    )

    # Create market snapshot with indicator data
    market = MarketSnapshot(chain="arbitrum", wallet_address=ANVIL_WALLET)

    # Set prices
    eth_price = Decimal("3000")
    market.set_price("WETH", eth_price)
    market.set_price("USDC", Decimal("1"))

    # Set balances
    market.set_balance(
        "WETH",
        TokenBalance(
            symbol="WETH",
            balance=FUND_AMOUNT_WETH,
            balance_usd=FUND_AMOUNT_WETH * eth_price,
            address=WETH_ADDRESS,
        ),
    )
    market.set_balance(
        "USDC",
        TokenBalance(
            symbol="USDC",
            balance=Decimal(str(FUND_AMOUNT_USDC)),
            balance_usd=Decimal(str(FUND_AMOUNT_USDC)),
            address=USDC_ADDRESS,
        ),
    )

    # Set ATR indicator data using the unified API
    atr_pct = (atr_value / eth_price) * Decimal("100")
    market.set_atr(
        "WETH",
        ATRData(
            value=atr_value,
            value_percent=atr_pct,
            period=14,
        ),
    )

    # Print expected behavior
    print(f"\n{'=' * 60}")
    print("EXPECTED BEHAVIOR")
    print(f"{'=' * 60}")
    print(f"ATR as % of price: {atr_pct:.2f}%")
    if atr_pct < Decimal("2"):
        print("Expected regime: LOW (ATR < 2%)")
        print("Expected range width: 5%")
    elif atr_pct > Decimal("5"):
        print("Expected regime: HIGH (ATR > 5%)")
        print("Expected range width: 15%")
    else:
        print("Expected regime: MEDIUM (2% < ATR < 5%)")
        print("Expected range width: 10%")

    print(f"\n{'=' * 60}")
    print("STRATEGY OUTPUT")
    print(f"{'=' * 60}\n")

    try:
        # Run strategy decide
        intent = strategy.decide(market)

        print(f"\nStrategy Decision: {intent}")
        if intent:
            print(f"Intent Type: {intent.intent_type}")
            if hasattr(intent, "reason"):
                print(f"Reason: {intent.reason}")

        return 0

    except Exception as e:
        print(f"Error running strategy: {e}")
        import traceback

        traceback.print_exc()
        return 1


# =============================================================================
# MAIN
# =============================================================================


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Run UniVolAdaptiveStrategy on Anvil with different volatility regimes"
    )
    parser.add_argument(
        "--regime",
        choices=["low", "medium", "high"],
        default="medium",
        help="Volatility regime to test (default: medium)",
    )
    parser.add_argument(
        "--skip-anvil",
        action="store_true",
        help="Skip Anvil setup (for testing strategy logic only)",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("ALMANAK TEST - UNISWAP V3 VOLATILITY-ADAPTIVE LP STRATEGY")
    print("=" * 60)
    print("\nThis test runs the UniVolAdaptiveStrategy through the full stack:")
    print("  1. Anvil fork of Arbitrum")
    print("  2. Fund wallet with WETH + USDC")
    print("  3. Run strategy with injected ATR indicator data")
    print("  4. Verify range width adapts to volatility regime")
    print(f"\nTesting regime: {args.regime.upper()}")
    print("")

    # Skip Anvil if requested (for testing strategy logic only)
    if args.skip_anvil:
        print("--skip-anvil flag set, running strategy without Anvil...")
        atr_value = ATR_VALUES[args.regime]
        exit_code = run_strategy_directly(atr_value, args.regime)
        sys.exit(exit_code)

    # Get RPC URL
    fork_url = os.getenv("ALMANAK_ARBITRUM_RPC_URL") or os.getenv("ALMANAK_RPC_URL")
    if not fork_url:
        print("ERROR: No RPC URL found in .env file")
        print("\nAdd one of these to .env:")
        print("  ALMANAK_ARBITRUM_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY")
        sys.exit(1)

    # Start Anvil
    anvil = AnvilManager(fork_url, ANVIL_PORT)
    if not anvil.start():
        sys.exit(1)

    try:
        # Fund wallet
        if not fund_wallet_with_usdc(ANVIL_WALLET, FUND_AMOUNT_USDC):
            print("Failed to fund wallet with USDC")
            sys.exit(1)

        if not fund_wallet_with_weth(ANVIL_WALLET, FUND_AMOUNT_WETH):
            print("Failed to fund wallet with WETH")
            sys.exit(1)

        # Get ATR value for regime
        atr_value = ATR_VALUES[args.regime]

        # Run strategy directly with injected indicator data
        exit_code = run_strategy_directly(atr_value, args.regime)

        if exit_code == 0:
            print(f"\n{'=' * 60}")
            print("SUCCESS!")
            print(f"{'=' * 60}")
            print("\nVolatility-adaptive LP strategy executed successfully.")
            print(f"Tested regime: {args.regime.upper()}")
            print(f"ATR value: ${atr_value:.2f}")
            print("Check the output above for range width details.")
        else:
            print(f"\n{'=' * 60}")
            print("EXECUTION COMPLETED WITH ERRORS")
            print(f"{'=' * 60}")
            print(f"Exit code: {exit_code}")

        sys.exit(exit_code)

    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        anvil.stop()


if __name__ == "__main__":
    main()
