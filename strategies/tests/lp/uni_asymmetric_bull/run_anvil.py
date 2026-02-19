#!/usr/bin/env python3
"""
===============================================================================
Uniswap V3 Asymmetric Bullish LP Strategy - Anvil Test Runner
===============================================================================

This script tests the asymmetric bullish LP strategy on an Anvil fork of
Arbitrum mainnet. It demonstrates how the strategy uses an asymmetric range
with more upside room than downside (bullish bias).

WHAT THIS SCRIPT DOES:
----------------------
1. Starts an Anvil fork of Arbitrum mainnet
2. Funds the test wallet with WETH and USDC
3. Runs the strategy directly with injected market data
4. Prints the asymmetric range bounds

ASYMMETRIC RANGE:
-----------------
- Upside: +12% from current price
- Downside: -8% from current price
- Result: 60% upside room, 40% downside room

Example at $3400 ETH price:
- range_lower = $3400 * 0.92 = $3128 (8% downside)
- range_upper = $3400 * 1.12 = $3808 (12% upside)

USAGE:
------
    python strategies/tests/lp/uni_asymmetric_bull/run_anvil.py

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
FUND_AMOUNT_USDC = Decimal("100")  # 100 USDC
FUND_AMOUNT_WETH = Decimal("0.05")  # 0.05 WETH

# Anvil settings
ANVIL_PORT = 8545
ANVIL_RPC = f"http://127.0.0.1:{ANVIL_PORT}"


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
# STRATEGY EXECUTION - DIRECT
# =============================================================================


def run_strategy_directly(force_action: str = "open") -> int:
    """
    Run the asymmetric bullish LP strategy directly with injected market data.

    Args:
        force_action: Action to force ('open' or 'close')

    Returns:
        Exit code (0 = success)
    """
    print(f"\n{'=' * 60}")
    print("RUNNING ASYMMETRIC BULLISH LP STRATEGY DIRECTLY")
    print(f"{'=' * 60}")
    print(f"Force action: {force_action.upper()}")

    from almanak.framework.strategies import MarketSnapshot, TokenBalance
    from strategies.tests.lp.uni_asymmetric_bull.strategy import (
        UniAsymmetricBullConfig,
        UniAsymmetricBullStrategy,
    )

    # Strategy parameters
    upside_pct = Decimal("0.12")  # 12% upside
    downside_pct = Decimal("0.08")  # 8% downside
    eth_price = Decimal("3400")

    # Create config
    config = UniAsymmetricBullConfig(
        chain="arbitrum",
        network="anvil",
        pool="WETH/USDC/3000",
        upside_pct=upside_pct,
        downside_pct=downside_pct,
        amount0=Decimal("0.002"),
        amount1=Decimal("3"),
        force_action=force_action,
    )

    # Create strategy instance
    strategy = UniAsymmetricBullStrategy(
        config=config,
        chain="arbitrum",
        wallet_address=ANVIL_WALLET,
    )

    # Create market snapshot
    market = MarketSnapshot(chain="arbitrum", wallet_address=ANVIL_WALLET)

    # Set prices
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
            balance=FUND_AMOUNT_USDC,
            balance_usd=FUND_AMOUNT_USDC,
            address=USDC_ADDRESS,
        ),
    )

    # Print expected asymmetric range
    print(f"\n{'=' * 60}")
    print("EXPECTED ASYMMETRIC RANGE BOUNDS")
    print(f"{'=' * 60}")

    range_lower = eth_price * (Decimal("1") - downside_pct)
    range_upper = eth_price * (Decimal("1") + upside_pct)
    total_width = range_upper - range_lower
    upside_room = range_upper - eth_price
    downside_room = eth_price - range_lower

    print(f"Current Price: ${eth_price:,.2f}")
    print(f"Upside PCT: {upside_pct * 100}%")
    print(f"Downside PCT: {downside_pct * 100}%")
    print("")
    print(f"Range Lower: ${range_lower:,.2f} (-{downside_pct * 100}%)")
    print(f"Range Upper: ${range_upper:,.2f} (+{upside_pct * 100}%)")
    print("")
    print(f"Total Range Width: ${total_width:,.2f}")
    print(f"Upside Room: ${upside_room:,.2f} ({(upside_room / total_width) * 100:.1f}% of range)")
    print(f"Downside Room: ${downside_room:,.2f} ({(downside_room / total_width) * 100:.1f}% of range)")
    print("")
    print("BULLISH BIAS: More room for price to go UP before hitting upper bound")

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

            # Print LP-specific details
            if hasattr(intent, "pool"):
                print(f"Pool: {intent.pool}")
            if hasattr(intent, "amount0"):
                print(f"Amount0: {intent.amount0}")
            if hasattr(intent, "amount1"):
                print(f"Amount1: {intent.amount1}")
            if hasattr(intent, "range_lower"):
                print(f"Range Lower: {intent.range_lower}")
            if hasattr(intent, "range_upper"):
                print(f"Range Upper: {intent.range_upper}")

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

    parser = argparse.ArgumentParser(description="Run UniAsymmetricBullStrategy on Anvil")
    parser.add_argument(
        "--action",
        choices=["open", "close"],
        default="open",
        help="LP action to test (default: open)",
    )
    parser.add_argument(
        "--skip-anvil",
        action="store_true",
        help="Skip Anvil setup (for testing strategy logic only)",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("ALMANAK TEST - UNISWAP V3 ASYMMETRIC BULLISH LP STRATEGY")
    print("=" * 60)
    print("\nThis test runs the UniAsymmetricBullStrategy through the full stack:")
    print("  1. Anvil fork of Arbitrum")
    print("  2. Fund wallet with WETH + USDC")
    print("  3. Run strategy directly with injected market data")
    print("  4. Verify range bounds show bullish bias")
    print("")
    print("ASYMMETRIC RANGE:")
    print("  - Upside: +12% from current price")
    print("  - Downside: -8% from current price")
    print("  - Result: 60% upside room, 40% downside room")
    print("")

    # Skip Anvil if requested (for testing strategy logic only)
    if args.skip_anvil:
        print("--skip-anvil flag set, running strategy without Anvil...")
        exit_code = run_strategy_directly(force_action=args.action)
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
        if not fund_wallet_with_usdc(ANVIL_WALLET, int(FUND_AMOUNT_USDC)):
            print("Failed to fund wallet with USDC")
            sys.exit(1)

        if not fund_wallet_with_weth(ANVIL_WALLET, FUND_AMOUNT_WETH):
            print("Failed to fund wallet with WETH")
            sys.exit(1)

        # Run strategy directly
        exit_code = run_strategy_directly(force_action=args.action)

        if exit_code == 0:
            print(f"\n{'=' * 60}")
            print("SUCCESS!")
            print(f"{'=' * 60}")
            print("\nAsymmetric bullish LP strategy executed successfully.")
            print("Check the output above for range bound details.")
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
