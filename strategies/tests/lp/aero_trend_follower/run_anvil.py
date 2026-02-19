#!/usr/bin/env python3
"""
===============================================================================
Aerodrome Trend-Following LP Strategy - Anvil Test Script
===============================================================================

This script tests the Aerodrome Trend-Following LP strategy on Anvil.
It forks Base chain and tests volatile pool LP operations with WETH/USDC.

The strategy uses EMA(9) and EMA(21) crossovers for entry/exit signals.
For testing, we use forced EMA values to simulate bullish/bearish trends.

WHAT THIS SCRIPT DOES:
----------------------
1. Starts an Anvil fork of Base chain
2. Funds the test wallet with WETH and USDC
3. Runs the strategy via the CLI runner with forced EMA values
4. Tests both bullish (open) and bearish (close) scenarios

USAGE:
------
    python strategies/tests/lp/aero_trend_follower/run_anvil.py

    # Force bullish trend (EMA9 > EMA21):
    python strategies/tests/lp/aero_trend_follower/run_anvil.py --trend bullish

    # Force bearish trend (EMA9 < EMA21):
    python strategies/tests/lp/aero_trend_follower/run_anvil.py --trend bearish

    # Force specific action:
    python strategies/tests/lp/aero_trend_follower/run_anvil.py --action open
    python strategies/tests/lp/aero_trend_follower/run_anvil.py --action close

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

# Base chain token addresses
WETH_ADDRESS = "0x4200000000000000000000000000000000000006"  # WETH on Base
USDC_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"  # Native USDC on Base

# Amounts to fund (~$6 total worth)
FUND_AMOUNT_WETH = Decimal("0.005")  # 0.005 WETH (~$15 at $3000)
FUND_AMOUNT_USDC = 10  # 10 USDC

# Anvil settings
ANVIL_PORT = 8548  # Use different port to avoid conflicts
ANVIL_RPC = f"http://127.0.0.1:{ANVIL_PORT}"

# Base Chain ID
BASE_CHAIN_ID = 8453


# =============================================================================
# ANVIL MANAGER
# =============================================================================


class AnvilManager:
    """Manages the Anvil fork lifecycle."""

    def __init__(self, fork_url: str, port: int = 8548, chain_id: int = 8453):
        self.fork_url = fork_url
        self.port = port
        self.chain_id = chain_id
        self.process: subprocess.Popen | None = None

    def start(self) -> bool:
        """Start Anvil fork."""
        print(f"\n{'=' * 60}")
        print("STARTING ANVIL FORK OF BASE CHAIN")
        print(f"{'=' * 60}")
        print(f"Forking from: {self.fork_url[:50]}...")

        cmd = [
            "anvil",
            "--fork-url",
            self.fork_url,
            "--port",
            str(self.port),
            "--chain-id",
            str(self.chain_id),
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
            time.sleep(10)

            if self.process.poll() is not None:
                stderr = self.process.stderr.read().decode() if self.process.stderr else ""
                print(f"ERROR: Anvil failed to start: {stderr[:500]}")
                return False

            print(f"Anvil started on port {self.port} (Chain ID: {self.chain_id})")
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
    # Handle hex output
    if output.startswith("0x"):
        return int(output, 16)
    return int(output.replace(",", ""))


def fund_wallet_with_eth(wallet: str, amount_eth: Decimal = Decimal("1")) -> bool:
    """Fund wallet with ETH for gas."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_eth} ETH")
    print(f"{'=' * 60}")

    amount_wei = int(amount_eth * 10**18)

    try:
        run_cast(
            [
                "rpc",
                "anvil_setBalance",
                wallet,
                hex(amount_wei),
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        # Check ETH balance
        eth_balance = run_cast(
            [
                "balance",
                wallet,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        print(f"Wallet ETH balance: {eth_balance}")
        return True

    except Exception as e:
        print(f"ERROR: Failed to fund wallet with ETH: {e}")
        return False


def fund_wallet_with_weth(wallet: str, amount: Decimal) -> bool:
    """Fund wallet with WETH by wrapping ETH."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount} WETH")
    print(f"{'=' * 60}")

    amount_wei = int(amount * 10**18)

    try:
        # On Base, WETH is the native wrapped token
        # We can call deposit() on the WETH contract to wrap ETH

        # First, ensure wallet has enough ETH (already done)
        # Then call deposit() on WETH contract
        tx_hash = run_cast(
            [
                "send",
                WETH_ADDRESS,
                "deposit()",
                "--value",
                str(amount_wei),
                "--private-key",
                ANVIL_PRIVATE_KEY,
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )
        print(f"Deposit TX: {tx_hash[:20] if tx_hash else 'N/A'}...")

        # Verify balance
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
        new_balance = parse_cast_uint(balance)
        print(f"Wallet WETH balance: {new_balance / 10**18:.6f}")

        return new_balance >= amount_wei

    except Exception as e:
        print(f"ERROR: Failed to fund wallet with WETH: {e}")
        import traceback

        traceback.print_exc()
        return False


def fund_wallet_with_usdc(wallet: str, amount: int) -> bool:
    """Fund wallet with USDC using storage slot manipulation."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount} USDC")
    print(f"{'=' * 60}")

    # USDC has 6 decimals
    amount_wei = amount * 10**6

    try:
        # Use cast index to find storage slot and set balance directly
        slot_hex = run_cast(
            [
                "index",
                "address",
                wallet,
                "9",  # USDC balance slot on Base
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        if slot_hex:
            # Set the balance directly
            run_cast(
                [
                    "rpc",
                    "anvil_setStorageAt",
                    USDC_ADDRESS,
                    slot_hex,
                    f"0x{amount_wei:064x}",
                    "--rpc-url",
                    ANVIL_RPC,
                ],
                check=False,
            )

        # Verify balance
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
        print(f"ERROR: Failed to fund wallet with USDC: {e}")
        import traceback

        traceback.print_exc()
        return False


# =============================================================================
# STRATEGY EXECUTION
# =============================================================================


def run_strategy_directly(
    force_action: str = "",
    ema9_value: Decimal | None = None,
    ema21_value: Decimal | None = None,
) -> int:
    """
    Run the Aerodrome Trend-Following strategy directly with injected indicator data.

    Args:
        force_action: Force "open" or "close"
        ema9_value: EMA(9) value to inject into market snapshot
        ema21_value: EMA(21) value to inject into market snapshot

    Returns:
        Exit code (0 = success)
    """
    print(f"\n{'=' * 60}")
    print("RUNNING AERODROME TREND-FOLLOWING STRATEGY DIRECTLY")
    print(f"{'=' * 60}")

    from almanak.framework.strategies import MAData, MarketSnapshot, TokenBalance
    from strategies.tests.lp.aero_trend_follower.strategy import (
        AeroTrendFollowerConfig,
        AeroTrendFollowerStrategy,
    )

    # Print EMA info
    if ema9_value and ema21_value:
        trend = "BULLISH" if ema9_value > ema21_value else "BEARISH"
        print(f"EMA values: EMA9={ema9_value}, EMA21={ema21_value}")
        print(f"Trend signal: {trend}")
    elif force_action:
        print(f"Forced action: {force_action.upper()}")

    # Create config
    config = AeroTrendFollowerConfig(
        chain="base",
        network="anvil",
        pool="WETH/USDC",
        stable=False,
        amount0=Decimal("0.002"),
        amount1=Decimal("3"),
        force_action=force_action,
    )

    # Create strategy instance
    strategy = AeroTrendFollowerStrategy(
        config=config,
        chain="base",
        wallet_address=ANVIL_WALLET,
    )

    # Create market snapshot with indicator data
    market = MarketSnapshot(chain="base", wallet_address=ANVIL_WALLET)

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

    # Set EMA indicator data using the unified API
    if ema9_value is not None:
        market.set_ma(
            "WETH",
            MAData(
                value=ema9_value,
                ma_type="EMA",
                period=9,
                current_price=eth_price,
            ),
            ma_type="EMA",
            period=9,
        )
    if ema21_value is not None:
        market.set_ma(
            "WETH",
            MAData(
                value=ema21_value,
                ma_type="EMA",
                period=21,
                current_price=eth_price,
            ),
            ma_type="EMA",
            period=21,
        )

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

    parser = argparse.ArgumentParser(description="Run AeroTrendFollowerStrategy on Anvil")
    parser.add_argument(
        "--action",
        choices=["open", "close"],
        default="",
        help="Force LP action (default: use EMA logic)",
    )
    parser.add_argument(
        "--trend",
        choices=["bullish", "bearish"],
        default="bullish",
        help="Force trend for testing (default: bullish)",
    )
    parser.add_argument(
        "--skip-anvil",
        action="store_true",
        help="Skip Anvil setup (for testing strategy logic only)",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("ALMANAK TEST - AERODROME TREND-FOLLOWING LP ON ANVIL")
    print("=" * 60)
    print("\nThis test runs the AeroTrendFollowerStrategy through the full stack:")
    print("  1. Anvil fork of Base chain")
    print("  2. Fund wallet with WETH + USDC")
    print("  3. Run strategy directly with injected market data")
    print("  4. Print strategy decision")
    print("\nPool: WETH/USDC (volatile)")

    # Set up EMA values based on trend
    if args.action:
        print(f"Forced action: {args.action.upper()}")
        ema9_value = None
        ema21_value = None
    else:
        print(f"Trend: {args.trend.upper()}")
        if args.trend == "bullish":
            # EMA9 > EMA21 = bullish (will open LP)
            ema9_value = Decimal("3100")
            ema21_value = Decimal("3000")
        else:
            # EMA9 < EMA21 = bearish (will close LP if in position)
            ema9_value = Decimal("2900")
            ema21_value = Decimal("3000")

    print("")

    # Skip Anvil if requested (for testing strategy logic only)
    if args.skip_anvil:
        print("--skip-anvil flag set, running strategy without Anvil...")
        if args.action:
            exit_code = run_strategy_directly(force_action=args.action)
        else:
            exit_code = run_strategy_directly(
                ema9_value=ema9_value,
                ema21_value=ema21_value,
            )
        sys.exit(exit_code)

    # Get RPC URL - Base specific
    fork_url = os.getenv("ALMANAK_BASE_RPC_URL")
    if not fork_url:
        # Use public Base RPC as fallback
        fork_url = "https://mainnet.base.org"
        print("Note: Using public Base RPC (set ALMANAK_BASE_RPC_URL for better reliability)")
    print(f"Fork URL: {fork_url[:50]}...")

    # Start Anvil
    anvil = AnvilManager(fork_url, ANVIL_PORT, BASE_CHAIN_ID)
    if not anvil.start():
        sys.exit(1)

    try:
        # Fund wallet with ETH for gas (and for WETH wrapping)
        if not fund_wallet_with_eth(ANVIL_WALLET, Decimal("2")):
            print("Failed to fund wallet with ETH")
            sys.exit(1)

        # Fund wallet with WETH
        if not fund_wallet_with_weth(ANVIL_WALLET, FUND_AMOUNT_WETH):
            print("Failed to fund wallet with WETH (continuing anyway for testing)")

        # Fund wallet with USDC
        if not fund_wallet_with_usdc(ANVIL_WALLET, FUND_AMOUNT_USDC):
            print("Failed to fund wallet with USDC (continuing anyway for testing)")

        # Run strategy directly with injected indicator data
        if args.action:
            exit_code = run_strategy_directly(force_action=args.action)
        else:
            exit_code = run_strategy_directly(
                ema9_value=ema9_value,
                ema21_value=ema21_value,
            )

        if exit_code == 0:
            print(f"\n{'=' * 60}")
            print("SUCCESS!")
            print(f"{'=' * 60}")
            print("\nAerodrome trend-following LP strategy executed successfully.")
            print(f"Trend: {args.trend.upper() if not args.action else args.action.upper()}")
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
