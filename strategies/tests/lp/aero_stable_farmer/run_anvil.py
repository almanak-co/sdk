#!/usr/bin/env python3
"""
===============================================================================
Aerodrome Stable Yield Farmer - Anvil Test Script
===============================================================================

This script tests the Aerodrome Stable Yield Farmer strategy on Anvil.
It forks Base chain and tests stable pool LP operations with USDC/USDbC.

WHAT THIS SCRIPT DOES:
----------------------
1. Starts an Anvil fork of Base chain
2. Funds the test wallet with USDC and USDbC
3. Runs the strategy directly with injected market data
4. Prints the strategy decision

USAGE:
------
    python strategies/tests/lp/aero_stable_farmer/run_anvil.py

    # With custom options:
    python strategies/tests/lp/aero_stable_farmer/run_anvil.py --action open
    python strategies/tests/lp/aero_stable_farmer/run_anvil.py --action close

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
USDC_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"  # Native USDC on Base
USDBC_ADDRESS = "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA"  # Bridged USDC on Base

# Amounts to fund (~$6 total)
FUND_AMOUNT_USDC = Decimal("10")  # 10 USDC
FUND_AMOUNT_USDBC = Decimal("10")  # 10 USDbC

# Anvil settings
ANVIL_PORT = 8547  # Use different port to avoid conflicts
ANVIL_RPC = f"http://127.0.0.1:{ANVIL_PORT}"

# Base Chain ID
BASE_CHAIN_ID = 8453


# =============================================================================
# ANVIL MANAGER
# =============================================================================


class AnvilManager:
    """Manages the Anvil fork lifecycle."""

    def __init__(self, fork_url: str, port: int = 8547, chain_id: int = 8453):
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


def fund_wallet_with_token(wallet: str, amount: int, token_address: str, token_name: str, slot: int = 9) -> bool:
    """Fund wallet with token using storage slot manipulation."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount} {token_name}")
    print(f"{'=' * 60}")

    # USDC/USDbC have 6 decimals
    amount_wei = amount * 10**6

    try:
        # Use cast index to find storage slot and set balance directly
        slot_hex = run_cast(
            [
                "index",
                "address",
                wallet,
                str(slot),
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
                    token_address,
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
                token_address,
                "balanceOf(address)(uint256)",
                wallet,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        new_balance = parse_cast_uint(balance)
        print(f"Wallet {token_name} balance: {new_balance / 10**6:,.2f}")

        if new_balance >= amount_wei:
            return True

        # Try different slot if first didn't work
        print(f"Slot {slot} didn't work, trying slot 0...")
        slot_hex = run_cast(
            [
                "index",
                "address",
                wallet,
                "0",
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        if slot_hex:
            run_cast(
                [
                    "rpc",
                    "anvil_setStorageAt",
                    token_address,
                    slot_hex,
                    f"0x{amount_wei:064x}",
                    "--rpc-url",
                    ANVIL_RPC,
                ],
                check=False,
            )

        # Verify balance again
        balance = run_cast(
            [
                "call",
                token_address,
                "balanceOf(address)(uint256)",
                wallet,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        new_balance = parse_cast_uint(balance)
        print(f"Wallet {token_name} balance: {new_balance / 10**6:,.2f}")
        return new_balance >= amount_wei

    except Exception as e:
        print(f"ERROR: Failed to fund wallet with {token_name}: {e}")
        import traceback

        traceback.print_exc()
        return False


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


# =============================================================================
# STRATEGY EXECUTION - DIRECT
# =============================================================================


def run_strategy_directly(force_action: str = "open") -> int:
    """
    Run the Aerodrome Stable Yield Farmer strategy directly with injected market data.

    Args:
        force_action: Action to force ('open' or 'close')

    Returns:
        Exit code (0 = success)
    """
    print(f"\n{'=' * 60}")
    print("RUNNING AERODROME STABLE YIELD FARMER STRATEGY DIRECTLY")
    print(f"{'=' * 60}")
    print(f"Force action: {force_action.upper()}")

    from almanak.framework.strategies import MarketSnapshot, TokenBalance
    from strategies.tests.lp.aero_stable_farmer.strategy import (
        AeroStableFarmerConfig,
        AeroStableFarmerStrategy,
    )

    # Create config
    config = AeroStableFarmerConfig(
        chain="base",
        network="anvil",
        pool="USDC/USDbC",
        stable=True,
        amount0=Decimal("3"),
        amount1=Decimal("3"),
        force_action=force_action,
    )

    # Create strategy instance
    strategy = AeroStableFarmerStrategy(
        config=config,
        chain="base",
        wallet_address=ANVIL_WALLET,
    )

    # Create market snapshot
    market = MarketSnapshot(chain="base", wallet_address=ANVIL_WALLET)

    # Set prices (stablecoins are ~$1)
    usdc_price = Decimal("1")
    usdbc_price = Decimal("1")
    market.set_price("USDC", usdc_price)
    market.set_price("USDbC", usdbc_price)

    # Set balances
    market.set_balance(
        "USDC",
        TokenBalance(
            symbol="USDC",
            balance=FUND_AMOUNT_USDC,
            balance_usd=FUND_AMOUNT_USDC * usdc_price,
            address=USDC_ADDRESS,
        ),
    )
    market.set_balance(
        "USDbC",
        TokenBalance(
            symbol="USDbC",
            balance=FUND_AMOUNT_USDBC,
            balance_usd=FUND_AMOUNT_USDBC * usdbc_price,
            address=USDBC_ADDRESS,
        ),
    )

    # Print expected behavior
    print(f"\n{'=' * 60}")
    print("MARKET SNAPSHOT")
    print(f"{'=' * 60}")
    print(f"USDC Price: ${usdc_price}")
    print(f"USDbC Price: ${usdbc_price}")
    print(f"USDC Balance: {FUND_AMOUNT_USDC}")
    print(f"USDbC Balance: {FUND_AMOUNT_USDBC}")
    print("Pool: USDC/USDbC (stable)")

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

    parser = argparse.ArgumentParser(description="Run AeroStableFarmerStrategy on Anvil")
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
    print("ALMANAK TEST - AERODROME STABLE YIELD FARMER ON ANVIL")
    print("=" * 60)
    print("\nThis test runs the AeroStableFarmerStrategy through the full stack:")
    print("  1. Anvil fork of Base chain")
    print("  2. Fund wallet with USDC + USDbC")
    print("  3. Run strategy directly with injected market data")
    print("  4. Print strategy decision")
    print(f"\nAction: {args.action.upper()}")
    print("Pool: USDC/USDbC (stable)")
    print("")

    # Skip Anvil if requested (for testing strategy logic only)
    if args.skip_anvil:
        print("--skip-anvil flag set, running strategy without Anvil...")
        exit_code = run_strategy_directly(force_action=args.action)
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
        # Fund wallet with ETH for gas
        if not fund_wallet_with_eth(ANVIL_WALLET):
            print("Failed to fund wallet with ETH")
            sys.exit(1)

        # Fund wallet with USDC (slot 9 for Base USDC)
        if not fund_wallet_with_token(ANVIL_WALLET, int(FUND_AMOUNT_USDC), USDC_ADDRESS, "USDC", slot=9):
            print("Failed to fund wallet with USDC (continuing anyway for testing)")

        # Fund wallet with USDbC (try slot 9, then slot 0)
        if not fund_wallet_with_token(ANVIL_WALLET, int(FUND_AMOUNT_USDBC), USDBC_ADDRESS, "USDbC", slot=9):
            print("Failed to fund wallet with USDbC (continuing anyway for testing)")

        # Run strategy directly
        exit_code = run_strategy_directly(force_action=args.action)

        if exit_code == 0:
            print(f"\n{'=' * 60}")
            print("SUCCESS!")
            print(f"{'=' * 60}")
            print("\nAerodrome stable LP strategy executed successfully.")
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
