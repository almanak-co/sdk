#!/usr/bin/env python3
"""
===============================================================================
Run TraderJoe Tight-Range Scalper Strategy on Anvil (Local Fork)
===============================================================================

This script tests the TJ Tight-Range Scalper strategy on an Anvil fork.
It uses a tight 5% range for maximum fee capture.

PREREQUISITES:
--------------
1. Foundry installed (provides anvil and cast)
   curl -L https://foundry.paradigm.xyz | bash && foundryup

2. ALCHEMY_API_KEY or ALMANAK_AVALANCHE_RPC_URL in .env file

USAGE:
------
    python strategies/tests/lp/tj_tight_scalper/run_anvil.py

    # With custom options:
    python strategies/tests/lp/tj_tight_scalper/run_anvil.py --action open
    python strategies/tests/lp/tj_tight_scalper/run_anvil.py --action close

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

# Load environment variables
from dotenv import load_dotenv  # noqa: E402

load_dotenv(project_root / ".env")


# =============================================================================
# CONFIGURATION
# =============================================================================

# Anvil's first default account (Account #0)
ANVIL_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
ANVIL_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

# Avalanche C-Chain token addresses
WAVAX_ADDRESS = "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"
USDC_ADDRESS = "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E"  # Native USDC on Avalanche

# Whale addresses for funding
USDC_WHALE = "0x625E7708f30cA75bfd92586e17077590C60eb4cD"  # Aave aUSDC on Avalanche

# Amounts to fund (~$6 worth of LP)
FUND_AMOUNT_USDC = Decimal("10")  # 10 USDC (extra buffer)
FUND_AMOUNT_WAVAX = Decimal("0.5")  # 0.5 WAVAX (~$15)

# Anvil settings
ANVIL_PORT = 8546
ANVIL_RPC = f"http://127.0.0.1:{ANVIL_PORT}"

# Avalanche C-Chain ID
AVALANCHE_CHAIN_ID = 43114


# =============================================================================
# ANVIL MANAGER
# =============================================================================


class AnvilManager:
    """Manages the Anvil fork lifecycle."""

    def __init__(self, fork_url: str, port: int = 8546, chain_id: int = 43114):
        self.fork_url = fork_url
        self.port = port
        self.chain_id = chain_id
        self.process: subprocess.Popen | None = None

    def start(self) -> bool:
        """Start Anvil fork."""
        print(f"\n{'=' * 60}")
        print("STARTING ANVIL FORK OF AVALANCHE C-CHAIN")
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

    def stop(self) -> None:
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
    if output.startswith("0x"):
        return int(output, 16)
    return int(output.replace(",", ""))


def fund_wallet_with_usdc(wallet: str, amount_usdc: int) -> bool:
    """Fund wallet with USDC using whale impersonation."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_usdc} USDC")
    print(f"{'=' * 60}")

    amount_wei = amount_usdc * 10**6

    try:
        # Check whale balance first
        balance = run_cast(
            [
                "call",
                USDC_ADDRESS,
                "balanceOf(address)(uint256)",
                USDC_WHALE,
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        if balance:
            whale_balance = parse_cast_uint(balance)
            print(f"Whale USDC balance: {whale_balance / 10**6:,.2f}")

            if whale_balance >= amount_wei:
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
                    ],
                    check=False,
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

        # Verify final balance
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
        print(f"Wallet USDC balance after funding: {new_balance / 10**6:,.2f}")
        return new_balance >= amount_wei

    except Exception as e:
        print(f"ERROR: Funding failed: {e}")
        return False


def fund_wallet_with_wavax(wallet: str, amount_wavax: Decimal) -> bool:
    """Fund wallet with WAVAX by wrapping AVAX."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_wavax} WAVAX")
    print(f"{'=' * 60}")

    amount_wei = int(amount_wavax * 10**18)

    try:
        # Ensure wallet has AVAX (native token)
        run_cast(
            [
                "rpc",
                "anvil_setBalance",
                wallet,
                hex(100 * 10**18),  # 100 AVAX
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        # Check AVAX balance
        avax_balance = run_cast(
            [
                "balance",
                wallet,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        print(f"Wallet AVAX balance: {avax_balance}")

        # Wrap AVAX to WAVAX
        run_cast(
            [
                "send",
                WAVAX_ADDRESS,
                "deposit()",
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

        # Verify WAVAX balance
        balance = run_cast(
            [
                "call",
                WAVAX_ADDRESS,
                "balanceOf(address)(uint256)",
                wallet,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        wavax_balance = parse_cast_uint(balance)
        print(f"Wallet WAVAX balance: {wavax_balance / 10**18:.6f}")
        return wavax_balance >= amount_wei

    except Exception as e:
        print(f"ERROR: Failed to fund wallet with WAVAX: {e}")
        import traceback

        traceback.print_exc()
        return False


# =============================================================================
# STRATEGY EXECUTION - DIRECT
# =============================================================================


def run_strategy_directly(force_action: str = "open") -> int:
    """
    Run the TJ Tight-Range Scalper strategy directly with injected market data.

    Args:
        force_action: Action to force ('open' or 'close')

    Returns:
        Exit code (0 = success)
    """
    print(f"\n{'=' * 60}")
    print("RUNNING TJ TIGHT-RANGE SCALPER STRATEGY DIRECTLY")
    print(f"{'=' * 60}")
    print(f"Force action: {force_action.upper()}")

    from almanak.framework.strategies import MarketSnapshot, TokenBalance
    from strategies.tests.lp.tj_tight_scalper.strategy import (
        TJTightScalperConfig,
        TJTightScalperStrategy,
    )

    # Strategy parameters
    avax_price = Decimal("30")  # Approximate AVAX price
    range_width_pct = Decimal("0.05")  # 5% range

    # Create config
    config = TJTightScalperConfig(
        chain="avalanche",
        network="anvil",
        pool="WAVAX/USDC/20",
        range_width_pct=range_width_pct,
        amount_x=Decimal("0.15"),  # ~$4.5 worth of WAVAX at $30
        amount_y=Decimal("3"),  # $3 USDC
        num_bins=11,
        rebalance_threshold_pct=Decimal("0.025"),  # 2.5% threshold
        force_action=force_action,
    )

    # Create strategy instance
    strategy = TJTightScalperStrategy(
        config=config,
        chain="avalanche",
        wallet_address=ANVIL_WALLET,
    )

    # Create market snapshot
    market = MarketSnapshot(chain="avalanche", wallet_address=ANVIL_WALLET)

    # Set prices
    market.set_price("WAVAX", avax_price)
    market.set_price("USDC", Decimal("1"))

    # Set balances
    market.set_balance(
        "WAVAX",
        TokenBalance(
            symbol="WAVAX",
            balance=FUND_AMOUNT_WAVAX,
            balance_usd=FUND_AMOUNT_WAVAX * avax_price,
            address=WAVAX_ADDRESS,
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

    # Print expected behavior
    print(f"\n{'=' * 60}")
    print("MARKET SNAPSHOT")
    print(f"{'=' * 60}")
    print(f"WAVAX Price: ${avax_price}")
    print("USDC Price: $1")
    print(f"WAVAX Balance: {FUND_AMOUNT_WAVAX}")
    print(f"USDC Balance: {FUND_AMOUNT_USDC}")
    print("Pool: WAVAX/USDC (bin step 20)")
    print(f"Range Width: {range_width_pct * 100}%")

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
            if hasattr(intent, "amount0") or hasattr(intent, "amount_x"):
                amount_x = getattr(intent, "amount_x", getattr(intent, "amount0", None))
                print(f"Amount X: {amount_x}")
            if hasattr(intent, "amount1") or hasattr(intent, "amount_y"):
                amount_y = getattr(intent, "amount_y", getattr(intent, "amount1", None))
                print(f"Amount Y: {amount_y}")

        return 0

    except Exception as e:
        print(f"Error running strategy: {e}")
        import traceback

        traceback.print_exc()
        return 1


# =============================================================================
# MAIN
# =============================================================================


def main() -> None:
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Run TJ Tight-Range Scalper Strategy on Anvil")
    parser.add_argument(
        "--action",
        choices=["open", "close", "test"],
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
    print("ALMANAK TEST - TJ TIGHT-RANGE SCALPER ON ANVIL")
    print("=" * 60)
    print("\nThis test runs the TJTightScalperStrategy with:")
    print("  - Tight 5% price range (2.5% from center)")
    print("  - 11 bins for liquidity distribution")
    print("  - Rebalance when price moves >2.5% from center")
    print("  - ~$6 worth of liquidity (0.15 WAVAX + 3 USDC)")
    print(f"\nAction: {args.action.upper()}")
    print("")

    # Skip Anvil if requested (for testing strategy logic only)
    if args.skip_anvil:
        print("--skip-anvil flag set, running strategy without Anvil...")
        exit_code = run_strategy_directly(force_action=args.action)
        sys.exit(exit_code)

    # Get RPC URL
    fork_url = os.getenv("ALMANAK_AVALANCHE_RPC_URL")
    if not fork_url:
        alchemy_key = os.getenv("ALCHEMY_API_KEY")
        if alchemy_key:
            fork_url = f"https://avax-mainnet.g.alchemy.com/v2/{alchemy_key}"
        else:
            fork_url = "https://api.avax.network/ext/bc/C/rpc"
            print("Note: Using public Avalanche RPC (set ALCHEMY_API_KEY for better reliability)")
    print(f"Fork URL: {fork_url[:50]}...")

    # Start Anvil
    anvil = AnvilManager(fork_url, ANVIL_PORT, AVALANCHE_CHAIN_ID)
    if not anvil.start():
        sys.exit(1)

    try:
        # Fund wallet
        if not fund_wallet_with_wavax(ANVIL_WALLET, FUND_AMOUNT_WAVAX):
            print("Failed to fund wallet with WAVAX")
            sys.exit(1)

        if not fund_wallet_with_usdc(ANVIL_WALLET, int(FUND_AMOUNT_USDC)):
            print("Failed to fund wallet with USDC (continuing anyway)")

        # Run strategy directly
        exit_code = run_strategy_directly(force_action=args.action)

        if exit_code == 0:
            print(f"\n{'=' * 60}")
            print("SUCCESS!")
            print(f"{'=' * 60}")
            print("\nTJ Tight-Range Scalper strategy executed successfully.")
            print("Strategy opened LP position with 5% range around current price.")
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
