#!/usr/bin/env python3
"""
===============================================================================
TraderJoe Wide-Range Accumulator Strategy - Anvil Test Runner
===============================================================================

This script tests the TJ Wide-Range Accumulator strategy on an Anvil fork of
Avalanche C-Chain. It demonstrates how the strategy uses a wide 15% range
for JOE/AVAX accumulation with hybrid rebalancing.

WHAT THIS SCRIPT DOES:
----------------------
1. Starts an Anvil fork of Avalanche C-Chain
2. Funds the test wallet with JOE and WAVAX
3. Runs the strategy directly with injected market data
4. Prints the wide range bounds and rebalance thresholds

WIDE RANGE:
-----------
- Range Width: 15% (+/-7.5% from current price)
- Bins: 21 bins for liquidity distribution
- Rebalance: After 7 days OR when price moves >7%

Example at $0.0133 JOE/WAVAX price:
- range_lower = $0.0133 * 0.925 = $0.0123 (7.5% below)
- range_upper = $0.0133 * 1.075 = $0.0143 (7.5% above)

USAGE:
------
    python strategies/tests/lp/tj_wide_accumulator/run_anvil.py

    # With custom options:
    python strategies/tests/lp/tj_wide_accumulator/run_anvil.py --action open
    python strategies/tests/lp/tj_wide_accumulator/run_anvil.py --action close

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

# Avalanche C-Chain token addresses
WAVAX_ADDRESS = "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"
JOE_ADDRESS = "0x6e84a6216eA6dACC71eE8E6b0a5B7322EEbC0fDd"

# Whale addresses for funding (JOE holders on Avalanche)
# TraderJoe: sJOE staking contract has significant JOE
JOE_WHALE = "0x102D195C3eE8BF8A9A89d63FB3659432d3174d81"  # sJOE staking contract

# Amounts to fund
FUND_AMOUNT_JOE = Decimal("20")  # ~20 JOE (~$8 at $0.4)
FUND_AMOUNT_WAVAX = Decimal("0.3")  # ~$9 worth of WAVAX at $30

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


def fund_wallet_with_joe(wallet: str, amount_joe: int) -> bool:
    """Fund wallet with JOE using whale impersonation."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_joe} JOE")
    print(f"{'=' * 60}")

    amount_wei = amount_joe * 10**18

    try:
        # Check whale balance first
        balance = run_cast(
            [
                "call",
                JOE_ADDRESS,
                "balanceOf(address)(uint256)",
                JOE_WHALE,
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        if balance:
            whale_balance = parse_cast_uint(balance)
            print(f"Whale JOE balance: {whale_balance / 10**18:,.2f}")

            if whale_balance >= amount_wei:
                # Give whale ETH for gas
                run_cast(
                    [
                        "rpc",
                        "anvil_setBalance",
                        JOE_WHALE,
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
                        JOE_WHALE,
                        "--rpc-url",
                        ANVIL_RPC,
                    ],
                    check=False,
                )

                run_cast(
                    [
                        "send",
                        JOE_ADDRESS,
                        "transfer(address,uint256)(bool)",
                        wallet,
                        str(amount_wei),
                        "--from",
                        JOE_WHALE,
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
                        JOE_WHALE,
                        "--rpc-url",
                        ANVIL_RPC,
                    ],
                    check=False,
                )

        # Verify final balance
        balance = run_cast(
            [
                "call",
                JOE_ADDRESS,
                "balanceOf(address)(uint256)",
                wallet,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        new_balance = parse_cast_uint(balance)
        print(f"Wallet JOE balance after funding: {new_balance / 10**18:,.2f}")
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
    Run the TJ Wide-Range Accumulator strategy directly with injected market data.

    Args:
        force_action: Action to force ('open' or 'close')

    Returns:
        Exit code (0 = success)
    """
    print(f"\n{'=' * 60}")
    print("RUNNING TJ WIDE-RANGE ACCUMULATOR STRATEGY DIRECTLY")
    print(f"{'=' * 60}")
    print(f"Force action: {force_action.upper()}")

    from almanak.framework.strategies import MarketSnapshot, TokenBalance
    from strategies.tests.lp.tj_wide_accumulator.strategy import (
        TJWideAccumulatorConfig,
        TJWideAccumulatorStrategy,
    )

    # Strategy parameters
    range_width_pct = Decimal("0.15")  # 15% range
    joe_price = Decimal("0.4")  # ~$0.4 per JOE
    wavax_price = Decimal("30")  # ~$30 per WAVAX
    # JOE/WAVAX ratio: 0.4/30 = 0.0133
    joe_wavax_price = joe_price / wavax_price

    # Create config
    config = TJWideAccumulatorConfig(
        chain="avalanche",
        network="anvil",
        pool="JOE/WAVAX/20",
        range_width_pct=range_width_pct,
        amount_x=Decimal("15"),  # ~$6 worth of JOE
        amount_y=Decimal("0.15"),  # ~$4.5 worth of WAVAX
        num_bins=21,
        rebalance_price_threshold_pct=Decimal("0.07"),  # 7% threshold
        rebalance_time_days=7,
        force_action=force_action,
    )

    # Create strategy instance
    strategy = TJWideAccumulatorStrategy(
        config=config,
        chain="avalanche",
        wallet_address=ANVIL_WALLET,
    )

    # Create market snapshot
    market = MarketSnapshot(chain="avalanche", wallet_address=ANVIL_WALLET)

    # Set prices
    market.set_price("JOE", joe_price)
    market.set_price("WAVAX", wavax_price)

    # Set balances
    market.set_balance(
        "JOE",
        TokenBalance(
            symbol="JOE",
            balance=FUND_AMOUNT_JOE,
            balance_usd=FUND_AMOUNT_JOE * joe_price,
            address=JOE_ADDRESS,
        ),
    )
    market.set_balance(
        "WAVAX",
        TokenBalance(
            symbol="WAVAX",
            balance=FUND_AMOUNT_WAVAX,
            balance_usd=FUND_AMOUNT_WAVAX * wavax_price,
            address=WAVAX_ADDRESS,
        ),
    )

    # Print expected wide range bounds
    print(f"\n{'=' * 60}")
    print("EXPECTED WIDE RANGE BOUNDS")
    print(f"{'=' * 60}")

    half_width = range_width_pct / Decimal("2")
    range_lower = joe_wavax_price * (Decimal("1") - half_width)
    range_upper = joe_wavax_price * (Decimal("1") + half_width)
    total_width = range_upper - range_lower

    print(f"JOE Price: ${joe_price}")
    print(f"WAVAX Price: ${wavax_price}")
    print(f"JOE/WAVAX Ratio: {joe_wavax_price:.6f}")
    print("")
    print(f"Range Width: {range_width_pct * 100}% (+/- {half_width * 100}%)")
    print(f"Range Lower: {range_lower:.6f} (-{half_width * 100}%)")
    print(f"Range Upper: {range_upper:.6f} (+{half_width * 100}%)")
    print(f"Total Range Width: {total_width:.6f}")
    print("")
    print("HYBRID REBALANCING:")
    print("  - Time-based: Rebalance after 7 days")
    print("  - Price-based: Rebalance when price moves >7%")
    print("  - Bins: 21 bins for liquidity distribution")

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
                print(f"Amount0 (JOE): {intent.amount0}")
            if hasattr(intent, "amount1"):
                print(f"Amount1 (WAVAX): {intent.amount1}")
            if hasattr(intent, "range_lower"):
                print(f"Range Lower: {intent.range_lower}")
            if hasattr(intent, "range_upper"):
                print(f"Range Upper: {intent.range_upper}")
            if hasattr(intent, "protocol"):
                print(f"Protocol: {intent.protocol}")

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

    parser = argparse.ArgumentParser(description="Run TJ Wide-Range Accumulator Strategy on Anvil")
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
    print("ALMANAK TEST - TJ WIDE-RANGE ACCUMULATOR ON ANVIL")
    print("=" * 60)
    print("\nThis test runs the TJWideAccumulatorStrategy through the full stack:")
    print("  1. Anvil fork of Avalanche C-Chain")
    print("  2. Fund wallet with JOE + WAVAX")
    print("  3. Run strategy directly with injected market data")
    print("  4. Verify wide range bounds show proper distribution")
    print("")
    print("WIDE RANGE STRATEGY:")
    print("  - Range Width: 15% (+/-7.5% from current)")
    print("  - Bins: 21 for wide liquidity distribution")
    print("  - Rebalance: 7 days OR 7% price movement")
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

        if not fund_wallet_with_joe(ANVIL_WALLET, int(FUND_AMOUNT_JOE)):
            print("Failed to fund wallet with JOE (continuing anyway)")

        # Run strategy directly
        exit_code = run_strategy_directly(force_action=args.action)

        if exit_code == 0:
            print(f"\n{'=' * 60}")
            print("SUCCESS!")
            print(f"{'=' * 60}")
            print("\nTJ Wide-Range Accumulator strategy executed successfully.")
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
