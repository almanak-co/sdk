#!/usr/bin/env python3
"""
===============================================================================
TUTORIAL: Running a TraderJoe LP Strategy on Anvil (Local Fork)
===============================================================================

This script demonstrates how to test a TraderJoe V2 LP strategy on Anvil.
It forks Avalanche C-Chain and tests the Liquidity Book LP operations.

WHAT THIS SCRIPT DOES:
----------------------
1. Starts an Anvil fork of Avalanche C-Chain
2. Funds the test wallet with WAVAX and USDC
3. Runs the strategy via the CLI runner
4. The CLI handles market data, compilation, and execution

PREREQUISITES:
--------------
1. Foundry installed (provides anvil and cast)
   curl -L https://foundry.paradigm.xyz | bash && foundryup

2. RPC URL in .env file:
   ALMANAK_AVALANCHE_RPC_URL=https://api.avax.network/ext/bc/C/rpc

3. Python dependencies installed:
   uv sync

USAGE:
------
    python strategies/demo/traderjoe_lp/run_anvil.py

    # With custom options:
    python strategies/demo/traderjoe_lp/run_anvil.py --action open
    python strategies/demo/traderjoe_lp/run_anvil.py --action close

===============================================================================
"""

import os
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
from dotenv import load_dotenv

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

# Whale addresses for funding (large holders on Avalanche)
# Using Aave V3 pool as USDC whale
USDC_WHALE = "0x625E7708f30cA75bfd92586e17077590C60eb4cD"  # Aave aUSDC on Avalanche

# Amounts to fund
FUND_AMOUNT_USDC = 100  # 100 USDC
FUND_AMOUNT_WAVAX = Decimal("5")  # 5 WAVAX (~$150)

# Anvil settings
ANVIL_PORT = 8547  # Avalanche chain default port (matches gateway expectation)
ANVIL_RPC = f"http://127.0.0.1:{ANVIL_PORT}"

# Avalanche C-Chain ID
AVALANCHE_CHAIN_ID = 43114


# =============================================================================
# ANVIL MANAGER
# =============================================================================


class AnvilManager:
    """Manages the Anvil fork lifecycle."""

    def __init__(self, fork_url: str, port: int = 8547, chain_id: int = 43114):
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


def fund_wallet_with_usdc(wallet: str, amount_usdc: int) -> bool:
    """Fund wallet with USDC using whale impersonation (most reliable method)."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_usdc} USDC")
    print(f"{'=' * 60}")

    # Use impersonation method directly - it's more reliable
    return fund_wallet_with_usdc_impersonate(wallet, amount_usdc)


def fund_wallet_with_usdc_impersonate(wallet: str, amount_usdc: int) -> bool:
    """Fund wallet with USDC by impersonating a whale (fallback)."""
    print("Trying whale impersonation method...")

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
        print(f"ERROR: Impersonation method failed: {e}")
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

        # Wrap AVAX to WAVAX by calling deposit() on WAVAX contract
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
# STRATEGY EXECUTION
# =============================================================================


def run_strategy_via_cli(force_action: str = "open") -> int:
    """
    Run the TraderJoe LP strategy through the CLI runner.

    Returns:
        Exit code (0 = success)
    """
    print(f"\n{'=' * 60}")
    print("RUNNING TRADERJOE LP STRATEGY VIA CLI")
    print(f"{'=' * 60}")

    # Build environment for CLI
    env = os.environ.copy()
    env["ALMANAK_CHAIN"] = "avalanche"
    env["ALMANAK_RPC_URL"] = ANVIL_RPC
    env["ALMANAK_AVALANCHE_RPC_URL"] = ANVIL_RPC  # CLI checks chain-specific URL first
    env["ALMANAK_PRIVATE_KEY"] = ANVIL_PRIVATE_KEY

    # Build config
    import json
    import tempfile

    config = {
        "strategy_id": "demo_traderjoe_lp",
        "strategy_name": "demo_traderjoe_lp",
        "pool": "WAVAX/USDC/20",
        "range_width_pct": 0.10,
        "amount_x": "1.0",  # 1 WAVAX
        "amount_y": "30",  # 30 USDC (roughly matching at $30/AVAX)
        "bin_step": 20,  # 0.2% fee tier
        "num_bins": 11,  # Distribute across 11 bins
        "force_action": force_action,
        "chain": "avalanche",
    }

    # Write temp config
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(config, f)
        config_path = f.name

    try:
        # Run CLI
        strategy_dir = project_root / "strategies" / "demo" / "traderjoe_lp"
        cmd = [
            "uv",
            "run",
            "almanak",
            "strat",
            "run",
            "--working-dir",
            str(strategy_dir),
            "--config",
            config_path,
            "--once",
            "--verbose",
            "--network",
            "anvil",
        ]

        print(f"Command: {' '.join(cmd)}")
        print(f"Config: {json.dumps(config, indent=2)}")
        print(f"\n{'=' * 60}")
        print("CLI OUTPUT")
        print(f"{'=' * 60}\n")

        result = subprocess.run(
            cmd,
            cwd=str(project_root),
            env=env,
        )

        return result.returncode

    finally:
        os.unlink(config_path)


def run_direct_test() -> bool:
    """
    Run a direct test of the TraderJoe V2 adapter without the full CLI.

    This is useful for debugging and verifying the adapter works.
    """
    print(f"\n{'=' * 60}")
    print("DIRECT ADAPTER TEST")
    print(f"{'=' * 60}")

    try:
        from almanak.framework.connectors.traderjoe_v2 import (
            TraderJoeV2Adapter,
            TraderJoeV2Config,
        )

        # Create config
        config = TraderJoeV2Config(
            chain="avalanche",
            wallet_address=ANVIL_WALLET,
            rpc_url=ANVIL_RPC,
        )

        print("Creating TraderJoe V2 Adapter...")
        print(f"  Chain: {config.chain}")
        print(f"  Wallet: {config.wallet_address}")
        print(f"  RPC: {config.rpc_url}")

        adapter = TraderJoeV2Adapter(config)

        print("\nAdapter created successfully!")
        print(f"  Router: {adapter.sdk.router_address}")
        print(f"  Factory: {adapter.sdk.factory_address}")

        # Test bin math
        from decimal import Decimal

        price = Decimal("30")  # $30 per AVAX
        bin_step = 20

        # Calculate bin ID from price
        import math

        base = 1 + bin_step / 10000
        bin_id = int(math.log(float(price)) / math.log(base)) + 8388608
        print("\nBin Math Test:")
        print(f"  Price: ${price}")
        print(f"  Bin Step: {bin_step}")
        print(f"  Calculated Bin ID: {bin_id}")

        # Verify reverse calculation
        recovered_price = Decimal(str(base ** (bin_id - 8388608)))
        print(f"  Recovered Price: ${recovered_price:.4f}")

        return True

    except Exception as e:
        print(f"ERROR: Direct test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


# =============================================================================
# MAIN
# =============================================================================


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Run TraderJoeLPStrategy on Anvil")
    parser.add_argument(
        "--action",
        choices=["open", "close", "test"],
        default="open",
        help="LP action to test (default: open, use 'test' for adapter test only)",
    )
    parser.add_argument(
        "--skip-cli",
        action="store_true",
        help="Skip CLI execution (only fund wallet)",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("ALMANAK DEMO - TRADERJOE V2 LP STRATEGY ON ANVIL")
    print("=" * 60)
    print("\nThis test runs the TraderJoeLPStrategy through the full stack:")
    print("  1. Anvil fork of Avalanche C-Chain")
    print("  2. Fund wallet with WAVAX + USDC")
    print("  3. Run strategy via CLI runner")
    print("  4. CLI handles compilation and execution")
    print(f"\nAction: {args.action.upper()}")
    print("")

    # Get RPC URL - Avalanche specific
    fork_url = os.getenv("ALMANAK_AVALANCHE_RPC_URL")
    if not fork_url:
        # Use public Avalanche RPC as fallback
        fork_url = "https://api.avax.network/ext/bc/C/rpc"
        print("Note: Using public Avalanche RPC (set ALMANAK_AVALANCHE_RPC_URL for better reliability)")
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

        if not fund_wallet_with_usdc(ANVIL_WALLET, FUND_AMOUNT_USDC):
            print("Failed to fund wallet with USDC (continuing anyway for testing)")

        # Run direct adapter test if requested
        if args.action == "test":
            success = run_direct_test()
            sys.exit(0 if success else 1)

        # Skip CLI if requested
        if args.skip_cli:
            print("\n--skip-cli flag set, stopping before CLI execution")
            print("Wallet has been funded. You can now test manually.")
            input("Press Enter to stop Anvil...")
            sys.exit(0)

        # Run strategy via CLI
        exit_code = run_strategy_via_cli(force_action=args.action)

        if exit_code == 0:
            print(f"\n{'=' * 60}")
            print("SUCCESS!")
            print(f"{'=' * 60}")
            print("\nTraderJoe LP strategy executed successfully.")
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
