#!/usr/bin/env python3
"""
===============================================================================
TUTORIAL: Running a GMX Perpetuals Strategy on Anvil (Local Fork)
===============================================================================

This script demonstrates how to test a GMX perpetual futures strategy on Anvil.

WHAT THIS SCRIPT DOES:
----------------------
1. Starts an Anvil fork of Arbitrum mainnet
2. Funds the test wallet with WETH (for collateral)
3. Funds the test wallet with ETH (for execution fees)
4. Runs the strategy via the CLI runner
5. The strategy opens a perpetual position on GMX V2

PREREQUISITES:
--------------
1. Foundry installed:
   curl -L https://foundry.paradigm.xyz | bash && foundryup

2. RPC URL in .env file:
   ALMANAK_ARBITRUM_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY

IMPORTANT NOTES:
----------------
GMX V2 perpetual orders are asynchronous:
- When you submit an order, it goes to the GMX order book
- Keepers execute the order after a short delay
- On Anvil, there are no keepers, so orders won't execute
- This test validates the order creation, not full execution

USAGE:
------
    python strategies/demo/gmx_perps/run_anvil.py

    # Force specific action:
    python strategies/demo/gmx_perps/run_anvil.py --action open
    python strategies/demo/gmx_perps/run_anvil.py --action close

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

# Anvil's first default account
ANVIL_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
ANVIL_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

# Arbitrum token addresses
WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

# Amounts to fund
FUND_AMOUNT_WETH = Decimal("0.5")  # 0.5 WETH for collateral
FUND_AMOUNT_ETH = Decimal("0.1")  # 0.1 ETH for execution fees

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
            "42161",
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


def fund_wallet_with_eth(wallet: str, amount_eth: Decimal) -> bool:
    """Fund wallet with ETH for gas and execution fees."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_eth} ETH")
    print(f"{'=' * 60}")

    amount_wei = int(amount_eth * 10**18)

    try:
        # Set ETH balance using Anvil RPC
        run_cast(
            [
                "rpc",
                "anvil_setBalance",
                wallet,
                hex(amount_wei + 10 * 10**18),  # Extra for gas
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        # Verify
        balance = run_cast(
            [
                "balance",
                wallet,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        print(f"Wallet ETH balance: {balance}")
        return True

    except Exception as e:
        print(f"ERROR: Failed to fund wallet with ETH: {e}")
        return False


def fund_wallet_with_weth(wallet: str, amount_weth: Decimal) -> bool:
    """Fund wallet with WETH by wrapping ETH."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_weth} WETH")
    print(f"{'=' * 60}")

    amount_wei = int(amount_weth * 10**18)

    try:
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
        print(f"ERROR: Failed to fund wallet with WETH: {e}")
        return False


# =============================================================================
# STRATEGY EXECUTION VIA CLI
# =============================================================================


def run_strategy_via_cli(force_action: str = "open") -> int:
    """
    Run the GMX perps strategy through the CLI runner.

    Returns:
        Exit code (0 = success)
    """
    print(f"\n{'=' * 60}")
    print("RUNNING GMX PERPS STRATEGY VIA CLI")
    print(f"{'=' * 60}")

    # Build environment for CLI
    env = os.environ.copy()
    env["ALMANAK_CHAIN"] = "arbitrum"
    env["ALMANAK_RPC_URL"] = ANVIL_RPC
    env["ALMANAK_PRIVATE_KEY"] = ANVIL_PRIVATE_KEY

    # Build config
    import json
    import tempfile

    config = {
        "strategy_id": "demo_gmx_perps",
        "strategy_name": "demo_gmx_perps",
        "market": "ETH/USD",
        "collateral_token": "WETH",
        "collateral_amount": "0.1",
        "leverage": "2.0",
        "is_long": True,
        "hold_minutes": 60,
        "max_slippage_pct": 1.0,
        "force_action": force_action,
        "chain": "arbitrum",
    }

    # Write temp config
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(config, f)
        config_path = f.name

    try:
        # Run CLI
        strategy_dir = project_root / "strategies" / "demo" / "gmx_perps"
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


# =============================================================================
# MAIN
# =============================================================================


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Run GMXPerpsStrategy on Anvil")
    parser.add_argument(
        "--action",
        choices=["open", "close"],
        default="open",
        help="Action to test (default: open)",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("ALMANAK DEMO - GMX PERPETUALS STRATEGY ON ANVIL")
    print("=" * 60)
    print("\nThis test runs the GMXPerpsStrategy through the full stack:")
    print("  1. Anvil fork of Arbitrum")
    print("  2. Fund wallet with ETH (for gas + execution fee)")
    print("  3. Fund wallet with WETH (for collateral)")
    print("  4. Run strategy via CLI runner")
    print("  5. Strategy opens a perpetual position on GMX V2")
    print(f"\nAction: {args.action.upper()}")
    print("")
    print("NOTE: GMX orders are asynchronous - keepers execute them.")
    print("On Anvil, there are no keepers, so orders will be created")
    print("but not executed. This tests the order creation flow.")
    print("")

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
        # Fund wallet with ETH
        if not fund_wallet_with_eth(ANVIL_WALLET, FUND_AMOUNT_ETH):
            print("Failed to fund wallet with ETH")
            sys.exit(1)

        # Fund wallet with WETH
        if not fund_wallet_with_weth(ANVIL_WALLET, FUND_AMOUNT_WETH):
            print("Failed to fund wallet with WETH")
            sys.exit(1)

        # Run strategy via CLI
        exit_code = run_strategy_via_cli(force_action=args.action)

        if exit_code == 0:
            print(f"\n{'=' * 60}")
            print("SUCCESS!")
            print(f"{'=' * 60}")
            print("\nGMX perpetuals strategy executed successfully.")
            print("Check the CLI output above for execution details.")
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
