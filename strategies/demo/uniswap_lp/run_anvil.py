#!/usr/bin/env python3
"""
===============================================================================
TUTORIAL: Running an LP Strategy on Anvil (Local Fork)
===============================================================================

This script demonstrates how to test an LP strategy on Anvil. Unlike manual
testing, this script uses the CLI runner which provides the full stack:
- Price oracle (CoinGecko)
- Balance provider (Web3)
- Execution orchestrator (local signing + mempool submission)
- State management (SQLite)

WHAT THIS SCRIPT DOES:
----------------------
1. Starts an Anvil fork of Arbitrum mainnet
2. Funds the test wallet with WETH and USDC
3. Runs the strategy via the CLI runner
4. The CLI handles market data, compilation, and execution

PREREQUISITES:
--------------
1. Foundry installed (provides anvil and cast)
   curl -L https://foundry.paradigm.xyz | bash && foundryup

2. RPC URL in .env file:
   ALMANAK_ARBITRUM_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY

3. Python dependencies installed:
   uv sync

USAGE:
------
    python strategies/demo/uniswap_lp/run_anvil.py

    # With custom options:
    python strategies/demo/uniswap_lp/run_anvil.py --action open
    python strategies/demo/uniswap_lp/run_anvil.py --action close --position-id 123456

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

# Arbitrum mainnet token addresses
USDC_ADDRESS = "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8"  # USDC.e (bridged)
WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

# Whale addresses for funding
USDC_WHALE = "0x489ee077994B6658eAfA855C308275EAd8097C4A"  # Aave V3 pool

# Amounts to fund
FUND_AMOUNT_USDC = 1000  # 1000 USDC
FUND_AMOUNT_WETH = Decimal("0.5")  # 0.5 WETH

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
# STRATEGY EXECUTION VIA CLI
# =============================================================================


def run_strategy_via_cli(force_action: str = "open", position_id: str | None = None) -> int:
    """
    Run the LP strategy through the CLI runner.

    This is the recommended way to run strategies - the CLI handles:
    - Price oracle setup (CoinGecko)
    - Balance provider setup (Web3)
    - Intent compilation
    - Transaction execution
    - State management

    Returns:
        Exit code (0 = success)
    """
    print(f"\n{'=' * 60}")
    print("RUNNING LP STRATEGY VIA CLI")
    print(f"{'=' * 60}")

    # Build environment for CLI
    env = os.environ.copy()
    env["ALMANAK_CHAIN"] = "arbitrum"
    env["ALMANAK_RPC_URL"] = ANVIL_RPC
    env["ALMANAK_ARBITRUM_RPC_URL"] = ANVIL_RPC  # CLI checks chain-specific URL first
    env["ALMANAK_PRIVATE_KEY"] = ANVIL_PRIVATE_KEY

    # Build config overrides
    # The CLI loads config.json, but we can override via a custom config
    import json
    import tempfile

    config = {
        "strategy_id": "demo_uniswap_lp",
        "strategy_name": "demo_uniswap_lp",
        "pool": "WETH/USDC.e/500",
        "range_width_pct": 0.20,
        "amount0": "0.1",  # 0.1 WETH
        "amount1": "340",  # 340 USDC (roughly matching at $3400/ETH)
        "force_action": force_action,
        "chain": "arbitrum",
    }

    if position_id:
        config["position_id"] = position_id

    # Write temp config
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(config, f)
        config_path = f.name

    try:
        # Run CLI
        strategy_dir = project_root / "strategies" / "demo" / "uniswap_lp"
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
        # Cleanup temp config
        os.unlink(config_path)


# =============================================================================
# MAIN
# =============================================================================


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Run UniswapLPStrategy on Anvil")
    parser.add_argument(
        "--action",
        choices=["open", "close"],
        default="open",
        help="LP action to test (default: open)",
    )
    parser.add_argument(
        "--position-id",
        type=str,
        default=None,
        help="Position ID for close action",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("ALMANAK DEMO - UNISWAP LP STRATEGY ON ANVIL")
    print("=" * 60)
    print("\nThis test runs the UniswapLPStrategy through the full stack:")
    print("  1. Anvil fork of Arbitrum")
    print("  2. Fund wallet with WETH + USDC")
    print("  3. Run strategy via CLI runner")
    print("  4. CLI handles compilation and execution")
    print(f"\nAction: {args.action.upper()}")
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
        # Fund wallet
        if not fund_wallet_with_usdc(ANVIL_WALLET, FUND_AMOUNT_USDC):
            print("Failed to fund wallet with USDC")
            sys.exit(1)

        if not fund_wallet_with_weth(ANVIL_WALLET, FUND_AMOUNT_WETH):
            print("Failed to fund wallet with WETH")
            sys.exit(1)

        # Run strategy via CLI
        exit_code = run_strategy_via_cli(
            force_action=args.action,
            position_id=args.position_id,
        )

        if exit_code == 0:
            print(f"\n{'=' * 60}")
            print("SUCCESS!")
            print(f"{'=' * 60}")
            print("\nLP strategy executed successfully through the CLI runner.")
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
