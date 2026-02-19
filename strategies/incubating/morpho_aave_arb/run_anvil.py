#!/usr/bin/env python3
"""
Test Morpho-Aave Rate Arbitrage Strategy on Anvil Fork.

Tests:
  1. Deploy to Morpho (force_protocol=morpho) -- supply wstETH as collateral
  2. Deploy to Aave (force_protocol=aave) -- supply wstETH to Aave V3

Prerequisites:
    1. .env file with ALCHEMY_API_KEY
    2. Foundry installed (anvil, cast)

Usage:
    python strategies/incubating/morpho_aave_arb/run_anvil.py morpho   # Test Morpho supply
    python strategies/incubating/morpho_aave_arb/run_anvil.py aave     # Test Aave supply
    python strategies/incubating/morpho_aave_arb/run_anvil.py both     # Test both (default)
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import tempfile
import time
from decimal import Decimal
from pathlib import Path

# Add project root to path -- handle worktree case where .env is in main repo
project_root = Path(__file__).parent.parent.parent.parent
# If in a worktree (.claude/worktrees/X), the main repo root is 4 levels up from the worktree
main_repo_root = project_root
if ".claude/worktrees" in str(project_root):
    # Navigate from worktree root to main repo: .claude/worktrees/morpho-3strats -> repo root
    main_repo_root = project_root.parent.parent.parent
sys.path.insert(0, str(main_repo_root))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(main_repo_root / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration
ANVIL_PORT = 8549
ANVIL_URL = f"http://127.0.0.1:{ANVIL_PORT}"
TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
WSTETH_ADDRESS = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"

STRATEGY_DIR = Path(__file__).parent
# For CLI commands, use the main repo root (where uv.lock, pyproject.toml live)
CLI_CWD = main_repo_root


def start_anvil() -> subprocess.Popen | None:
    """Start Anvil fork of Ethereum."""
    fork_url = os.getenv("ALMANAK_ETHEREUM_RPC_URL") or os.getenv("ALMANAK_RPC_URL")
    if not fork_url:
        alchemy_key = os.getenv("ALCHEMY_API_KEY")
        if alchemy_key:
            fork_url = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_key}"
        else:
            print("ERROR: No RPC URL or ALCHEMY_API_KEY found in .env")
            return None

    print(f"Starting Anvil fork on port {ANVIL_PORT}...")
    try:
        proc = subprocess.Popen(
            [
                "anvil",
                "--fork-url", fork_url,
                "--port", str(ANVIL_PORT),
                "--chain-id", "1",
                "--timeout", "60000",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )
        time.sleep(8)
        if proc.poll() is not None:
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            print(f"ERROR: Anvil failed to start: {stderr[:500]}")
            return None
        print(f"Anvil running on port {ANVIL_PORT}")
        return proc
    except FileNotFoundError:
        print("ERROR: 'anvil' not found. Install Foundry: curl -L https://foundry.paradigm.xyz | bash && foundryup")
        return None


def fund_erc20(token_address: str, token_symbol: str, amount: Decimal,
                decimals: int = 18, balance_slot: int = 0) -> bool:
    """Fund test wallet with ERC20 token via storage manipulation."""
    logger.info(f"Funding {TEST_WALLET} with {amount} {token_symbol} (slot={balance_slot})...")
    try:
        # Get storage slot for balanceOf mapping
        result = subprocess.run(
            ["cast", "index", "address", TEST_WALLET, str(balance_slot)],
            capture_output=True, text=True, check=True, timeout=15,
        )
        slot = result.stdout.strip()

        amount_wei = int(amount * 10**decimals)
        amount_hex = f"0x{amount_wei:064x}"
        subprocess.run(
            ["cast", "rpc", "anvil_setStorageAt", token_address, slot, amount_hex,
             "--rpc-url", ANVIL_URL],
            capture_output=True, check=True, timeout=15,
        )

        # Mine a block to apply changes
        subprocess.run(
            ["cast", "rpc", "evm_mine", "--rpc-url", ANVIL_URL],
            capture_output=True, check=True, timeout=15,
        )

        # Verify using cast call with explicit return type
        bal = subprocess.run(
            ["cast", "call", token_address,
             "balanceOf(address)(uint256)", TEST_WALLET,
             "--rpc-url", ANVIL_URL],
            capture_output=True, text=True, timeout=15,
        )
        if bal.returncode != 0:
            logger.warning(f"Balance check failed (may still be funded): {bal.stderr}")
            return True  # Assume success if storage set worked

        raw = bal.stdout.strip()
        # cast may return "2000000000000000000 [2e18]" or just a number
        balance_str = raw.split()[0].replace(",", "") if raw else "0"
        try:
            balance_wei = int(balance_str)
        except ValueError:
            balance_wei = int(balance_str, 16) if balance_str.startswith("0x") else 0
        balance = Decimal(balance_wei) / Decimal(10**decimals)
        logger.info(f"{token_symbol} balance: {balance}")
        return balance >= amount
    except Exception as e:
        logger.error(f"Failed to fund {token_symbol}: {e}")
        return False


def fund_wsteth(amount: Decimal) -> bool:
    """Fund test wallet with wstETH (balance slot 0)."""
    return fund_erc20(WSTETH_ADDRESS, "wstETH", amount, decimals=18, balance_slot=0)


def fund_eth(amount: Decimal) -> bool:
    """Fund test wallet with ETH."""
    logger.info(f"Funding {TEST_WALLET} with {amount} ETH...")
    try:
        amount_hex = hex(int(amount * 10**18))
        subprocess.run(
            ["cast", "rpc", "anvil_setBalance", TEST_WALLET, amount_hex,
             "--rpc-url", ANVIL_URL],
            capture_output=True, check=True,
        )
        return True
    except Exception as e:
        logger.error(f"Failed to fund ETH: {e}")
        return False


def run_strategy(force_protocol: str) -> int:
    """Run the strategy via CLI."""
    config = {
        "strategy_id": "morpho_aave_arb_test",
        "strategy_name": "demo_morpho_aave_arb",
        "token": "wstETH",
        "morpho_market_id": "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
        "deploy_amount": "0.5",
        "min_spread_bps": 50,
        "cooldown_seconds": 0,
        "morpho_apy_override": None,
        "aave_apy_override": None,
        "force_protocol": force_protocol,
        "chain": "ethereum",
    }

    env = os.environ.copy()
    env["ALMANAK_CHAIN"] = "ethereum"
    env["ALMANAK_PRIVATE_KEY"] = TEST_PRIVATE_KEY

    config_file = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config, f, indent=2)
            config_file = f.name

        cmd = [
            "uv", "run", "almanak", "strat", "run",
            "--working-dir", str(STRATEGY_DIR),
            "--config", config_file,
            "--once",
            "--fresh",
            "--verbose",
            "--network", "anvil",
        ]

        print(f"\nCommand: {' '.join(cmd)}")
        print(f"Config: force_protocol={force_protocol}")
        print(f"{'=' * 60}")

        result = subprocess.run(cmd, cwd=str(CLI_CWD), env=env)
        return result.returncode
    finally:
        if config_file:
            os.unlink(config_file)


def main():
    parser = argparse.ArgumentParser(description="Test Morpho-Aave Arb on Anvil")
    parser.add_argument("action", nargs="?", default="both",
                        choices=["morpho", "aave", "both"],
                        help="Protocol to test (default: both)")
    args = parser.parse_args()

    print("=" * 60)
    print("Morpho-Aave Rate Arbitrage - Anvil Test")
    print(f"Action: {args.action.upper()}")
    print("=" * 60)

    anvil_proc = start_anvil()
    if not anvil_proc:
        sys.exit(1)

    try:
        # Fund wallet
        fund_eth(Decimal("100"))
        if not fund_wsteth(Decimal("2")):
            print("ERROR: Failed to fund wstETH")
            sys.exit(1)

        results = {}

        if args.action in ("morpho", "both"):
            print(f"\n{'=' * 60}")
            print("TEST 1: Supply to Morpho Blue")
            print(f"{'=' * 60}")
            rc = run_strategy("morpho")
            results["morpho"] = rc
            if rc == 0:
                print("Morpho supply: SUCCESS")
            else:
                print(f"Morpho supply: FAILED (exit={rc})")

        if args.action in ("aave", "both"):
            # If testing both, restart Anvil for clean state
            if args.action == "both":
                print("\nRestarting Anvil for clean state...")
                anvil_proc.terminate()
                anvil_proc.wait(timeout=5)
                anvil_proc = start_anvil()
                if not anvil_proc:
                    sys.exit(1)
                fund_eth(Decimal("100"))
                if not fund_wsteth(Decimal("2")):
                    print("ERROR: Failed to fund wstETH for Aave test")
                    sys.exit(1)

            print(f"\n{'=' * 60}")
            print("TEST 2: Supply to Aave V3")
            print(f"{'=' * 60}")
            rc = run_strategy("aave")
            results["aave"] = rc
            if rc == 0:
                print("Aave supply: SUCCESS")
            else:
                print(f"Aave supply: FAILED (exit={rc})")

        # Summary
        print(f"\n{'=' * 60}")
        print("SUMMARY")
        print(f"{'=' * 60}")
        all_pass = True
        for test, rc in results.items():
            status = "PASS" if rc == 0 else "FAIL"
            print(f"  {test}: {status}")
            if rc != 0:
                all_pass = False

        sys.exit(0 if all_pass else 1)

    except KeyboardInterrupt:
        print("\nInterrupted")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if anvil_proc:
            anvil_proc.terminate()
            try:
                anvil_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                anvil_proc.kill()
            print("Anvil stopped.")


if __name__ == "__main__":
    main()
