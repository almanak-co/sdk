#!/usr/bin/env python3
"""
===============================================================================
TUTORIAL: Running the Fluid Vault Borrow Strategy on Anvil (Local Fork)
===============================================================================

WHAT THIS SCRIPT DOES:
----------------------
1. Starts an Anvil fork of Arbitrum mainnet (gateway port 8545)
2. Clears the 7702 delegation code anvil key0 inherits from mainnet
   (mandatory before any native-ETH step — Fluid Phase-0 finding 1)
3. Funds the test wallet with USDC (ETH comes native from Anvil)
4. Runs the strategy via the CLI runner: ONE atomic operate() opens the
   NFT-CDP (mint + ETH collateral + USDC debt)

PREREQUISITES:
--------------
1. Foundry installed:
   curl -L https://foundry.paradigm.xyz | bash && foundryup
2. RPC URL in .env:
   ALMANAK_ARBITRUM_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY

USAGE:
------
    python almanak/demo_strategies/fluid_borrow/run_anvil.py [action]

Actions:
    open    - Atomic open: collateral + borrow in one operate() (default)
    supply  - Add collateral only
    repay   - Partial repay (requires a previous open on the same Anvil)

    --skip-cli  - Start Anvil + fund the wallet, then block (CI sidecar mode)

===============================================================================
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

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from almanak.config.demo_runtime import (  # noqa: E402
    demo_anvil_port,
    demo_anvil_url,
    demo_chain_rpc_url,
    demo_fork_block,
    demo_subprocess_env,
    load_demo_dotenv,
)

load_demo_dotenv(project_root)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

ARBITRUM_ANVIL_PORT = demo_anvil_port("arbitrum", default=8545)  # gateway's Arbitrum port
ANVIL_URL = demo_anvil_url("arbitrum", default_port=8545)

# Anvil's first default account
TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

# Arbitrum native USDC (vault id 1 debt token); balances mapping slot 9.
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
USDC_BALANCE_SLOT = 9

BASE_STRATEGY_CONFIG = {
    "deployment_id": "fluid-borrow-test",
    "market_id": "0xeAbBfca72F8a8bf14C4ac59e69ECB2eB69F0811C",
    "collateral_token": "ETH",
    "collateral_amount": "0.2",
    "borrow_token": "USDC",
    "ltv_target": 0.3,
    "repay_amount": "50",
}


# =============================================================================
# Anvil lifecycle
# =============================================================================


class AnvilManager:
    """Manages the Anvil fork lifecycle."""

    def __init__(self, fork_url: str, port: int, chain_id: int = 42161):
        self.fork_url = fork_url
        self.port = port
        self.chain_id = chain_id
        self.process: subprocess.Popen | None = None
        # stderr goes to a file, NEVER subprocess.PIPE: an undrained pipe
        # fills its OS buffer and blocks anvil mid-run.
        self.stderr_log = Path("/tmp/fluid-borrow-anvil.log")

    def start(self) -> bool:
        print(f"\n{'=' * 60}\nSTARTING ANVIL FORK (arbitrum)\n{'=' * 60}")
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
        fork_block_env = demo_fork_block("arbitrum")
        if fork_block_env:
            cmd.extend(["--fork-block-number", fork_block_env])
            print(f"Pinning fork block to {fork_block_env}")
        try:
            # Append mode: Popen dup()s the fd, so closing our handle right
            # after spawn is safe and leaks nothing.
            with self.stderr_log.open("a") as stderr_log:
                self.process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=stderr_log)
            print(f"Anvil stderr -> {self.stderr_log}")
            print("Waiting for Anvil to fork (polling eth_blockNumber)...")
            if not self._wait_until_ready():
                stderr = ""
                try:
                    stderr = self.stderr_log.read_text()[-500:]
                except OSError:
                    pass
                print(f"ERROR: Anvil failed to become ready: {stderr}")
                self.stop()
                return False
            print(f"Anvil started on port {self.port}")
            return True
        except FileNotFoundError:
            print("ERROR: 'anvil' command not found! Install Foundry:")
            print("  curl -L https://foundry.paradigm.xyz | bash && foundryup")
            return False

    def _wait_until_ready(self, timeout_seconds: float = 60.0, poll_interval: float = 0.5) -> bool:
        """Poll eth_blockNumber until the fork answers (active readiness check).

        A fixed sleep either wastes time on fast forks or races slow RPC
        providers; polling the actual JSON-RPC surface does neither.
        """
        deadline = time.monotonic() + timeout_seconds
        url = f"http://127.0.0.1:{self.port}"
        while time.monotonic() < deadline:
            if self.process is not None and self.process.poll() is not None:
                return False  # anvil died — no point polling further
            try:
                # Per-probe timeout: a hanging cast call must not be able to
                # outlive the outer deadline.
                probe = subprocess.run(
                    ["cast", "rpc", "eth_blockNumber", "--rpc-url", url],
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
            except subprocess.TimeoutExpired:
                continue
            if probe.returncode == 0 and probe.stdout.strip():
                return True
            time.sleep(poll_interval)
        return False

    def stop(self):
        if self.process:
            print("\nStopping Anvil...")
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            print("Anvil stopped.")


# =============================================================================
# Helpers
# =============================================================================


def clear_7702_sweeper_code() -> None:
    """Clear the 7702 delegation code anvil key0 inherits from mainnet.

    Mandatory before any native-ETH-receiving step (Fluid Phase-0 finding 1,
    re-confirmed 2026-06-12): the delegated code sweeps incoming ETH.
    """
    subprocess.run(
        ["cast", "rpc", "anvil_setCode", TEST_WALLET, "0x", "--rpc-url", ANVIL_URL],
        capture_output=True,
        check=True,
    )
    logger.info("Cleared 7702 delegation code on %s", TEST_WALLET)


def fund_wallet_with_usdc(amount: Decimal) -> None:
    """Fund the test wallet with Arbitrum native USDC via storage-slot write."""
    amount_wei = int(amount * 10**6)
    storage_slot = subprocess.run(
        ["cast", "index", "address", TEST_WALLET, str(USDC_BALANCE_SLOT)],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    subprocess.run(
        [
            "cast",
            "rpc",
            "anvil_setStorageAt",
            USDC_ADDRESS,
            storage_slot,
            f"0x{amount_wei:064x}",
            "--rpc-url",
            ANVIL_URL,
        ],
        capture_output=True,
        check=True,
    )
    balance_out = subprocess.run(
        ["cast", "call", USDC_ADDRESS, "balanceOf(address)(uint256)", TEST_WALLET, "--rpc-url", ANVIL_URL],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    balance_wei = int(balance_out.split()[0].replace(",", ""))
    logger.info("Wallet USDC balance: %s", Decimal(balance_wei) / Decimal(10**6))
    if balance_wei < amount_wei:
        raise ValueError(f"Failed to fund wallet: got {balance_wei}, expected {amount_wei}")


# =============================================================================
# Strategy execution via CLI
# =============================================================================


def run_strategy_via_cli(action: str, config: dict) -> int:
    """Run the Fluid borrow strategy through the CLI runner."""
    print(f"\n{'=' * 60}\nRUNNING FLUID BORROW STRATEGY - {action.upper()}\n{'=' * 60}")
    env = demo_subprocess_env(chain="arbitrum", rpc_url=ANVIL_URL, private_key=TEST_PRIVATE_KEY)

    config_file = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config, f, indent=2)
            config_file = f.name

        cmd = [
            "uv",
            "run",
            "almanak",
            "strat",
            "run",
            "--working-dir",
            str(Path(__file__).parent),
            "--config",
            config_file,
            "--once",
            "--verbose",
            "--network",
            "anvil",
        ]
        print(f"Command: {' '.join(cmd)}")
        result = subprocess.run(cmd, cwd=str(project_root), env=env)
        return result.returncode
    finally:
        if config_file:
            os.unlink(config_file)


# =============================================================================
# Main
# =============================================================================


def parse_args():
    parser = argparse.ArgumentParser(description="Run the Fluid vault borrow strategy on an Anvil fork")
    parser.add_argument(
        "action",
        nargs="?",
        default="open",
        choices=["open", "supply", "repay"],
        help="Action to test (default: open — the atomic collateral+borrow operate())",
    )
    parser.add_argument(
        "--skip-cli",
        action="store_true",
        help="Skip CLI execution (only fund wallet). Keeps Anvil running while this process lives.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    print("=" * 70)
    print("Fluid Vault Borrow Strategy - Anvil Test")
    print(f"Action: {args.action.upper()}")
    print("=" * 70)

    fork_url = demo_chain_rpc_url("arbitrum")
    if not fork_url:
        print("ERROR: No RPC URL found in .env. Add:")
        print("  ALMANAK_ARBITRUM_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY")
        sys.exit(1)

    anvil = AnvilManager(fork_url, ARBITRUM_ANVIL_PORT)
    if not anvil.start():
        sys.exit(1)

    try:
        # Mandatory native-ETH hygiene + repay funding for any action.
        clear_7702_sweeper_code()
        fund_wallet_with_usdc(Decimal("1000"))

        if args.skip_cli:
            # CI sidecar-regression mode: the workflow starts the gateway
            # AFTER this script and holds stdin open.
            print("\n--skip-cli flag set, stopping before CLI execution")
            print("Wallet has been funded. You can now test manually.")
            try:
                # Blocks while the CI workflow holds stdin open (the keep-alive
                # contract); EOF (stdin closed / non-TTY) is the stop signal,
                # not a crash.
                input("Press Enter to stop Anvil...")
            except EOFError:
                print("stdin closed (non-TTY) — stopping Anvil")
            sys.exit(0)

        config = BASE_STRATEGY_CONFIG.copy()
        config["force_action"] = args.action
        exit_code = run_strategy_via_cli(args.action, config)

        print(f"\n{'=' * 60}")
        print("SUCCESS!" if exit_code == 0 else f"EXECUTION COMPLETED WITH ERRORS (exit {exit_code})")
        print(f"{'=' * 60}")
        sys.exit(exit_code)

    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as exc:
        print(f"\nError: {exc}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
    finally:
        anvil.stop()


if __name__ == "__main__":
    main()
