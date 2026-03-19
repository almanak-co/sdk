#!/usr/bin/env python3
"""
Run PancakeSwap V3 Simple Strategy on Anvil Fork.

This script tests the PancakeSwap V3 swap execution on a local Anvil fork.

Prerequisites:
    1. Start Anvil fork of Arbitrum on port 8545:
       anvil --fork-url https://arb-mainnet.g.alchemy.com/v2/$ALCHEMY_API_KEY --port 8545 --chain-id 42161

    2. Start the gateway in another terminal:
       uv run almanak gateway --network anvil

    3. Run this script:
       python strategies/demo/pancakeswap_simple/run_anvil.py

What it does:
    1. Funds a test wallet with WETH (by wrapping ETH)
    2. Runs the PancakeSwap swap strategy via CLI runner
    3. Shows execution status
"""

import json
import logging
import os
import subprocess
import sys
import tempfile
from decimal import Decimal
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
from dotenv import load_dotenv  # noqa: E402

load_dotenv(project_root / ".env")

from web3 import Web3  # noqa: E402

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================

# Anvil default URL
# Note: Gateway uses port 8545 for Arbitrum when network=anvil
# (see almanak/gateway/utils/rpc_provider.py ANVIL_CHAIN_PORTS)
ARBITRUM_ANVIL_PORT = int(os.environ.get("ANVIL_ARBITRUM_PORT", "8545"))
ARBITRUM_ANVIL_URL = f"http://127.0.0.1:{ARBITRUM_ANVIL_PORT}"
ANVIL_URL = os.environ.get("ANVIL_URL", ARBITRUM_ANVIL_URL)

# Test wallet (Anvil account 0)
TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

# Token addresses on Arbitrum
WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

# Base strategy configuration
BASE_STRATEGY_CONFIG = {
    "swap_amount_usd": "10",
    "max_slippage": "0.05",
    "from_token": "WETH",
    "to_token": "USDC",
}


# =============================================================================
# ANVIL MANAGER
# =============================================================================


class AnvilManager:
    """Manages the Anvil fork lifecycle."""

    def __init__(self, fork_url: str, port: int = 8545, chain_id: int = 42161):
        self.fork_url = fork_url
        self.port = port
        self.chain_id = chain_id
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
            str(self.chain_id),
            "--timeout",
            "60000",
        ]

        try:
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            print("Waiting for Anvil to fork (this may take ~10 seconds)...")
            import time
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
# Helpers
# =============================================================================


def run_cast(args: list[str], check: bool = True) -> str:
    """Run cast command and return output."""
    cmd = ["cast"] + args
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"Cast failed: {result.stderr}")
    return result.stdout.strip()


def fund_wallet_with_weth(amount_weth: Decimal) -> None:
    """Fund test wallet with WETH by wrapping ETH.

    Funds on the port that the gateway will use (8547 for Arbitrum).
    """
    logger.info(f"Funding wallet with {amount_weth} WETH on gateway's Arbitrum port ({ARBITRUM_ANVIL_PORT})...")

    # Set ETH balance
    run_cast(
        ["rpc", "anvil_setBalance", TEST_WALLET, hex(100 * 10**18), "--rpc-url", ARBITRUM_ANVIL_URL],
        check=False,
    )

    # Wrap ETH to WETH
    amount_wei = int(amount_weth * 10**18)
    run_cast([
        "send", WETH_ADDRESS, "--value", str(amount_wei),
        "--from", TEST_WALLET, "--private-key", TEST_PRIVATE_KEY,
        "--rpc-url", ARBITRUM_ANVIL_URL,
    ])

    # Verify balance
    balance_result = run_cast([
        "call", WETH_ADDRESS, "balanceOf(address)(uint256)", TEST_WALLET,
        "--rpc-url", ARBITRUM_ANVIL_URL,
    ])
    balance_str = balance_result.split()[0].replace(",", "")
    balance_wei = int(balance_str)
    balance = Decimal(balance_wei) / Decimal(10**18)
    logger.info(f"Wallet WETH balance on port {ARBITRUM_ANVIL_PORT}: {balance}")

    if balance < amount_weth:
        raise ValueError(f"Failed to fund wallet: got {balance}, expected {amount_weth}")


def check_anvil_connection() -> Web3 | None:
    """Check if Anvil is running and return Web3 instance."""
    try:
        w3 = Web3(Web3.HTTPProvider(ANVIL_URL))
        if w3.is_connected():
            chain_id = w3.eth.chain_id
            block = w3.eth.block_number
            logger.info(f"Connected to Anvil - Chain ID: {chain_id}, Block: {block}")
            return w3
        else:
            logger.error("Could not connect to Anvil")
            return None
    except Exception as e:
        logger.error(f"Error connecting to Anvil: {e}")
        return None


# =============================================================================
# Strategy Execution via CLI
# =============================================================================


def run_strategy_via_cli(config: dict) -> int:
    """
    Run the PancakeSwap strategy through the CLI runner.

    Args:
        config: Strategy configuration dict

    Returns:
        Exit code (0 = success)
    """
    print(f"\n{'=' * 60}")
    print("RUNNING PANCAKESWAP V3 SIMPLE SWAP")
    print(f"{'=' * 60}")

    # Build environment for CLI
    env = os.environ.copy()
    env["ALMANAK_CHAIN"] = "arbitrum"
    env["ALMANAK_RPC_URL"] = ANVIL_URL
    env["ALMANAK_PRIVATE_KEY"] = TEST_PRIVATE_KEY

    # Write config to temp file
    config_file = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config, f, indent=2)
            config_file = f.name

        # Run CLI via uv run almanak
        cmd = [
            "uv",
            "run",
            "almanak",
            "strat",
            "run",
            "--working-dir",
            str(project_root / "strategies" / "demo" / "pancakeswap_simple"),
            "--config",
            config_file,
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
        if config_file:
            os.unlink(config_file)


# =============================================================================
# Main
# =============================================================================


def main():
    """Run the PancakeSwap simple strategy on Anvil."""
    print("=" * 70)
    print("PancakeSwap V3 Simple Strategy - Anvil Test")
    print("=" * 70)

    # Get RPC URL for forking
    fork_url = os.getenv("ALMANAK_ARBITRUM_RPC_URL") or os.getenv("ALMANAK_RPC_URL")
    if not fork_url:
        print("ERROR: No RPC URL found in .env file")
        print("\nAdd one of these to .env:")
        print("  ALMANAK_ARBITRUM_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY")
        sys.exit(1)

    # Start Anvil
    anvil = AnvilManager(fork_url, ARBITRUM_ANVIL_PORT, chain_id=42161)
    if not anvil.start():
        sys.exit(1)

    try:
        # Check if gateway is running (required)
        import socket

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                result = sock.connect_ex(("localhost", 50051))
            if result != 0:
                print("\nERROR: Gateway is not running!")
                print("Please start the gateway in another terminal:")
                print("  uv run almanak gateway --network anvil")
                sys.exit(1)
        except Exception as e:
            logger.error(f"Gateway connectivity check failed: {e}")
            print("\nERROR: Could not check gateway connectivity")
            print("Please ensure the gateway is running:")
            print("  uv run almanak gateway --network anvil")
            sys.exit(1)

        # Fund wallet with WETH
        try:
            fund_wallet_with_weth(Decimal("1.0"))
        except Exception as e:
            print(f"\nERROR: Failed to fund WETH: {e}")
            sys.exit(1)

        # Run strategy
        config = BASE_STRATEGY_CONFIG.copy()
        exit_code = run_strategy_via_cli(config)

        # Result summary
        if exit_code == 0:
            print(f"\n{'=' * 60}")
            print("SUCCESS!")
            print(f"{'=' * 60}")
            print("\nPancakeSwap V3 swap executed successfully.")
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
