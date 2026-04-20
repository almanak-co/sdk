#!/usr/bin/env python3
"""
Run Morpho Looping Strategy on Anvil Fork

This script tests the Morpho looping strategy on a local Anvil fork.

Prerequisites:
    1. Start Anvil fork of Ethereum on port 8549:
       anvil --fork-url https://eth-mainnet.g.alchemy.com/v2/$ALCHEMY_API_KEY --port 8549

    2. Start the gateway in another terminal:
       uv run almanak gateway --network anvil

    3. Run this script:
       python strategies/demo/morpho_looping/run_anvil.py [action]

Actions:
    supply  - Test supply collateral (default)
    borrow  - Test borrow after supply
    repay   - Test repay after borrow
    all     - Test full flow: supply -> borrow -> repay

What it does:
    1. Funds a test wallet with wstETH (for supply/borrow) or USDC (for repay)
    2. Runs the looping strategy via CLI runner with force_action
    3. Shows transaction status after execution
"""

import argparse
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
# Note: Gateway uses port 8549 for Ethereum when network=anvil
# (see almanak/gateway/utils/rpc_provider.py ANVIL_CHAIN_PORTS)
ETHEREUM_ANVIL_PORT = int(os.environ.get("ANVIL_ETHEREUM_PORT", "8549"))  # Gateway's default for Ethereum
ETHEREUM_ANVIL_URL = f"http://127.0.0.1:{ETHEREUM_ANVIL_PORT}"
# ANVIL_URL defaults to the gateway's Ethereum port for consistency
ANVIL_URL = os.environ.get("ANVIL_URL", ETHEREUM_ANVIL_URL)

# Test wallet (Anvil account 0)
TEST_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
TEST_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

# Token addresses on Ethereum
WSTETH_ADDRESS = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"
USDC_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"

# Base strategy configuration
BASE_STRATEGY_CONFIG = {
    "strategy_id": "morpho-looping-test",
    "market_id": "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
    "collateral_token": "wstETH",
    "borrow_token": "USDC",
    "initial_collateral": "0.1",  # Start with 0.1 wstETH for testing
    "target_loops": 2,  # Only 2 loops for testing
    "target_ltv": "0.70",  # 70% LTV (conservative)
    "min_health_factor": "1.5",
    "swap_slippage": "0.01",  # 1% slippage for Anvil
}


# =============================================================================
# ANVIL MANAGER
# =============================================================================


class AnvilManager:
    """Manages the Anvil fork lifecycle."""

    def __init__(self, fork_url: str, port: int = 8549, chain_id: int = 1):
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


def fund_wallet_with_token(token_address: str, token_symbol: str, amount: Decimal, decimals: int = 18) -> None:
    """Fund test wallet with a token using cast commands.

    Funds on the port that the gateway will use (8549 for Ethereum).
    This ensures the gateway sees the funded balance.
    """
    logger.info(f"Funding wallet with {amount} {token_symbol} on gateway's Ethereum port ({ETHEREUM_ANVIL_PORT})...")

    amount_wei = int(amount * 10**decimals)

    try:
        # Use cast index to find storage slot (most ERC20s use slot 0 for balances mapping)
        result = subprocess.run(
            ["cast", "index", "address", TEST_WALLET, "0"],
            capture_output=True,
            text=True,
            check=True,
        )
        storage_slot = result.stdout.strip()

        # Set the storage value on the gateway's Ethereum port
        amount_hex = f"0x{amount_wei:064x}"
        subprocess.run(
            [
                "cast",
                "rpc",
                "anvil_setStorageAt",
                token_address,
                storage_slot,
                amount_hex,
                "--rpc-url",
                ETHEREUM_ANVIL_URL,
            ],
            capture_output=True,
            check=True,
        )

        # Verify balance using cast on the gateway's port
        balance_result = subprocess.run(
            [
                "cast",
                "call",
                token_address,
                "balanceOf(address)(uint256)",
                TEST_WALLET,
                "--rpc-url",
                ETHEREUM_ANVIL_URL,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        # Parse balance (cast may return formatted output like "1000000000000000000 [1e18]")
        balance_str = balance_result.stdout.strip().split()[0].replace(",", "")
        balance_wei = int(balance_str)
        balance = Decimal(balance_wei) / Decimal(10**decimals)
        logger.info(f"Wallet {token_symbol} balance on port {ETHEREUM_ANVIL_PORT}: {balance}")

        if balance < amount:
            raise ValueError(f"Failed to fund wallet: got {balance}, expected {amount}")

    except subprocess.CalledProcessError as e:
        logger.exception(f"Cast command failed: {e.stderr}")
        raise
    except Exception as e:
        logger.exception(f"Failed to fund wallet with {token_symbol}: {e}")
        raise


def fund_wallet_with_wsteth(amount: Decimal) -> None:
    """Fund test wallet with wstETH."""
    fund_wallet_with_token(WSTETH_ADDRESS, "wstETH", amount, decimals=18)


def fund_wallet_with_usdc(amount: Decimal) -> None:
    """Fund test wallet with USDC."""
    fund_wallet_with_token(USDC_ADDRESS, "USDC", amount, decimals=6)


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


def run_strategy_via_cli(action: str, config: dict) -> int:
    """
    Run the Morpho looping strategy through the CLI runner.

    Args:
        action: Action name (supply, borrow, repay)
        config: Strategy configuration dict

    Returns:
        Exit code (0 = success)
    """
    print(f"\n{'=' * 60}")
    print(f"RUNNING MORPHO LOOPING STRATEGY - {action.upper()}")
    print(f"{'=' * 60}")

    # Build environment for CLI
    env = os.environ.copy()
    env["ALMANAK_CHAIN"] = "ethereum"
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
            str(project_root / "strategies" / "demo" / "morpho_looping"),
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


def run_full_flow() -> int:
    """
    Run the full supply -> borrow -> repay flow sequentially.

    Returns:
        Exit code (0 = all successful)
    """
    print("\n" + "=" * 70)
    print("MORPHO LOOPING STRATEGY - FULL FLOW TEST")
    print("Testing: SUPPLY -> BORROW -> REPAY")
    print("=" * 70)

    # Step 1: Supply collateral (0.1 wstETH)
    print("\n" + "-" * 70)
    print("STEP 1: SUPPLY COLLATERAL")
    print("-" * 70)

    supply_config = BASE_STRATEGY_CONFIG.copy()
    supply_config["force_action"] = "supply"

    exit_code = run_strategy_via_cli("supply", supply_config)
    if exit_code != 0:
        print(f"SUPPLY failed with exit code {exit_code}")
        return exit_code
    print("SUPPLY completed successfully!")

    # Step 2: Borrow against collateral
    print("\n" + "-" * 70)
    print("STEP 2: BORROW AGAINST COLLATERAL")
    print("-" * 70)

    borrow_config = BASE_STRATEGY_CONFIG.copy()
    borrow_config["force_action"] = "borrow"

    exit_code = run_strategy_via_cli("borrow", borrow_config)
    if exit_code != 0:
        print(f"BORROW failed with exit code {exit_code}")
        return exit_code
    print("BORROW completed successfully!")

    # Step 3: Fund wallet with USDC to repay (need to fund after borrow since we'll use the borrowed amount)
    # For testing, fund with a bit more than what we borrowed
    print("\n" + "-" * 70)
    print("STEP 3: FUND WALLET WITH USDC FOR REPAY")
    print("-" * 70)

    try:
        # Fund with 300 USDC (more than what we borrowed with 0.1 wstETH @ 70% LTV)
        fund_wallet_with_usdc(Decimal("300"))
        print("Funded wallet with 300 USDC for repay")
    except Exception as e:
        print(f"Warning: Failed to fund USDC: {e}")
        print("Continuing anyway...")

    # Step 4: Repay the borrowed amount
    print("\n" + "-" * 70)
    print("STEP 4: REPAY BORROWED AMOUNT")
    print("-" * 70)

    repay_config = BASE_STRATEGY_CONFIG.copy()
    repay_config["force_action"] = "repay"

    exit_code = run_strategy_via_cli("repay", repay_config)
    if exit_code != 0:
        print(f"REPAY failed with exit code {exit_code}")
        return exit_code
    print("REPAY completed successfully!")

    return 0


# =============================================================================
# Main
# =============================================================================


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Run Morpho looping strategy on Anvil fork",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Actions:
  supply  - Test supply collateral only
  borrow  - Test borrow only (requires previous supply on same Anvil instance)
  repay   - Test repay only (requires previous borrow on same Anvil instance)
  all     - Test full flow: supply -> borrow -> repay (default)

Prerequisites:
  1. Start Anvil fork of Ethereum on port 8549:
     anvil --fork-url https://eth-mainnet.g.alchemy.com/v2/$ALCHEMY_API_KEY --port 8549

  2. Start the gateway in another terminal:
     uv run almanak gateway --network anvil
""",
    )
    parser.add_argument(
        "action",
        nargs="?",
        default="all",
        choices=["supply", "borrow", "repay", "all"],
        help="Action to test (default: all)",
    )
    return parser.parse_args()


def main():
    """Run the Morpho looping strategy on Anvil."""
    args = parse_args()

    print("=" * 70)
    print("Morpho Blue Looping Strategy - Anvil Test")
    print(f"Action: {args.action.upper()}")
    print("=" * 70)

    # Get RPC URL for forking
    fork_url = os.getenv("ALMANAK_ETHEREUM_RPC_URL") or os.getenv("ALMANAK_RPC_URL")
    if not fork_url:
        print("ERROR: No RPC URL found in .env file")
        print("\nAdd one of these to .env:")
        print("  ALMANAK_ETHEREUM_RPC_URL=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY")
        sys.exit(1)

    # Start Anvil
    anvil = AnvilManager(fork_url, ETHEREUM_ANVIL_PORT, chain_id=1)
    if not anvil.start():
        sys.exit(1)

    try:
        # Check if gateway is running (required)
        import socket

        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(("localhost", 50051))
            sock.close()
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

        # Handle different actions
        if args.action == "all":
            # Full flow: supply -> borrow -> repay
            # Fund wallet with wstETH for supply
            try:
                fund_wallet_with_wsteth(Decimal("1.0"))
            except Exception as e:
                print(f"\nWARNING: Failed to fund wstETH: {e}")
                print("Continuing anyway...")

            exit_code = run_full_flow()

        elif args.action == "supply":
            # Fund wallet with wstETH
            try:
                fund_wallet_with_wsteth(Decimal("1.0"))
            except Exception as e:
                print(f"\nWARNING: Failed to fund wstETH: {e}")
                print("Continuing anyway...")

            config = BASE_STRATEGY_CONFIG.copy()
            config["force_action"] = "supply"
            exit_code = run_strategy_via_cli("supply", config)

        elif args.action == "borrow":
            # Borrow requires collateral already supplied
            config = BASE_STRATEGY_CONFIG.copy()
            config["force_action"] = "borrow"
            exit_code = run_strategy_via_cli("borrow", config)

        elif args.action == "repay":
            # Fund wallet with USDC for repay
            try:
                fund_wallet_with_usdc(Decimal("500"))
                print("Funded wallet with 500 USDC")
            except Exception as e:
                print(f"\nWARNING: Failed to fund USDC: {e}")
                print("Continuing anyway...")

            config = BASE_STRATEGY_CONFIG.copy()
            config["force_action"] = "repay"
            exit_code = run_strategy_via_cli("repay", config)

        # Result summary
        if exit_code == 0:
            print(f"\n{'=' * 60}")
            print("SUCCESS!")
            print(f"{'=' * 60}")
            print(f"\nMorpho looping strategy [{args.action}] executed successfully.")
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
