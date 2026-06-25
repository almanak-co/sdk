#!/usr/bin/env python3
"""
===============================================================================
TUTORIAL: Running the Curve 3pool LP Strategy on Anvil (Local Fork)
===============================================================================

This script tests the Curve 3pool LP strategy on an Anvil fork of Ethereum.
It funds the wallet with all three 3pool coins (DAI + USDC + USDT) so the
strategy can execute a genuine 3-coin deposit.

WHAT THIS SCRIPT DOES:
----------------------
1. Starts an Anvil fork of Ethereum mainnet (default port 8549 — the
   gateway's canonical Ethereum port; override via ANVIL_ETHEREUM_PORT).
2. Funds the test wallet with DAI, USDC, and USDT via storage-slot writes.
3. (Default) Runs the strategy via the CLI runner, which handles market data,
   compilation, and execution. With --skip-cli it stops after funding (this is
   the mode the sidecar regression harness uses).

PREREQUISITES:
--------------
1. Foundry installed (provides anvil and cast):
   curl -L https://foundry.paradigm.xyz | bash && foundryup
2. RPC URL in .env: ALMANAK_ETHEREUM_RPC_URL=...
3. Python dependencies: uv sync

USAGE:
------
    python almanak/demo_strategies/curve_3pool_lp/run_anvil.py
    python almanak/demo_strategies/curve_3pool_lp/run_anvil.py --action open
    python almanak/demo_strategies/curve_3pool_lp/run_anvil.py --skip-cli

===============================================================================
"""

import os
import subprocess
import sys
import time
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from almanak.config.demo_runtime import (
    demo_anvil_port,
    demo_chain_rpc_url,
    demo_fork_block,
    demo_subprocess_env,
    load_demo_dotenv,
)

load_demo_dotenv(project_root)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Anvil's first default account (Account #0)
ANVIL_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
ANVIL_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

# Ethereum mainnet token addresses (Curve 3pool coins).
DAI_ADDRESS = "0x6B175474E89094C44Da98b954EedeAC495271d0F"
USDC_ADDRESS = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
USDT_ADDRESS = "0xdAC17F958D2ee523a2206206994597C13D831ec7"

# ERC-20 `balanceOf` mapping storage slots on Ethereum mainnet.
# Sourced from tests/intents/conftest CHAIN_CONFIGS["ethereum"] + the Curve
# 3pool intent test (DAI slot 2). Slot-based funding is preferred over whale
# impersonation: it does not depend on any whale's balance at the fork block.
DAI_BALANCE_SLOT = 2
USDC_BALANCE_SLOT = 9
USDT_BALANCE_SLOT = 2

# Per-coin funding amounts (human units). Enough to deposit 100 of each coin
# into 3pool with headroom for gas/rounding.
FUND_DAI = 10_000
FUND_USDC = 10_000
FUND_USDT = 10_000

# Anvil settings. The standalone gateway (sidecar harness) resolves the
# Ethereum Anvil port from the chain descriptor (anvil_port=8549), NOT 8545.
# We must start Anvil on that same port or the gateway's web3 provider can't
# reach the fork ("Cannot connect to host 127.0.0.1:8549"). demo_anvil_port
# honours an ANVIL_ETHEREUM_PORT override and otherwise returns the gateway's
# canonical default (mirrors morpho_looping/run_anvil.py).
ANVIL_PORT = demo_anvil_port("ethereum", default=8549)
ANVIL_RPC = f"http://127.0.0.1:{ANVIL_PORT}"
ETHEREUM_CHAIN_ID = 1


# =============================================================================
# ANVIL MANAGER
# =============================================================================


class AnvilManager:
    """Manages the Anvil fork lifecycle."""

    def __init__(self, fork_url: str, port: int = ANVIL_PORT, chain_id: int = ETHEREUM_CHAIN_ID):
        self.fork_url = fork_url
        self.port = port
        self.chain_id = chain_id
        self.process: subprocess.Popen | None = None

    def start(self) -> bool:
        """Start Anvil fork."""
        print(f"\n{'=' * 60}")
        print("STARTING ANVIL FORK OF ETHEREUM MAINNET")
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

        # CI exports ANVIL_FORK_BLOCK to pin Anvil to a stable per-week block so
        # Foundry's RPC disk cache (keyed by (chain_id, block)) hits across runs.
        fork_block_env = demo_fork_block("ethereum")
        if fork_block_env:
            cmd.extend(["--fork-block-number", fork_block_env])
            print(f"Pinning fork block to {fork_block_env}")

        try:
            # Discard Anvil's stdout/stderr. The sidecar harness keeps this
            # process alive for the whole job while Anvil serves transactions;
            # piping without a continuous reader would fill the ~64KB OS pipe
            # buffer and deadlock the fork once it logs enough.
            self.process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print("Waiting for Anvil to fork (this may take ~10 seconds)...")
            time.sleep(10)

            if self.process.poll() is not None:
                print(f"ERROR: Anvil failed to start (exited early). Check whether port {self.port} is already in use.")
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
    if output.startswith("0x"):
        return int(output, 16)
    return int(output.replace(",", ""))


def fund_erc20(symbol: str, token: str, slot: int, amount_human: int, decimals: int) -> None:
    """Fund the wallet with an ERC-20 via anvil_setStorageAt on the balances slot.

    Fails hard on a verification miss rather than returning False — letting CI
    advertise readiness with an empty wallet is the failure mode this guards.
    """
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_human} {symbol}")
    print(f"{'=' * 60}")

    amount_wei = amount_human * 10**decimals
    storage_key = run_cast(["index", "address", ANVIL_WALLET, str(slot)])
    storage_value = "0x" + format(amount_wei, "064x")
    run_cast(["rpc", "anvil_setStorageAt", token, storage_key, storage_value, "--rpc-url", ANVIL_RPC])

    balance = run_cast(["call", token, "balanceOf(address)(uint256)", ANVIL_WALLET, "--rpc-url", ANVIL_RPC])
    new_balance = parse_cast_uint(balance)
    print(f"Wallet {symbol} balance: {new_balance / 10**decimals:,.2f}")
    if new_balance < amount_wei:
        raise RuntimeError(
            f"{symbol} funding verification failed: got {new_balance / 10**decimals}, expected at least {amount_human}"
        )


def fund_native(amount_eth: int = 100) -> None:
    """Give the wallet native ETH for gas."""
    run_cast(
        ["rpc", "anvil_setBalance", ANVIL_WALLET, hex(amount_eth * 10**18), "--rpc-url", ANVIL_RPC],
        check=False,
    )
    print(f"Funded wallet with {amount_eth} ETH for gas")


def fund_all_coins() -> None:
    """Fund the wallet with native ETH + all three 3pool coins."""
    fund_native(100)
    fund_erc20("DAI", DAI_ADDRESS, DAI_BALANCE_SLOT, FUND_DAI, decimals=18)
    fund_erc20("USDC", USDC_ADDRESS, USDC_BALANCE_SLOT, FUND_USDC, decimals=6)
    fund_erc20("USDT", USDT_ADDRESS, USDT_BALANCE_SLOT, FUND_USDT, decimals=6)


# =============================================================================
# STRATEGY EXECUTION
# =============================================================================


def run_strategy_via_cli(force_action: str = "open") -> int:
    """Run the Curve 3pool LP strategy through the CLI runner."""
    print(f"\n{'=' * 60}")
    print("RUNNING CURVE 3POOL LP STRATEGY VIA CLI")
    print(f"{'=' * 60}")

    env = demo_subprocess_env(chain="ethereum", rpc_url=ANVIL_RPC, private_key=ANVIL_PRIVATE_KEY)

    import json
    import tempfile

    config = {
        "deployment_id": "demo_curve_3pool_lp",
        "strategy_name": "demo_curve_3pool_lp",
        "pool": "3pool",
        "amount_dai": "100",
        "amount_usdc": "100",
        "amount_usdt": "100",
        "min_position_usd": "100",
        "force_action": force_action,
        "chain": "ethereum",
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(config, f)
        config_path = f.name

    try:
        strategy_dir = Path(__file__).parent
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
        result = subprocess.run(cmd, cwd=str(project_root), env=env)
        return result.returncode
    finally:
        os.unlink(config_path)


# =============================================================================
# MAIN
# =============================================================================


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Run Curve3poolLPStrategy on Anvil")
    parser.add_argument(
        "--action",
        choices=["open", "close"],
        default="open",
        help="LP action to test (default: open)",
    )
    parser.add_argument(
        "--skip-cli",
        action="store_true",
        help="Skip CLI execution (only fund wallet) — used by the sidecar harness",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("ALMANAK DEMO - CURVE 3POOL LP STRATEGY ON ANVIL")
    print("=" * 60)
    print("\nThis test runs the Curve3poolLPStrategy through the full stack:")
    print("  1. Anvil fork of Ethereum mainnet")
    print("  2. Fund wallet with DAI + USDC + USDT")
    print("  3. Run strategy via CLI runner (3-coin deposit)")
    print(f"\nAction: {args.action.upper()}\n")

    fork_url = demo_chain_rpc_url("ethereum", allow_generic_fallback=False, fallback="")
    if not fork_url:
        print("ERROR: set ALMANAK_ETHEREUM_RPC_URL to an Ethereum archive RPC URL")
        sys.exit(1)
    print(f"Fork URL: {fork_url[:50]}...")

    anvil = AnvilManager(fork_url, ANVIL_PORT, ETHEREUM_CHAIN_ID)
    if not anvil.start():
        sys.exit(1)

    try:
        fund_all_coins()

        if args.skip_cli:
            print("\n--skip-cli flag set, stopping before CLI execution")
            print("Wallet has been funded. You can now test manually.")
            input("Press Enter to stop Anvil...")
            sys.exit(0)

        exit_code = run_strategy_via_cli(force_action=args.action)
        if exit_code == 0:
            print(f"\n{'=' * 60}\nSUCCESS!\n{'=' * 60}")
            print("\nCurve 3pool LP strategy executed successfully.")
        else:
            print(f"\n{'=' * 60}\nEXECUTION COMPLETED WITH ERRORS\n{'=' * 60}")
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
