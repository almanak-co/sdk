#!/usr/bin/env python3
"""Run the Uniswap V4 hooks LP demo on an Anvil fork of Base.

Starts the fork, funds the default Anvil wallet with WETH and USDC, then runs
the strategy once through the CLI. With ``--skip-cli`` it stops after funding,
prints "Wallet has been funded." and blocks on input() so the sidecar-regression
workflow can drive ``almanak strat run`` against the funded fork itself.

Usage:
    python almanak/demo_strategies/uniswap_v4_hooks/run_anvil.py
    python almanak/demo_strategies/uniswap_v4_hooks/run_anvil.py --skip-cli
"""

import argparse
import subprocess
import sys
import tempfile
import time
from decimal import Decimal
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from almanak.config.demo_runtime import (  # noqa: E402
    demo_chain_rpc_url,
    demo_fork_block,
    demo_subprocess_env,
    load_demo_dotenv,
)

load_demo_dotenv(project_root)

WETH_ADDRESS = "0x4200000000000000000000000000000000000006"
USDC_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"

ANVIL_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
ANVIL_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

# Base USDC proxy balances mapping slot; matches tests/intents/base.
USDC_BALANCE_SLOT = 9

FUND_AMOUNT_USDC = 1000
FUND_AMOUNT_WETH = Decimal("0.5")

# The standalone gateway resolves Base's Anvil RPC to this port
# (almanak/core/chains/base.py:anvil_port); a fork on 8545 is invisible to it.
ANVIL_PORT = 8548
ANVIL_RPC = f"http://127.0.0.1:{ANVIL_PORT}"


class AnvilManager:
    """Manages the Anvil fork lifecycle."""

    def __init__(self, fork_url: str, port: int = ANVIL_PORT):
        self.fork_url = fork_url
        self.port = port
        self.process: subprocess.Popen | None = None
        self.log_path = Path(tempfile.gettempdir()) / f"anvil-base-{port}.log"

    def start(self) -> bool:
        print(f"Forking Base from: {self.fork_url[:50]}...")
        cmd = [
            "anvil",
            "--fork-url",
            self.fork_url,
            "--port",
            str(self.port),
            "--chain-id",
            "8453",
            "--timeout",
            "60000",
        ]
        fork_block_env = demo_fork_block("base")
        if fork_block_env:
            cmd.extend(["--fork-block-number", fork_block_env])
            print(f"Pinning fork block to {fork_block_env}")
        try:
            # Anvil logs every RPC call, and --skip-cli leaves this process parked
            # on input() for the whole CI job with nothing reading the child's
            # output. A pipe would fill its ~64KB OS buffer and block Anvil mid-run,
            # so the fork stops serving while still holding the port. Redirect to a
            # file: unbounded, and the startup diagnostic survives for failure triage.
            self._log_handle = open(self.log_path, "w")
            self.process = subprocess.Popen(cmd, stdout=self._log_handle, stderr=subprocess.STDOUT)
            time.sleep(8)
            if self.process.poll() is not None:
                print(f"ERROR: Anvil failed to start: {self._log_tail()}")
                return False
            print(f"Anvil started on port {self.port} (log: {self.log_path})")
            return True
        except FileNotFoundError:
            print("ERROR: 'anvil' command not found! Install Foundry.")
            return False

    def _log_tail(self, limit: int = 500) -> str:
        try:
            return self.log_path.read_text(errors="replace")[-limit:]
        except OSError as exc:
            return f"<no Anvil log at {self.log_path}: {exc}>"

    def stop(self):
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            print("Anvil stopped.")
        handle = getattr(self, "_log_handle", None)
        if handle is not None:
            handle.close()
            self._log_handle = None


def run_cast(args: list[str], check: bool = True, timeout: int = 60) -> str:
    """Run a ``cast`` command and return stripped stdout, bounded so a hung call cannot stall CI."""
    try:
        result = subprocess.run(["cast", *args], capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(f"cast {' '.join(args)} timed out after {timeout}s") from exc
    if check and result.returncode != 0:
        raise RuntimeError(f"Cast command failed: {result.stderr}")
    return result.stdout.strip()


def parse_cast_uint(output: str) -> int:
    return int(output.strip().split(" ")[0].replace(",", ""))


def fund_wallet_with_usdc(wallet: str, amount_usdc: int) -> bool:
    amount_wei = amount_usdc * 10**6
    try:
        storage_key = run_cast(["index", "address", wallet, str(USDC_BALANCE_SLOT)])
        storage_value = "0x" + format(amount_wei, "064x")
        run_cast(["rpc", "anvil_setStorageAt", USDC_ADDRESS, storage_key, storage_value, "--rpc-url", ANVIL_RPC])
        balance = run_cast(["call", USDC_ADDRESS, "balanceOf(address)(uint256)", wallet, "--rpc-url", ANVIL_RPC])
        new_balance = parse_cast_uint(balance)
        print(f"Wallet USDC balance: {new_balance / 10**6:,.2f}")
        return new_balance >= amount_wei
    except Exception as e:  # noqa: BLE001
        print(f"ERROR: Failed to fund wallet with USDC: {e}")
        return False


def fund_wallet_with_weth(wallet: str, amount_weth: Decimal) -> bool:
    amount_wei = int(amount_weth * 10**18)
    try:
        run_cast(["rpc", "anvil_setBalance", wallet, hex(10 * 10**18), "--rpc-url", ANVIL_RPC], check=False)
        run_cast(
            ["send", WETH_ADDRESS, "--value", str(amount_wei), "--from", wallet]
            + ["--private-key", ANVIL_PRIVATE_KEY, "--rpc-url", ANVIL_RPC]
        )
        balance = run_cast(["call", WETH_ADDRESS, "balanceOf(address)(uint256)", wallet, "--rpc-url", ANVIL_RPC])
        weth_balance = parse_cast_uint(balance)
        print(f"Wallet WETH balance: {weth_balance / 10**18:.6f}")
        return weth_balance >= amount_wei
    except Exception as e:  # noqa: BLE001
        print(f"ERROR: Failed to fund wallet with WETH: {e}")
        return False


def run_strategy_via_cli() -> int:
    env = demo_subprocess_env(chain="base", rpc_url=ANVIL_RPC, private_key=ANVIL_PRIVATE_KEY)
    strategy_dir = Path(__file__).resolve().parent
    cmd = ["uv", "run", "almanak", "strat", "run", "--working-dir", str(strategy_dir), "--once", "--verbose"]
    cmd += ["--network", "anvil"]
    return subprocess.run(cmd, cwd=str(project_root), env=env).returncode


def main():
    parser = argparse.ArgumentParser(description="Run UniswapV4HooksStrategy on Anvil (Base)")
    parser.add_argument("--skip-cli", action="store_true", help="Only fund the wallet; keep Anvil alive.")
    args = parser.parse_args()

    fork_url = demo_chain_rpc_url("base")
    if not fork_url:
        print("ERROR: No RPC URL for base; set ALMANAK_BASE_RPC_URL")
        sys.exit(1)

    anvil = AnvilManager(fork_url, ANVIL_PORT)
    if not anvil.start():
        sys.exit(1)

    try:
        if not fund_wallet_with_usdc(ANVIL_WALLET, FUND_AMOUNT_USDC):
            sys.exit(1)
        if not fund_wallet_with_weth(ANVIL_WALLET, FUND_AMOUNT_WETH):
            sys.exit(1)

        if args.skip_cli:
            print("Wallet has been funded. You can now test manually.")
            input("Press Enter to stop Anvil...")
            sys.exit(0)

        sys.exit(run_strategy_via_cli())
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    finally:
        anvil.stop()


if __name__ == "__main__":
    main()
