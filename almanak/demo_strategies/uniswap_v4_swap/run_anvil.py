#!/usr/bin/env python3
"""
===============================================================================
TUTORIAL: Running the Uniswap V4 Swap Demo on Anvil (Base Fork)
===============================================================================

Mirrors almanak/demo_strategies/aerodrome_lp/run_anvil.py but for the Uniswap V4 swap
demo on Base. Base is where V4 pools have confirmed liquidity on Anvil forks
and the demo's default_chain matches, so decide() emits on the chain the
gateway is booted for (the VIB-2057 default_chain<->decide() mismatch that
affects the V4 LP/hooks demos does not bite this swap demo on base).

WHAT THIS SCRIPT DOES:
----------------------
1. Starts an Anvil fork of Base mainnet
2. Funds the test wallet with WETH and USDC (via anvil_setStorageAt / wrap)
3. Runs the strategy via the CLI runner (omit --skip-cli)

The CI sidecar-regression workflow runs this file with --skip-cli: it starts
Anvil + funds the wallet, prints "Wallet has been funded." and blocks on
input() while the workflow drives `almanak strat run` against the funded fork
under the socket sandbox.

USAGE:
------
    python almanak/demo_strategies/uniswap_v4_swap/run_anvil.py
    python almanak/demo_strategies/uniswap_v4_swap/run_anvil.py --skip-cli

===============================================================================
"""

import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from almanak.config.demo_runtime import (
    demo_chain_rpc_url,
    demo_fork_block,
    demo_subprocess_env,
    load_demo_dotenv,
)

load_demo_dotenv(project_root)


WETH_ADDRESS = "0x4200000000000000000000000000000000000006"
# Base mainnet USDC (native, Circle-issued).
USDC_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"

ANVIL_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"  # gitleaks:allow
ANVIL_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

# Base USDC balances slot (USDC proxy at 0x833589f...). Matches tests/intents/base.
USDC_BALANCE_SLOT = 9

FUND_AMOUNT_USDC = 1000
FUND_AMOUNT_WETH = Decimal("0.5")

# Base gets a DEDICATED Anvil port (8548), not the default 8545. The standalone
# gateway booted for `--chains base` resolves base's Anvil RPC to
# http://127.0.0.1:8548 (almanak/gateway/utils/rpc_provider.py ANVIL_CHAIN_PORTS,
# derived from almanak/core/chains/base.py:anvil_port=8548). Starting Anvil on
# 8545 leaves the gateway's balance/price RPC calls hitting a dead port — every
# fetch fails ("Failed to get native balance after 3 attempts") and the initial
# portfolio snapshot halts with ACCOUNTING_FAILED before decide() runs. This
# matches the passing aerodrome_lp/base demo (also 8548); arbitrum demos use
# 8545 and avalanche uses 8547, each matching its chain descriptor.
ANVIL_PORT = 8548
ANVIL_RPC = f"http://127.0.0.1:{ANVIL_PORT}"


class AnvilManager:
    """Manages the Anvil fork lifecycle."""

    def __init__(self, fork_url: str, port: int = ANVIL_PORT):
        self.fork_url = fork_url
        self.port = port
        self.process: subprocess.Popen | None = None

    def start(self) -> bool:
        print(f"\n{'=' * 60}")
        print("STARTING ANVIL FORK (BASE)")
        print(f"{'=' * 60}")
        print(f"Forking from: {self.fork_url[:50]}...")

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
            self.process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print("Waiting for Anvil to fork (this may take ~10 seconds)...")
            time.sleep(8)
            if self.process.poll() is not None:
                stderr = self.process.stderr.read().decode() if self.process.stderr else ""
                print(f"ERROR: Anvil failed to start: {stderr[:500]}")
                return False
            print(f"Anvil started on port {self.port}")
            return True
        except FileNotFoundError:
            print("ERROR: 'anvil' command not found! Install Foundry.")
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


def run_cast(args: list[str], check: bool = True, timeout: int = 60) -> str:
    """Run a ``cast`` command and return stripped stdout.

    ``timeout`` (default 60s) bounds the call so a hung ``cast`` invocation
    against the local Anvil fork cannot stall CI indefinitely. On timeout a
    RuntimeError is raised naming the command with any partial output captured.
    """
    try:
        result = subprocess.run(
            ["cast"] + args,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        if isinstance(stdout, bytes):
            stdout = stdout.decode("utf-8", "replace")
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", "replace")
        raise RuntimeError(
            f"cast {' '.join(args)} timed out after {timeout}s"
            + (f"\nstdout:\n{stdout}" if stdout else "")
            + (f"\nstderr:\n{stderr}" if stderr else "")
        ) from exc
    if check and result.returncode != 0:
        raise RuntimeError(f"Cast command failed: {result.stderr}")
    return result.stdout.strip()


def parse_cast_uint(output: str) -> int:
    output = output.strip()
    if " " in output:
        output = output.split(" ")[0]
    return int(output.replace(",", ""))


def fund_wallet_with_usdc(wallet: str, amount_usdc: int) -> bool:
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_usdc} USDC")
    print(f"{'=' * 60}")
    amount_wei = amount_usdc * 10**6
    try:
        storage_key = run_cast(["index", "address", wallet, str(USDC_BALANCE_SLOT)])
        storage_value = "0x" + format(amount_wei, "064x")
        run_cast(["rpc", "anvil_setStorageAt", USDC_ADDRESS, storage_key, storage_value, "--rpc-url", ANVIL_RPC])
        balance = run_cast(["call", USDC_ADDRESS, "balanceOf(address)(uint256)", wallet, "--rpc-url", ANVIL_RPC])
        new_balance = parse_cast_uint(balance)
        print(f"Wallet USDC balance: {new_balance / 10**6:,.2f}")
        return new_balance >= amount_wei
    except Exception as e:
        print(f"ERROR: Failed to fund wallet: {e}")
        return False


def fund_wallet_with_weth(wallet: str, amount_weth: Decimal) -> bool:
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_weth} WETH")
    print(f"{'=' * 60}")
    amount_wei = int(amount_weth * 10**18)
    try:
        run_cast(
            ["rpc", "anvil_setBalance", wallet, hex(10 * 10**18), "--rpc-url", ANVIL_RPC],
            check=False,
        )
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
        balance = run_cast(["call", WETH_ADDRESS, "balanceOf(address)(uint256)", wallet, "--rpc-url", ANVIL_RPC])
        weth_balance = int(balance.split()[0].replace(",", ""))
        print(f"Wallet WETH balance: {weth_balance / 10**18:.6f}")
        return weth_balance >= amount_wei
    except Exception as e:
        print(f"ERROR: Failed to fund wallet: {e}")
        return False


def run_strategy_via_cli() -> int:
    print(f"\n{'=' * 60}")
    print("RUNNING UNISWAP V4 SWAP DEMO VIA CLI")
    print(f"{'=' * 60}")
    env = demo_subprocess_env(chain="base", rpc_url=ANVIL_RPC, private_key=ANVIL_PRIVATE_KEY)
    strategy_dir = Path(__file__).resolve().parent
    cmd = [
        "uv",
        "run",
        "almanak",
        "strat",
        "run",
        "--working-dir",
        str(strategy_dir),
        "--once",
        "--verbose",
        "--network",
        "anvil",
    ]
    result = subprocess.run(cmd, cwd=str(project_root), env=env)
    return result.returncode


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Run UniswapV4SwapStrategy on Anvil (Base)")
    parser.add_argument("--skip-cli", action="store_true", help="Only fund wallet; keep Anvil alive.")
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
            print("Failed to fund wallet with USDC")
            sys.exit(1)
        if not fund_wallet_with_weth(ANVIL_WALLET, FUND_AMOUNT_WETH):
            print("Failed to fund wallet with WETH")
            sys.exit(1)

        if args.skip_cli:
            print("\n--skip-cli flag set, stopping before CLI execution")
            print("Wallet has been funded. You can now test manually.")
            input("Press Enter to stop Anvil...")
            sys.exit(0)

        exit_code = run_strategy_via_cli()
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
