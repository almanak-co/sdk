#!/usr/bin/env python3
"""
Run the Volatility-Adaptive TraderJoe LP Rebalancer on Anvil (local Avalanche fork).

Usage:
    python strategies/incubating/traderjoe_vol_rebalancer/run_anvil.py
    python strategies/incubating/traderjoe_vol_rebalancer/run_anvil.py --action close
"""

import json
import os
import subprocess
import sys
import tempfile
import time
from decimal import Decimal
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

load_dotenv(project_root / ".env")

# Anvil defaults
ANVIL_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
ANVIL_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
WAVAX_ADDRESS = "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"
USDC_ADDRESS = "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E"
USDC_WHALE = "0x625E7708f30cA75bfd92586e17077590C60eb4cD"
ANVIL_PORT = 8547
ANVIL_RPC = f"http://127.0.0.1:{ANVIL_PORT}"
AVALANCHE_CHAIN_ID = 43114


def run_cast(args: list[str], check: bool = True) -> str:
    result = subprocess.run(["cast"] + args, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"Cast failed: {result.stderr}")
    return result.stdout.strip()


def parse_cast_uint(output: str) -> int:
    output = output.strip()
    if " " in output:
        output = output.split(" ")[0]
    if output.startswith("0x"):
        return int(output, 16)
    return int(output.replace(",", ""))


class AnvilManager:
    def __init__(self, fork_url: str, port: int, chain_id: int):
        self.fork_url = fork_url
        self.port = port
        self.chain_id = chain_id
        self.process = None

    def start(self) -> bool:
        print(f"Starting Anvil fork of Avalanche (port {self.port})...")
        cmd = [
            "anvil", "--fork-url", self.fork_url,
            "--port", str(self.port), "--chain-id", str(self.chain_id),
            "--timeout", "60000",
        ]
        try:
            self.process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            time.sleep(10)
            if self.process.poll() is not None:
                stderr = self.process.stderr.read().decode() if self.process.stderr else ""
                print(f"Anvil failed: {stderr[:500]}")
                return False
            print("Anvil ready.")
            return True
        except FileNotFoundError:
            print("ERROR: 'anvil' not found. Install Foundry: curl -L https://foundry.paradigm.xyz | bash && foundryup")
            return False

    def stop(self):
        if self.process:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()


def fund_wallet():
    """Fund Anvil wallet with AVAX, WAVAX, and USDC."""
    print("Funding wallet...")

    # Native AVAX
    run_cast(["rpc", "anvil_setBalance", ANVIL_WALLET, hex(100 * 10**18), "--rpc-url", ANVIL_RPC], check=False)

    # Wrap AVAX -> WAVAX
    run_cast([
        "send", WAVAX_ADDRESS, "deposit()", "--value", str(5 * 10**18),
        "--from", ANVIL_WALLET, "--private-key", ANVIL_PRIVATE_KEY, "--rpc-url", ANVIL_RPC,
    ])
    bal = parse_cast_uint(run_cast(["call", WAVAX_ADDRESS, "balanceOf(address)(uint256)", ANVIL_WALLET, "--rpc-url", ANVIL_RPC]))
    print(f"  WAVAX: {bal / 10**18:.4f}")

    # USDC via whale impersonation
    amount_wei = 1000 * 10**6
    run_cast(["rpc", "anvil_setBalance", USDC_WHALE, "0x56BC75E2D63100000", "--rpc-url", ANVIL_RPC], check=False)
    run_cast(["rpc", "anvil_impersonateAccount", USDC_WHALE, "--rpc-url", ANVIL_RPC], check=False)
    run_cast([
        "send", USDC_ADDRESS, "transfer(address,uint256)(bool)", ANVIL_WALLET, str(amount_wei),
        "--from", USDC_WHALE, "--unlocked", "--gas-limit", "100000", "--rpc-url", ANVIL_RPC,
    ], check=False)
    run_cast(["rpc", "anvil_stopImpersonatingAccount", USDC_WHALE, "--rpc-url", ANVIL_RPC], check=False)
    bal = parse_cast_uint(run_cast(["call", USDC_ADDRESS, "balanceOf(address)(uint256)", ANVIL_WALLET, "--rpc-url", ANVIL_RPC]))
    print(f"  USDC: {bal / 10**6:.2f}")


def run_strategy(force_action: str = "open") -> int:
    """Run the strategy via CLI."""
    env = os.environ.copy()
    env["ALMANAK_CHAIN"] = "avalanche"
    env["ALMANAK_RPC_URL"] = ANVIL_RPC
    env["ALMANAK_AVALANCHE_RPC_URL"] = ANVIL_RPC
    env["ALMANAK_PRIVATE_KEY"] = ANVIL_PRIVATE_KEY

    config = {
        "strategy_id": "incubating_traderjoe_vol_rebalancer",
        "strategy_name": "incubating_traderjoe_vol_rebalancer",
        "pool": "WAVAX/USDC/20",
        "capital_x": "1.0",
        "capital_y": "30",
        "atr_period": 14,
        "low_vol_range_pct": "0.05",
        "med_vol_range_pct": "0.10",
        "high_vol_range_pct": "0.20",
        "atr_low_threshold": "0.02",
        "atr_high_threshold": "0.05",
        "drift_rebalance_pct": "0.60",
        "min_rebalance_interval_hours": 4,
        "force_action": force_action,
        "chain": "avalanche",
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(config, f)
        config_path = f.name

    try:
        strategy_dir = project_root / "strategies" / "incubating" / "traderjoe_vol_rebalancer"
        cmd = [
            "uv", "run", "almanak", "strat", "run",
            "--working-dir", str(strategy_dir),
            "--config", config_path,
            "--once", "--verbose", "--network", "anvil",
        ]
        print(f"Running: {' '.join(cmd[:8])}...")
        result = subprocess.run(cmd, cwd=str(project_root), env=env)
        return result.returncode
    finally:
        os.unlink(config_path)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run TJ Vol Rebalancer on Anvil")
    parser.add_argument("--action", choices=["open", "close"], default="open")
    args = parser.parse_args()

    fork_url = os.getenv("ALMANAK_AVALANCHE_RPC_URL", "https://api.avax.network/ext/bc/C/rpc")
    anvil = AnvilManager(fork_url, ANVIL_PORT, AVALANCHE_CHAIN_ID)

    if not anvil.start():
        sys.exit(1)

    try:
        fund_wallet()
        exit_code = run_strategy(force_action=args.action)
        print(f"\n{'SUCCESS' if exit_code == 0 else 'FAILED'} (exit={exit_code})")
        sys.exit(exit_code)
    except KeyboardInterrupt:
        print("\nInterrupted")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        anvil.stop()


if __name__ == "__main__":
    main()
