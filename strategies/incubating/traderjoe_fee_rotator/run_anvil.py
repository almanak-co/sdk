#!/usr/bin/env python3
"""
Run the Multi-Pool Fee Rotator on Anvil (local Avalanche fork).

NOTE: Use the CLI for the best experience:
    uv run almanak strat run -d strategies/incubating/traderjoe_fee_rotator --network anvil --once --verbose

This script is a convenience wrapper. The CLI handles Anvil fork, gateway,
wallet funding, and POA middleware automatically.

Usage:
    python strategies/incubating/traderjoe_fee_rotator/run_anvil.py
    python strategies/incubating/traderjoe_fee_rotator/run_anvil.py --action open_a
    python strategies/incubating/traderjoe_fee_rotator/run_anvil.py --action open_b
    python strategies/incubating/traderjoe_fee_rotator/run_anvil.py --action close_a
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

load_dotenv(project_root / ".env")


def run_strategy(force_action: str = "open_a") -> int:
    """Run the strategy via CLI."""
    env = os.environ.copy()
    env["ALMANAK_CHAIN"] = "avalanche"

    config = {
        "strategy_id": "incubating_traderjoe_fee_rotator",
        "strategy_name": "incubating_traderjoe_fee_rotator",
        "chain": "avalanche",
        "pool_a": "WAVAX/USDC/20",
        "pool_b": "WAVAX/WETH.e/15",
        "pool_a_wavax": "1.0",
        "pool_a_usdc": "30",
        "pool_b_wavax": "1.0",
        "pool_b_weth_e": "0.01",
        "swap_rotation_usdc": "20",
        "force_action": force_action,
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(config, f)
        config_path = f.name

    try:
        strategy_dir = project_root / "strategies" / "incubating" / "traderjoe_fee_rotator"
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
    parser = argparse.ArgumentParser(description="Run TJ Fee Rotator on Anvil")
    parser.add_argument(
        "--action",
        choices=["open_a", "open_b", "close_a", "close_b", "swap_usdc_to_weth", "open"],
        default="open_a",
        help="Force action for testing (default: open_a)",
    )
    args = parser.parse_args()

    exit_code = run_strategy(force_action=args.action)
    print(f"\n{'SUCCESS' if exit_code == 0 else 'FAILED'} (exit={exit_code})")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
