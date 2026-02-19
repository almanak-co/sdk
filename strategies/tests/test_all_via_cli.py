#!/usr/bin/env python3
"""
===============================================================================
Test All Strategies via CLI
===============================================================================

This script tests all test strategies via the CLI runner, simulating how a
real user would run them. For each strategy:

1. Kill any existing Anvil
2. Start Anvil fork of the appropriate chain
3. Fund the test wallet with required tokens
4. Run the strategy via CLI with --once --dry-run
5. Report results

USAGE:
------
    # Test all strategies
    python strategies/tests/test_all_via_cli.py

    # Test specific strategy type
    python strategies/tests/test_all_via_cli.py --type ta
    python strategies/tests/test_all_via_cli.py --type lp

    # Test a single strategy
    python strategies/tests/test_all_via_cli.py --strategy test_ma_crossover

===============================================================================
"""

import os
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

load_dotenv(project_root / ".env")

# =============================================================================
# CONFIGURATION
# =============================================================================

# Anvil's first default account
ANVIL_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
ANVIL_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"


@dataclass
class ChainConfig:
    """Configuration for a blockchain."""

    chain_id: int
    rpc_env_var: str
    fallback_rpc: str
    port: int
    native_symbol: str  # ETH, AVAX, etc.


@dataclass
class StrategyTestConfig:
    """Configuration for testing a strategy."""

    name: str
    chain: str
    tokens_to_fund: dict[str, tuple[str, int, int]]  # symbol -> (address, amount, decimals)
    config_overrides: dict | None = None


# Chain configurations
# NOTE: All chains use port 8545 because CLI default Anvil port is 8545
CHAINS = {
    "arbitrum": ChainConfig(
        chain_id=42161,
        rpc_env_var="ALMANAK_ARBITRUM_RPC_URL",
        fallback_rpc="https://arb1.arbitrum.io/rpc",
        port=8545,
        native_symbol="ETH",
    ),
    "base": ChainConfig(
        chain_id=8453,
        rpc_env_var="ALMANAK_BASE_RPC_URL",
        fallback_rpc="https://mainnet.base.org",
        port=8545,
        native_symbol="ETH",
    ),
    "avalanche": ChainConfig(
        chain_id=43114,
        rpc_env_var="ALMANAK_AVALANCHE_RPC_URL",
        fallback_rpc="https://api.avax.network/ext/bc/C/rpc",
        port=8545,
        native_symbol="AVAX",
    ),
}

# Token addresses by chain
TOKENS = {
    "arbitrum": {
        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        "USDC_WHALE": "0x489ee077994B6658eAfA855C308275EAd8097C4A",
    },
    "base": {
        "WETH": "0x4200000000000000000000000000000000000006",
        "USDC": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
        "USDbC": "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA",
    },
    "avalanche": {
        "WAVAX": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
        "USDC": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
        "JOE": "0x6e84a6216eA6dACC71eE8E6b0a5B7322EEbC0fDd",
        "JOE_WHALE": "0x102D195C3eE8BF8A9A89d63FB3659432d3174d81",
    },
}

# Strategy test configurations
TA_STRATEGIES = [
    StrategyTestConfig(
        name="test_ma_crossover",
        chain="arbitrum",
        tokens_to_fund={
            "WETH": (TOKENS["arbitrum"]["WETH"], 1, 18),  # 1 WETH
            "USDC": (TOKENS["arbitrum"]["USDC"], 1000, 6),  # 1000 USDC
        },
    ),
    StrategyTestConfig(
        name="test_rsi_reversion",
        chain="arbitrum",
        tokens_to_fund={
            "WETH": (TOKENS["arbitrum"]["WETH"], 1, 18),
            "USDC": (TOKENS["arbitrum"]["USDC"], 1000, 6),
        },
    ),
    StrategyTestConfig(
        name="test_macd_crossover",
        chain="arbitrum",
        tokens_to_fund={
            "WETH": (TOKENS["arbitrum"]["WETH"], 1, 18),
            "USDC": (TOKENS["arbitrum"]["USDC"], 1000, 6),
        },
    ),
    StrategyTestConfig(
        name="test_bollinger_reversion",
        chain="arbitrum",
        tokens_to_fund={
            "WETH": (TOKENS["arbitrum"]["WETH"], 1, 18),
            "USDC": (TOKENS["arbitrum"]["USDC"], 1000, 6),
        },
    ),
    StrategyTestConfig(
        name="test_stochastic_reversion",
        chain="arbitrum",
        tokens_to_fund={
            "WETH": (TOKENS["arbitrum"]["WETH"], 1, 18),
            "USDC": (TOKENS["arbitrum"]["USDC"], 1000, 6),
        },
    ),
    StrategyTestConfig(
        name="test_atr_volatility_gate",
        chain="arbitrum",
        tokens_to_fund={
            "WETH": (TOKENS["arbitrum"]["WETH"], 1, 18),
            "USDC": (TOKENS["arbitrum"]["USDC"], 1000, 6),
        },
    ),
    StrategyTestConfig(
        name="test_adx_trend_filter",
        chain="arbitrum",
        tokens_to_fund={
            "WETH": (TOKENS["arbitrum"]["WETH"], 1, 18),
            "USDC": (TOKENS["arbitrum"]["USDC"], 1000, 6),
        },
    ),
    StrategyTestConfig(
        name="test_obv_divergence",
        chain="arbitrum",
        tokens_to_fund={
            "WETH": (TOKENS["arbitrum"]["WETH"], 1, 18),
            "USDC": (TOKENS["arbitrum"]["USDC"], 1000, 6),
        },
    ),
    StrategyTestConfig(
        name="test_cci_reversion",
        chain="arbitrum",
        tokens_to_fund={
            "WETH": (TOKENS["arbitrum"]["WETH"], 1, 18),
            "USDC": (TOKENS["arbitrum"]["USDC"], 1000, 6),
        },
    ),
    StrategyTestConfig(
        name="test_ichimoku_crossover",
        chain="arbitrum",
        tokens_to_fund={
            "WETH": (TOKENS["arbitrum"]["WETH"], 1, 18),
            "USDC": (TOKENS["arbitrum"]["USDC"], 1000, 6),
        },
    ),
]

LP_STRATEGIES = [
    StrategyTestConfig(
        name="test_aero_stable_farmer",
        chain="base",
        tokens_to_fund={
            "USDC": (TOKENS["base"]["USDC"], 100, 6),
            "USDbC": (TOKENS["base"]["USDbC"], 100, 6),
        },
    ),
    StrategyTestConfig(
        name="test_aero_trend_follower",
        chain="base",
        tokens_to_fund={
            "WETH": (TOKENS["base"]["WETH"], 1, 18),
            "USDC": (TOKENS["base"]["USDC"], 100, 6),
        },
    ),
    StrategyTestConfig(
        name="test_uni_asymmetric_bull",
        chain="arbitrum",
        tokens_to_fund={
            "WETH": (TOKENS["arbitrum"]["WETH"], 1, 18),
            "USDC": (TOKENS["arbitrum"]["USDC"], 1000, 6),
        },
    ),
    StrategyTestConfig(
        name="test_uni_vol_adaptive",
        chain="arbitrum",
        tokens_to_fund={
            "WETH": (TOKENS["arbitrum"]["WETH"], 1, 18),
            "USDC": (TOKENS["arbitrum"]["USDC"], 1000, 6),
        },
    ),
    StrategyTestConfig(
        name="test_tj_tight_scalper",
        chain="avalanche",
        tokens_to_fund={
            "WAVAX": (TOKENS["avalanche"]["WAVAX"], 10, 18),
            "USDC": (TOKENS["avalanche"]["USDC"], 100, 6),
        },
    ),
    StrategyTestConfig(
        name="test_tj_wide_accumulator",
        chain="avalanche",
        tokens_to_fund={
            "WAVAX": (TOKENS["avalanche"]["WAVAX"], 10, 18),
            "JOE": (TOKENS["avalanche"]["JOE"], 100, 18),
        },
    ),
]


# =============================================================================
# ANVIL MANAGEMENT
# =============================================================================


def kill_anvil():
    """Kill any running Anvil processes."""
    subprocess.run(["pkill", "-f", "anvil"], capture_output=True)
    time.sleep(1)


def start_anvil(chain: str) -> subprocess.Popen | None:
    """Start Anvil fork for a chain."""
    config = CHAINS[chain]

    # Get RPC URL
    fork_url = os.getenv(config.rpc_env_var)
    if not fork_url:
        alchemy_key = os.getenv("ALCHEMY_API_KEY")
        if alchemy_key:
            if chain == "arbitrum":
                fork_url = f"https://arb-mainnet.g.alchemy.com/v2/{alchemy_key}"
            elif chain == "base":
                fork_url = f"https://base-mainnet.g.alchemy.com/v2/{alchemy_key}"
            elif chain == "avalanche":
                fork_url = f"https://avax-mainnet.g.alchemy.com/v2/{alchemy_key}"
        else:
            fork_url = config.fallback_rpc
            print(f"  Note: Using public RPC for {chain}")

    print(f"  Starting Anvil fork of {chain} on port {config.port}...")

    cmd = [
        "anvil",
        "--fork-url",
        fork_url,
        "--port",
        str(config.port),
        "--chain-id",
        str(config.chain_id),
        "--timeout",
        "60000",
    ]

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        time.sleep(8)  # Wait for fork

        if process.poll() is not None:
            stderr = process.stderr.read().decode() if process.stderr else ""
            print(f"  ERROR: Anvil failed to start: {stderr[:200]}")
            return None

        print(f"  Anvil started on port {config.port}")
        return process

    except FileNotFoundError:
        print("  ERROR: 'anvil' command not found!")
        return None


# =============================================================================
# WALLET FUNDING
# =============================================================================


def run_cast(args: list[str], rpc_url: str, check: bool = True) -> str:
    """Run a cast command."""
    cmd = ["cast"] + args + ["--rpc-url", rpc_url]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise RuntimeError(f"Cast failed: {result.stderr}")
    return result.stdout.strip()


def fund_native_token(wallet: str, rpc_url: str, amount_wei: int):
    """Fund wallet with native token (ETH/AVAX)."""
    run_cast(
        ["rpc", "anvil_setBalance", wallet, hex(amount_wei)],
        rpc_url,
        check=False,
    )


def fund_weth(wallet: str, rpc_url: str, weth_address: str, amount_wei: int):
    """Fund wallet with WETH by wrapping native token."""
    run_cast(
        ["send", weth_address, "--value", str(amount_wei), "--from", wallet, "--private-key", ANVIL_PRIVATE_KEY],
        rpc_url,
    )


def fund_token_via_slot(wallet: str, rpc_url: str, token_address: str, amount_wei: int, slot: int = 9):
    """Fund wallet with ERC20 token via storage slot manipulation."""
    slot_hex = run_cast(
        ["index", "address", wallet, str(slot)],
        rpc_url,
        check=False,
    )
    if slot_hex:
        run_cast(
            ["rpc", "anvil_setStorageAt", token_address, slot_hex, f"0x{amount_wei:064x}"],
            rpc_url,
            check=False,
        )


def fund_wallet(config: StrategyTestConfig, rpc_url: str):
    """Fund wallet with required tokens for a strategy."""
    CHAINS[config.chain]

    # Always fund with native token for gas
    fund_native_token(ANVIL_WALLET, rpc_url, 100 * 10**18)

    for symbol, (address, amount, decimals) in config.tokens_to_fund.items():
        amount_wei = amount * (10**decimals)

        if symbol in ["WETH", "WAVAX"]:
            # Wrap native token
            fund_weth(ANVIL_WALLET, rpc_url, address, amount_wei)
        else:
            # Use storage slot manipulation
            fund_token_via_slot(ANVIL_WALLET, rpc_url, address, amount_wei)

        print(f"    Funded {amount} {symbol}")


# =============================================================================
# STRATEGY TESTING
# =============================================================================


def test_strategy_via_cli(config: StrategyTestConfig) -> tuple[bool, str]:
    """
    Test a strategy via CLI.

    Returns:
        (success, message)
    """
    chain_config = CHAINS[config.chain]
    rpc_url = f"http://127.0.0.1:{chain_config.port}"

    # Set environment for CLI
    env = os.environ.copy()
    env["ALMANAK_CHAIN"] = config.chain
    env["ALMANAK_RPC_URL"] = rpc_url
    env[f"ALMANAK_{config.chain.upper()}_RPC_URL"] = rpc_url
    env["ALMANAK_PRIVATE_KEY"] = ANVIL_PRIVATE_KEY

    # Run CLI
    cmd = [
        sys.executable,
        "-m",
        "almanak.framework.cli.run",
        "--strategy",
        config.name,
        "--once",
        "--dry-run",
        "--network",
        "anvil",
        "--verbose",
    ]

    result = subprocess.run(
        cmd,
        cwd=str(project_root),
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )

    # Check result
    output = result.stdout + result.stderr
    if result.returncode == 0:
        # Check for intent in output
        if "Intent" in output or "HOLD" in output or "SWAP" in output or "LP_OPEN" in output:
            return True, "Strategy executed and produced intent"
        else:
            return True, "Strategy executed (no intent visible in output)"
    else:
        # Extract error
        error_lines = [l for l in output.split("\n") if "error" in l.lower() or "Error" in l]
        error_msg = error_lines[0] if error_lines else output[-500:]
        return False, f"CLI error: {error_msg}"


def run_strategy_test(config: StrategyTestConfig) -> tuple[bool, str]:
    """Run a complete strategy test."""
    print(f"\n{'=' * 60}")
    print(f"TESTING: {config.name}")
    print(f"{'=' * 60}")
    print(f"  Chain: {config.chain}")

    # Kill existing Anvil
    print("  Killing existing Anvil...")
    kill_anvil()

    # Start Anvil
    anvil_process = start_anvil(config.chain)
    if not anvil_process:
        return False, "Failed to start Anvil"

    try:
        chain_config = CHAINS[config.chain]
        rpc_url = f"http://127.0.0.1:{chain_config.port}"

        # Fund wallet
        print("  Funding wallet...")
        try:
            fund_wallet(config, rpc_url)
        except Exception as e:
            return False, f"Failed to fund wallet: {e}"

        # Test via CLI
        print("  Running strategy via CLI...")
        success, message = test_strategy_via_cli(config)

        if success:
            print(f"  PASS: {message}")
        else:
            print(f"  FAIL: {message}")

        return success, message

    finally:
        # Stop Anvil
        print("  Stopping Anvil...")
        anvil_process.terminate()
        try:
            anvil_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            anvil_process.kill()


# =============================================================================
# MAIN
# =============================================================================


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Test all strategies via CLI")
    parser.add_argument(
        "--type",
        choices=["ta", "lp", "all"],
        default="all",
        help="Type of strategies to test (default: all)",
    )
    parser.add_argument(
        "--strategy",
        type=str,
        help="Test a specific strategy by name",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("ALMANAK - TEST ALL STRATEGIES VIA CLI")
    print("=" * 60)

    # Determine which strategies to test
    if args.strategy:
        # Find specific strategy
        all_strategies = TA_STRATEGIES + LP_STRATEGIES
        strategies = [s for s in all_strategies if s.name == args.strategy]
        if not strategies:
            print(f"ERROR: Strategy '{args.strategy}' not found")
            sys.exit(1)
    elif args.type == "ta":
        strategies = TA_STRATEGIES
    elif args.type == "lp":
        strategies = LP_STRATEGIES
    else:
        strategies = TA_STRATEGIES + LP_STRATEGIES

    print(f"\nTesting {len(strategies)} strategies...")

    # Run tests
    results = []
    for config in strategies:
        success, message = run_strategy_test(config)
        results.append((config.name, success, message))

    # Print summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    passed = sum(1 for _, s, _ in results if s)
    failed = len(results) - passed

    for name, success, message in results:
        status = "PASS" if success else "FAIL"
        print(f"  [{status}] {name}: {message[:60]}")

    print(f"\nTotal: {passed}/{len(results)} passed, {failed} failed")

    # Cleanup
    kill_anvil()

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
