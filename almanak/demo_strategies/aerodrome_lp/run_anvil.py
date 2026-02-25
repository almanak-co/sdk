#!/usr/bin/env python3
"""
===============================================================================
TUTORIAL: Running an Aerodrome LP Strategy on Anvil (Local Fork)
===============================================================================

This script demonstrates how to test an Aerodrome LP strategy on Anvil.
It forks Base chain and tests the Solidly-based AMM LP operations.

WHAT THIS SCRIPT DOES:
----------------------
1. Starts an Anvil fork of Base chain
2. Funds the test wallet with WETH and USDC
3. Runs the strategy via the CLI runner
4. The CLI handles market data, compilation, and execution

PREREQUISITES:
--------------
1. Foundry installed (provides anvil and cast)
   curl -L https://foundry.paradigm.xyz | bash && foundryup

2. RPC URL in .env file:
   ALMANAK_BASE_RPC_URL=https://mainnet.base.org

3. Python dependencies installed:
   uv sync

USAGE:
------
    python strategies/demo/aerodrome_lp/run_anvil.py

    # With custom options:
    python strategies/demo/aerodrome_lp/run_anvil.py --action open
    python strategies/demo/aerodrome_lp/run_anvil.py --action close
    python strategies/demo/aerodrome_lp/run_anvil.py --pool WETH/USDbC --stable

===============================================================================
"""

import os
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
from dotenv import load_dotenv

load_dotenv(project_root / ".env")


# =============================================================================
# CONFIGURATION
# =============================================================================

# Anvil's first default account (Account #0)
ANVIL_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
ANVIL_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

# Base chain token addresses
WETH_ADDRESS = "0x4200000000000000000000000000000000000006"
USDC_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"  # Native USDC on Base
USDBC_ADDRESS = "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA"  # Bridged USDC on Base

# Whale addresses for funding (large holders on Base)
# Aave aUSDC on Base - has >100M USDC
USDC_WHALE = "0x4e65fE4DbA92790696d040ac24Aa414708F5c0AB"

# Aerodrome contract addresses
AERODROME_ROUTER = "0xcF77a3Ba9A5CA399B7c97c74d54e5b1Beb874E43"
AERODROME_FACTORY = "0x420DD381b31aEf6683db6B902084cB0FFECe40Da"

# Amounts to fund
FUND_AMOUNT_USDC = 500  # 500 USDC
FUND_AMOUNT_WETH = Decimal("0.2")  # 0.2 WETH (~$600)

# Anvil settings
ANVIL_PORT = 8548  # Base chain default port (matches gateway expectation)
ANVIL_RPC = f"http://127.0.0.1:{ANVIL_PORT}"

# Base Chain ID
BASE_CHAIN_ID = 8453


# =============================================================================
# ANVIL MANAGER
# =============================================================================


class AnvilManager:
    """Manages the Anvil fork lifecycle."""

    def __init__(self, fork_url: str, port: int = 8548, chain_id: int = 8453):
        self.fork_url = fork_url
        self.port = port
        self.chain_id = chain_id
        self.process: subprocess.Popen | None = None

    def start(self) -> bool:
        """Start Anvil fork."""
        print(f"\n{'=' * 60}")
        print("STARTING ANVIL FORK OF BASE CHAIN")
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
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            print("Waiting for Anvil to fork (this may take ~10 seconds)...")
            time.sleep(10)

            if self.process.poll() is not None:
                stderr = self.process.stderr.read().decode() if self.process.stderr else ""
                print(f"ERROR: Anvil failed to start: {stderr[:500]}")
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
    # Handle hex output
    if output.startswith("0x"):
        return int(output, 16)
    return int(output.replace(",", ""))


def fund_wallet_with_usdc(wallet: str, amount_usdc: int, token_address: str = USDC_ADDRESS) -> bool:
    """Fund wallet with USDC using storage slot manipulation."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_usdc} USDC")
    print(f"{'=' * 60}")

    amount_wei = amount_usdc * 10**6

    try:
        # Method 1: Use cast index to find storage slot and set balance directly
        # This is the most reliable method for ERC-20 tokens

        # Get slot using cast index (balanceOf mapping is typically slot 0 or 9)
        # For USDC on Base, the balance mapping is at slot 9
        slot = run_cast(
            [
                "index",
                "address",
                wallet,
                "9",  # slot 9 for USDC
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        if slot:
            # Set the balance directly
            run_cast(
                [
                    "rpc",
                    "anvil_setStorageAt",
                    token_address,
                    slot,
                    f"0x{amount_wei:064x}",
                    "--rpc-url",
                    ANVIL_RPC,
                ],
                check=False,
            )

        # Verify balance
        balance = run_cast(
            [
                "call",
                token_address,
                "balanceOf(address)(uint256)",
                wallet,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        new_balance = parse_cast_uint(balance)
        print(f"Wallet USDC balance: {new_balance / 10**6:,.2f}")

        if new_balance >= amount_wei:
            return True

        # Method 2: Try impersonation as fallback
        print("Storage method didn't work, trying impersonation...")
        return fund_wallet_with_usdc_impersonate(wallet, amount_usdc, token_address)

    except Exception as e:
        print(f"ERROR: Storage method failed: {e}")
        return fund_wallet_with_usdc_impersonate(wallet, amount_usdc, token_address)


def fund_wallet_with_usdc_impersonate(wallet: str, amount_usdc: int, token_address: str = USDC_ADDRESS) -> bool:
    """Fund wallet with USDC by impersonating a whale (fallback)."""
    print("Trying whale impersonation method...")

    amount_wei = amount_usdc * 10**6

    try:
        # Check whale balance first
        balance = run_cast(
            [
                "call",
                token_address,
                "balanceOf(address)(uint256)",
                USDC_WHALE,
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        if balance:
            whale_balance = parse_cast_uint(balance)
            print(f"Whale USDC balance: {whale_balance / 10**6:,.2f}")

            if whale_balance >= amount_wei:
                # Give whale ETH for gas
                run_cast(
                    [
                        "rpc",
                        "anvil_setBalance",
                        USDC_WHALE,
                        "0x56BC75E2D63100000",
                        "--rpc-url",
                        ANVIL_RPC,
                    ],
                    check=False,
                )

                # Impersonate and transfer
                run_cast(
                    [
                        "rpc",
                        "anvil_impersonateAccount",
                        USDC_WHALE,
                        "--rpc-url",
                        ANVIL_RPC,
                    ],
                    check=False,
                )

                run_cast(
                    [
                        "send",
                        token_address,
                        "transfer(address,uint256)(bool)",
                        wallet,
                        str(amount_wei),
                        "--from",
                        USDC_WHALE,
                        "--unlocked",
                        "--gas-limit",
                        "100000",
                        "--rpc-url",
                        ANVIL_RPC,
                    ],
                    check=False,
                )

                run_cast(
                    [
                        "rpc",
                        "anvil_stopImpersonatingAccount",
                        USDC_WHALE,
                        "--rpc-url",
                        ANVIL_RPC,
                    ],
                    check=False,
                )

        # Verify final balance
        balance = run_cast(
            [
                "call",
                token_address,
                "balanceOf(address)(uint256)",
                wallet,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        new_balance = parse_cast_uint(balance)
        print(f"Wallet USDC balance after funding: {new_balance / 10**6:,.2f}")
        return new_balance >= amount_wei

    except Exception as e:
        print(f"ERROR: Impersonation method failed: {e}")
        return False


def fund_wallet_with_weth(wallet: str, amount_weth: Decimal) -> bool:
    """Fund wallet with WETH by wrapping ETH."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_weth} WETH")
    print(f"{'=' * 60}")

    amount_wei = int(amount_weth * 10**18)

    try:
        # Ensure wallet has ETH (native token)
        run_cast(
            [
                "rpc",
                "anvil_setBalance",
                wallet,
                hex(100 * 10**18),  # 100 ETH
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        # Check ETH balance
        eth_balance = run_cast(
            [
                "balance",
                wallet,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        print(f"Wallet ETH balance: {eth_balance}")

        # Wrap ETH to WETH by calling deposit() on WETH contract
        run_cast(
            [
                "send",
                WETH_ADDRESS,
                "deposit()",
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

        # Verify WETH balance
        balance = run_cast(
            [
                "call",
                WETH_ADDRESS,
                "balanceOf(address)(uint256)",
                wallet,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        weth_balance = parse_cast_uint(balance)
        print(f"Wallet WETH balance: {weth_balance / 10**18:.6f}")
        return weth_balance >= amount_wei

    except Exception as e:
        print(f"ERROR: Failed to fund wallet with WETH: {e}")
        import traceback

        traceback.print_exc()
        return False


# =============================================================================
# STRATEGY EXECUTION
# =============================================================================


def run_strategy_via_cli(force_action: str = "open", pool: str = "WETH/USDC", stable: bool = False) -> int:
    """
    Run the Aerodrome LP strategy through the CLI runner.

    Returns:
        Exit code (0 = success)
    """
    print(f"\n{'=' * 60}")
    print("RUNNING AERODROME LP STRATEGY VIA CLI")
    print(f"{'=' * 60}")

    # Build environment for CLI
    env = os.environ.copy()
    env["ALMANAK_CHAIN"] = "base"
    env["ALMANAK_RPC_URL"] = ANVIL_RPC
    env["ALMANAK_BASE_RPC_URL"] = ANVIL_RPC  # CLI checks chain-specific URL first
    env["ALMANAK_PRIVATE_KEY"] = ANVIL_PRIVATE_KEY

    # Build config
    import json
    import tempfile

    # Parse pool to get token amounts
    pool_parts = pool.split("/")
    pool_parts[0] if len(pool_parts) > 0 else "WETH"
    pool_parts[1] if len(pool_parts) > 1 else "USDC"

    config = {
        "strategy_id": "demo_aerodrome_lp",
        "strategy_name": "demo_aerodrome_lp",
        "pool": pool,
        "stable": stable,
        "amount0": "0.1",  # 0.1 WETH
        "amount1": "300",  # 300 USDC (roughly matching at $3000/ETH)
        "force_action": force_action,
        "chain": "base",
    }

    # Write temp config
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(config, f)
        config_path = f.name

    try:
        # Run CLI
        strategy_dir = project_root / "strategies" / "demo" / "aerodrome_lp"
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
        os.unlink(config_path)


def run_direct_test() -> bool:
    """
    Run a direct test of the Aerodrome adapter without the full CLI.

    This is useful for debugging and verifying the adapter works.
    """
    print(f"\n{'=' * 60}")
    print("DIRECT ADAPTER TEST")
    print(f"{'=' * 60}")

    try:
        from almanak.framework.connectors.aerodrome import (
            AerodromeAdapter,
            AerodromeConfig,
        )

        # Create config
        config = AerodromeConfig(
            chain="base",
            wallet_address=ANVIL_WALLET,
            rpc_url=ANVIL_RPC,
        )

        print("Creating Aerodrome Adapter...")
        print(f"  Chain: {config.chain}")
        print(f"  Wallet: {config.wallet_address}")
        print(f"  RPC: {config.rpc_url}")

        adapter = AerodromeAdapter(config)

        print("\nAdapter created successfully!")
        print(f"  Router: {adapter.sdk.router_address}")
        print(f"  Factory: {adapter.sdk.factory_address}")

        # Test getting a quote
        from decimal import Decimal

        print("\nTesting swap quote...")
        try:
            quote = adapter.get_swap_quote(
                token_in="WETH",
                token_out="USDC",
                amount_in=Decimal("0.1"),
                stable=False,
            )
            print(f"  Quote: 0.1 WETH -> {quote.amount_out} USDC")
            print(f"  Price: {quote.effective_price}")
            print(f"  Pool: {quote.pool_address}")
        except Exception as e:
            print(f"  Quote failed (expected if pool doesn't exist): {e}")

        return True

    except Exception as e:
        print(f"ERROR: Direct test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


# =============================================================================
# MAIN
# =============================================================================


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Run AerodromeLPStrategy on Anvil")
    parser.add_argument(
        "--action",
        choices=["open", "close", "test"],
        default="open",
        help="LP action to test (default: open, use 'test' for adapter test only)",
    )
    parser.add_argument(
        "--pool",
        default="WETH/USDC",
        help="Pool to use (default: WETH/USDC)",
    )
    parser.add_argument(
        "--stable",
        action="store_true",
        help="Use stable pool type (default: volatile)",
    )
    parser.add_argument(
        "--skip-cli",
        action="store_true",
        help="Skip CLI execution (only fund wallet)",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("ALMANAK DEMO - AERODROME LP STRATEGY ON ANVIL")
    print("=" * 60)
    print("\nThis test runs the AerodromeLPStrategy through the full stack:")
    print("  1. Anvil fork of Base chain")
    print("  2. Fund wallet with WETH + USDC")
    print("  3. Run strategy via CLI runner")
    print("  4. CLI handles compilation and execution")
    print(f"\nAction: {args.action.upper()}")
    print(f"Pool: {args.pool}")
    print(f"Pool Type: {'stable' if args.stable else 'volatile'}")
    print("")

    # Get RPC URL - Base specific
    fork_url = os.getenv("ALMANAK_BASE_RPC_URL")
    if not fork_url:
        # Use public Base RPC as fallback
        fork_url = "https://mainnet.base.org"
        print("Note: Using public Base RPC (set ALMANAK_BASE_RPC_URL for better reliability)")
    print(f"Fork URL: {fork_url[:50]}...")

    # Start Anvil
    anvil = AnvilManager(fork_url, ANVIL_PORT, BASE_CHAIN_ID)
    if not anvil.start():
        sys.exit(1)

    try:
        # Fund wallet
        if not fund_wallet_with_weth(ANVIL_WALLET, FUND_AMOUNT_WETH):
            print("Failed to fund wallet with WETH")
            sys.exit(1)

        if not fund_wallet_with_usdc(ANVIL_WALLET, FUND_AMOUNT_USDC):
            print("Failed to fund wallet with USDC (continuing anyway for testing)")

        # Run direct adapter test if requested
        if args.action == "test":
            success = run_direct_test()
            sys.exit(0 if success else 1)

        # Skip CLI if requested
        if args.skip_cli:
            print("\n--skip-cli flag set, stopping before CLI execution")
            print("Wallet has been funded. You can now test manually.")
            input("Press Enter to stop Anvil...")
            sys.exit(0)

        # Run strategy via CLI
        exit_code = run_strategy_via_cli(
            force_action=args.action,
            pool=args.pool,
            stable=args.stable,
        )

        if exit_code == 0:
            print(f"\n{'=' * 60}")
            print("SUCCESS!")
            print(f"{'=' * 60}")
            print("\nAerodrome LP strategy executed successfully.")
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
