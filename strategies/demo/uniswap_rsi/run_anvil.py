#!/usr/bin/env python3
"""
===============================================================================
TUTORIAL: Running a Strategy on Anvil (Local Fork)
===============================================================================

This script demonstrates how to test a strategy on Anvil, a local Ethereum
fork. This is essential for development because:

1. NO REAL MONEY: You can test without risking real funds
2. FAST ITERATION: Quickly iterate on strategy logic
3. REPRODUCIBLE: Same starting state every time
4. DEBUGGING: Full access to transaction traces

WHAT THIS SCRIPT DOES:
----------------------
1. Starts an Anvil fork of Arbitrum mainnet
2. Funds the test wallet with USDC (for buy signals)
3. Funds the test wallet with WETH (for sell signals)
4. Forces a specific RSI condition to trigger a trade
5. Executes the strategy through the full stack
6. Verifies the trade executed correctly

PREREQUISITES:
--------------
1. Foundry installed (provides anvil and cast)
   curl -L https://foundry.paradigm.xyz | bash && foundryup

2. RPC URL in .env file (one of):
   ALMANAK_ARBITRUM_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY
   or ALMANAK_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY

3. Python dependencies installed:
   uv sync

USAGE:
------
    python strategies/demo/uniswap_rsi/run_anvil.py

    # With custom options:
    python strategies/demo/uniswap_rsi/run_anvil.py --action buy
    python strategies/demo/uniswap_rsi/run_anvil.py --action sell

===============================================================================
"""

import os
import subprocess
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

# Add project root to path so we can import our modules
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables from .env
from dotenv import load_dotenv

load_dotenv(project_root / ".env")


# =============================================================================
# CONFIGURATION
# =============================================================================
# These are the default settings for running on Anvil.
# Anvil provides pre-funded accounts with known private keys.

# Anvil's first default account (Account #0)
# This account comes pre-funded with 10,000 ETH on any Anvil fork
ANVIL_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
ANVIL_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"

# Arbitrum mainnet token addresses
# USDC.e is "bridged" USDC - it's the most liquid USDC on Arbitrum
USDC_ADDRESS = "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8"  # USDC.e
WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

# A wallet with lots of USDC that we'll "steal" from using impersonation
# (This is the Aave V3 pool - one of the largest USDC holders)
USDC_WHALE = "0x489ee077994B6658eAfA855C308275EAd8097C4A"

# Uniswap V3 addresses on Arbitrum
UNISWAP_V3_ROUTER = "0xE592427A0AEce92De3Edee1F18E0157C05861564"

# Strategy parameters
TRADE_SIZE_USD = Decimal("100")  # Trade $100 worth
FUND_AMOUNT_USDC = 1000  # Fund wallet with 1000 USDC
FUND_AMOUNT_WETH = Decimal("0.5")  # Fund wallet with 0.5 WETH

# Anvil settings
ANVIL_PORT = 8545
ANVIL_RPC = f"http://127.0.0.1:{ANVIL_PORT}"


# =============================================================================
# RESULT TRACKING
# =============================================================================


@dataclass
class SwapResult:
    """Track the results of a swap execution."""

    tx_hash: str
    action: str  # "buy" or "sell"
    token_in: str
    token_out: str
    amount_in: Decimal
    amount_out: Decimal
    gas_used: int


# =============================================================================
# ANVIL MANAGER
# =============================================================================
# This class handles starting and stopping the Anvil fork process.


class AnvilManager:
    """Manages the Anvil fork lifecycle."""

    def __init__(self, fork_url: str, port: int = 8545):
        """
        Initialize the Anvil manager.

        Args:
            fork_url: RPC URL to fork from (e.g., Alchemy Arbitrum URL)
            port: Port to run Anvil on (default 8545)
        """
        self.fork_url = fork_url
        self.port = port
        self.process: subprocess.Popen | None = None

    def start(self) -> bool:
        """
        Start Anvil fork of Arbitrum.

        Returns:
            True if started successfully, False otherwise
        """
        print(f"\n{'=' * 60}")
        print("STARTING ANVIL FORK")
        print(f"{'=' * 60}")
        print(f"Forking from: {self.fork_url[:50]}...")

        # Anvil command with Arbitrum settings
        cmd = [
            "anvil",
            "--fork-url",
            self.fork_url,
            "--port",
            str(self.port),
            "--chain-id",
            "42161",  # Arbitrum chain ID
            "--timeout",
            "60000",  # Transaction timeout
        ]

        try:
            # Start Anvil in background
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            # Wait for Anvil to be ready
            print("Waiting for Anvil to fork Arbitrum (this may take ~10 seconds)...")
            time.sleep(8)  # Give Anvil time to fork

            # Check if process crashed
            if self.process.poll() is not None:
                stderr = self.process.stderr.read().decode() if self.process.stderr else ""
                print(f"ERROR: Anvil failed to start: {stderr[:500]}")
                return False

            print(f"Anvil started on port {self.port}")
            print(f"RPC URL: {ANVIL_RPC}")
            return True

        except FileNotFoundError:
            print("ERROR: 'anvil' command not found!")
            print("\nPlease install Foundry:")
            print("  curl -L https://foundry.paradigm.xyz | bash")
            print("  foundryup")
            return False
        except Exception as e:
            print(f"ERROR: Failed to start Anvil: {e}")
            return False

    def stop(self):
        """Stop the Anvil process."""
        if self.process:
            print("\nStopping Anvil...")
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            print("Anvil stopped.")


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================


def run_cast(args: list[str], check: bool = True) -> str:
    """
    Run a cast command and return output.

    Cast is Foundry's command-line tool for interacting with Ethereum.
    We use it for simple operations like checking balances or sending transactions.

    Args:
        args: Command arguments (without 'cast' prefix)
        check: Whether to raise an exception on failure

    Returns:
        Command output as string
    """
    cmd = ["cast"] + args
    result = subprocess.run(cmd, capture_output=True, text=True)

    if check and result.returncode != 0:
        raise RuntimeError(f"Cast command failed: {result.stderr}")

    return result.stdout.strip()


def parse_cast_uint(output: str) -> int:
    """Parse a uint value from cast command output."""
    output = output.strip()
    if " " in output:
        output = output.split(" ")[0]
    output = output.replace(",", "")
    return int(output)


def fund_wallet_with_usdc(wallet: str, amount_usdc: int) -> bool:
    """
    Fund a wallet with USDC by impersonating a whale.

    This is how you get test tokens on Anvil:
    1. Find a wallet with lots of tokens (a "whale")
    2. Use anvil_impersonateAccount to pretend to be that wallet
    3. Transfer tokens to your test wallet
    4. Stop impersonating

    Args:
        wallet: Address to fund
        amount_usdc: Amount of USDC to transfer

    Returns:
        True if successful
    """
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_usdc} USDC")
    print(f"{'=' * 60}")

    amount_wei = amount_usdc * 10**6  # USDC has 6 decimals

    try:
        # Check whale has enough USDC
        balance = run_cast(
            [
                "call",
                USDC_ADDRESS,
                "balanceOf(address)(uint256)",
                USDC_WHALE,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        whale_balance = parse_cast_uint(balance)
        print(f"Whale USDC balance: {whale_balance / 10**6:,.2f}")

        if whale_balance < amount_wei:
            print("ERROR: Whale has insufficient USDC")
            return False

        # Give whale some ETH for gas
        run_cast(
            [
                "rpc",
                "anvil_setBalance",
                USDC_WHALE,
                "0x56BC75E2D63100000",  # 100 ETH
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        # Impersonate whale
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

        # Transfer USDC
        run_cast(
            [
                "send",
                USDC_ADDRESS,
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
            ]
        )

        # Stop impersonating
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

        # Verify balance
        balance = run_cast(
            [
                "call",
                USDC_ADDRESS,
                "balanceOf(address)(uint256)",
                wallet,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        new_balance = parse_cast_uint(balance)
        print(f"Wallet USDC balance: {new_balance / 10**6:,.2f} USDC")
        return new_balance >= amount_wei

    except Exception as e:
        print(f"ERROR: Failed to fund wallet: {e}")
        return False


def fund_wallet_with_weth(wallet: str, amount_weth: Decimal) -> bool:
    """
    Fund a wallet with WETH by wrapping ETH.

    Since Anvil accounts start with 10,000 ETH, we can simply:
    1. Send ETH to the WETH contract
    2. This automatically wraps it to WETH

    Args:
        wallet: Address to fund
        amount_weth: Amount of WETH to create

    Returns:
        True if successful
    """
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_weth} WETH")
    print(f"{'=' * 60}")

    amount_wei = int(amount_weth * 10**18)

    try:
        # Ensure wallet has ETH (Anvil accounts should have 10k ETH)
        run_cast(
            [
                "rpc",
                "anvil_setBalance",
                wallet,
                hex(10 * 10**18),  # 10 ETH
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        # Wrap ETH to WETH by sending ETH to WETH contract
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

        # Verify balance
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
        weth_balance = int(balance.split()[0].replace(",", ""))
        print(f"Wallet WETH balance: {weth_balance / 10**18:.6f} WETH")
        return weth_balance >= amount_wei

    except Exception as e:
        print(f"ERROR: Failed to fund wallet: {e}")
        return False


# =============================================================================
# STRATEGY EXECUTION
# =============================================================================


def run_strategy_on_anvil(force_action: str = "buy") -> SwapResult | None:
    """
    Run the UniswapRSIStrategy through the full stack on Anvil.

    This demonstrates the complete flow:
    1. Create strategy instance with config
    2. Create market snapshot with forced conditions
    3. Call strategy.decide() to get intent
    4. Compile intent to action bundle
    5. Execute transactions
    6. Verify results

    Args:
        force_action: "buy" or "sell" - which signal to force

    Returns:
        SwapResult if successful, None if failed
    """
    print(f"\n{'=' * 60}")
    print(f"RUNNING UNISWAP RSI STRATEGY (force: {force_action})")
    print(f"{'=' * 60}")

    from web3 import Web3

    from almanak.framework.intents import IntentCompiler
    from almanak.framework.intents.compiler import CompilationStatus
    from almanak.framework.models.hot_reload_config import HotReloadableConfig

    # Import stack components
    from almanak.framework.strategies import MarketSnapshot
    from almanak.framework.strategies.intent_strategy import RSIData, TokenBalance

    # Import our strategy
    from strategies.demo.uniswap_rsi import UniswapRSIStrategy

    # Connect to Anvil
    w3 = Web3(Web3.HTTPProvider(ANVIL_RPC))
    if not w3.is_connected():
        print("ERROR: Cannot connect to Anvil")
        return None

    print(f"Connected to Anvil at block: {w3.eth.block_number}")

    # =========================================================================
    # STEP 1: Create Strategy Instance
    # =========================================================================
    print("\n--- Step 1: Create Strategy ---")

    # Create config - we use HotReloadableConfig as base since the strategy
    # framework expects a config with to_dict() method.
    # Our strategy will read custom params from the config dict pattern.
    config = HotReloadableConfig(
        trade_size_usd=TRADE_SIZE_USD,
        max_slippage=Decimal("0.01"),  # 1% for testing
    )
    # Store additional config as attributes for our strategy
    config.rsi_period = 14
    config.rsi_oversold = Decimal("30")
    config.rsi_overbought = Decimal("70")
    config.max_slippage_bps = 300  # 3% for testing - real trading should use tighter slippage
    config.base_token = "WETH"
    config.quote_token = "USDC.e"  # Use bridged USDC on Arbitrum

    strategy = UniswapRSIStrategy(
        config=config,
        chain="arbitrum",
        wallet_address=ANVIL_WALLET,
    )

    print(f"Strategy: {strategy.STRATEGY_NAME}")
    print(f"Trade Size: ${TRADE_SIZE_USD}")

    # =========================================================================
    # STEP 2: Create Market Snapshot with Forced RSI
    # =========================================================================
    print("\n--- Step 2: Create Market Snapshot ---")

    # Create market snapshot
    market = MarketSnapshot(
        chain="arbitrum",
        wallet_address=ANVIL_WALLET,
    )

    # Set realistic ETH price
    eth_price = Decimal("3400")
    market.set_price("WETH", eth_price)
    market.set_price("ETH", eth_price)
    market.set_price("USDC", Decimal("1"))
    market.set_price("USDC.e", Decimal("1"))

    # Force RSI to trigger our desired action
    if force_action == "buy":
        forced_rsi = Decimal("25")  # Below 30 = oversold = buy
    else:
        forced_rsi = Decimal("75")  # Above 70 = overbought = sell

    # Create RSIData objects with the forced value
    rsi_data = RSIData(value=forced_rsi, period=14)
    market.set_rsi("WETH", rsi_data)
    market.set_rsi("ETH", rsi_data)

    print(f"ETH Price: ${eth_price}")
    print(f"Forced RSI: {forced_rsi} ({'oversold - BUY' if force_action == 'buy' else 'overbought - SELL'})")

    # Set wallet balances for the market snapshot
    # Get actual on-chain balances
    usdc_balance_raw = w3.eth.call(
        {
            "to": w3.to_checksum_address(USDC_ADDRESS),
            "data": bytes.fromhex("70a08231" + "000000000000000000000000" + ANVIL_WALLET[2:].lower()),
        }
    )
    usdc_balance = Decimal(int.from_bytes(usdc_balance_raw, "big")) / Decimal(10**6)

    weth_balance_raw = w3.eth.call(
        {
            "to": w3.to_checksum_address(WETH_ADDRESS),
            "data": bytes.fromhex("70a08231" + "000000000000000000000000" + ANVIL_WALLET[2:].lower()),
        }
    )
    weth_balance = Decimal(int.from_bytes(weth_balance_raw, "big")) / Decimal(10**18)

    # Create TokenBalance objects for the market snapshot
    usdc_balance_obj = TokenBalance(
        symbol="USDC.e",  # Use USDC.e on Arbitrum
        balance=usdc_balance,
        balance_usd=usdc_balance,  # USDC is $1
        address=USDC_ADDRESS,
    )
    weth_balance_obj = TokenBalance(
        symbol="WETH",
        balance=weth_balance,
        balance_usd=weth_balance * eth_price,
        address=WETH_ADDRESS,
    )
    market.set_balance("USDC.e", usdc_balance_obj)
    market.set_balance("WETH", weth_balance_obj)

    print(f"USDC.e Balance: ${usdc_balance:,.2f}")
    print(f"WETH Balance: {weth_balance:.6f} (${weth_balance * eth_price:,.2f})")

    # =========================================================================
    # STEP 3: Get Intent from Strategy
    # =========================================================================
    print("\n--- Step 3: Strategy Decision ---")

    intent = strategy.decide(market)

    if intent is None:
        print("ERROR: Strategy returned None")
        return None

    print(f"Intent Type: {intent.intent_type.value}")

    if intent.intent_type.value == "HOLD":
        print(f"Reason: {getattr(intent, 'reason', 'No reason')}")
        print("Strategy decided to HOLD - no trade executed")
        return None

    if hasattr(intent, "from_token"):
        print(f"From: {intent.from_token}")
        print(f"To: {intent.to_token}")
        print(f"Amount: ${intent.amount_usd}")

    # =========================================================================
    # STEP 4: Compile Intent to Action Bundle
    # =========================================================================
    print("\n--- Step 4: Compile Intent ---")

    compiler = IntentCompiler(
        chain="arbitrum",
        wallet_address=ANVIL_WALLET,
        price_oracle={
            "ETH": eth_price,
            "WETH": eth_price,
            "USDC": Decimal("1"),
            "USDC.e": Decimal("1"),
        },
    )

    result = compiler.compile(intent)

    if result.status != CompilationStatus.SUCCESS:
        print(f"ERROR: Compilation failed: {result.error}")
        return None

    action_bundle = result.action_bundle
    print(f"Action Bundle: {len(action_bundle.transactions)} transactions")

    for i, tx in enumerate(action_bundle.transactions):
        print(f"  {i + 1}. {tx.get('description', 'Unknown')}")

    # =========================================================================
    # STEP 5: Execute Transactions
    # =========================================================================
    print("\n--- Step 5: Execute Transactions ---")

    # Check balances before
    usdc_before = int.from_bytes(
        w3.eth.call(
            {
                "to": w3.to_checksum_address(USDC_ADDRESS),
                "data": bytes.fromhex("70a08231" + "000000000000000000000000" + ANVIL_WALLET[2:].lower()),
            }
        ),
        "big",
    )

    weth_before = int.from_bytes(
        w3.eth.call(
            {
                "to": w3.to_checksum_address(WETH_ADDRESS),
                "data": bytes.fromhex("70a08231" + "000000000000000000000000" + ANVIL_WALLET[2:].lower()),
            }
        ),
        "big",
    )

    print(f"Before - USDC: {usdc_before / 10**6:,.2f}, WETH: {weth_before / 10**18:.6f}")

    # Get account and gas settings
    account = w3.eth.account.from_key(ANVIL_PRIVATE_KEY)
    nonce = w3.eth.get_transaction_count(account.address)
    gas_price = w3.eth.gas_price
    block = w3.eth.get_block("latest")
    block["timestamp"] + 600

    swap_receipt = None

    for i, tx_data in enumerate(action_bundle.transactions):
        tx_type = tx_data.get("tx_type", "unknown")
        description = tx_data.get("description", "Unknown")
        print(f"\n  TX {i + 1}: {description}")

        to_address = w3.to_checksum_address(tx_data["to"])
        value = int(tx_data.get("value", 0))

        try:
            # Prepare transaction
            tx_data_bytes = tx_data["data"]
            if isinstance(tx_data_bytes, str):
                if tx_data_bytes.startswith("0x"):
                    tx_data_bytes = bytes.fromhex(tx_data_bytes[2:])
                else:
                    tx_data_bytes = bytes.fromhex(tx_data_bytes)

            gas_limit = tx_data.get("gas_estimate", 300000)
            if gas_limit < 200000:
                gas_limit = 300000

            tx = {
                "from": account.address,
                "to": to_address,
                "value": value,
                "gas": gas_limit,
                "gasPrice": gas_price,
                "nonce": nonce,
                "data": tx_data_bytes,
                "chainId": 42161,
            }

            # Sign and send
            signed_tx = account.sign_transaction(tx)
            tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

            status = "SUCCESS" if receipt["status"] == 1 else "REVERTED"
            print(f"    Status: {status}, Gas: {receipt['gasUsed']:,}")

            if receipt["status"] == 0:
                print("    ERROR: Transaction reverted!")
                return None

            if tx_type != "approve":
                swap_receipt = receipt

            nonce += 1

        except Exception as e:
            print(f"    ERROR: {e}")
            import traceback

            traceback.print_exc()
            return None

    # =========================================================================
    # STEP 6: Verify Results
    # =========================================================================
    print("\n--- Step 6: Verify Results ---")

    # Check balances after
    usdc_after = int.from_bytes(
        w3.eth.call(
            {
                "to": w3.to_checksum_address(USDC_ADDRESS),
                "data": bytes.fromhex("70a08231" + "000000000000000000000000" + ANVIL_WALLET[2:].lower()),
            }
        ),
        "big",
    )

    weth_after = int.from_bytes(
        w3.eth.call(
            {
                "to": w3.to_checksum_address(WETH_ADDRESS),
                "data": bytes.fromhex("70a08231" + "000000000000000000000000" + ANVIL_WALLET[2:].lower()),
            }
        ),
        "big",
    )

    usdc_delta = (usdc_after - usdc_before) / 10**6
    weth_delta = (weth_after - weth_before) / 10**18

    print("\nBalance Changes:")
    print(f"  USDC: {usdc_before / 10**6:,.2f} -> {usdc_after / 10**6:,.2f} ({usdc_delta:+,.2f})")
    print(f"  WETH: {weth_before / 10**18:.6f} -> {weth_after / 10**18:.6f} ({weth_delta:+.6f})")

    # Determine what happened
    if force_action == "buy":
        # Bought WETH with USDC
        return SwapResult(
            tx_hash=swap_receipt["transactionHash"].hex() if swap_receipt else "",
            action="buy",
            token_in="USDC",
            token_out="WETH",
            amount_in=Decimal(str(abs(usdc_delta))),
            amount_out=Decimal(str(weth_delta)),
            gas_used=swap_receipt["gasUsed"] if swap_receipt else 0,
        )
    else:
        # Sold WETH for USDC
        return SwapResult(
            tx_hash=swap_receipt["transactionHash"].hex() if swap_receipt else "",
            action="sell",
            token_in="WETH",
            token_out="USDC",
            amount_in=Decimal(str(abs(weth_delta))),
            amount_out=Decimal(str(usdc_delta)),
            gas_used=swap_receipt["gasUsed"] if swap_receipt else 0,
        )


# =============================================================================
# MAIN
# =============================================================================


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Run UniswapRSIStrategy on Anvil")
    parser.add_argument(
        "--action",
        choices=["buy", "sell"],
        default="buy",
        help="Force buy or sell signal (default: buy)",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("ALMANAK DEMO - UNISWAP RSI STRATEGY ON ANVIL")
    print("=" * 60)
    print("\nThis test runs the UniswapRSIStrategy through the full stack:")
    print("  1. Strategy.decide() -> returns Intent")
    print("  2. IntentCompiler.compile() -> returns ActionBundle")
    print("  3. Execute transactions on Anvil fork")
    print("  4. Verify swap executed correctly")
    print(f"\nForced action: {args.action.upper()}")
    print("")

    # Get RPC URL for forking
    # Try ALMANAK_ARBITRUM_RPC_URL first, then ALMANAK_RPC_URL
    fork_url = os.getenv("ALMANAK_ARBITRUM_RPC_URL") or os.getenv("ALMANAK_RPC_URL")
    if not fork_url:
        print("ERROR: No RPC URL found in .env file")
        print("\nAdd one of these to .env:")
        print("  ALMANAK_ARBITRUM_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY")
        print("  ALMANAK_RPC_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY")
        sys.exit(1)

    # Start Anvil
    anvil = AnvilManager(fork_url, ANVIL_PORT)
    if not anvil.start():
        sys.exit(1)

    try:
        # Fund wallet
        if not fund_wallet_with_usdc(ANVIL_WALLET, FUND_AMOUNT_USDC):
            print("Failed to fund wallet with USDC")
            sys.exit(1)

        if not fund_wallet_with_weth(ANVIL_WALLET, FUND_AMOUNT_WETH):
            print("Failed to fund wallet with WETH")
            sys.exit(1)

        # Run strategy
        result = run_strategy_on_anvil(force_action=args.action)

        if result:
            print(f"\n{'=' * 60}")
            print("SUCCESS!")
            print(f"{'=' * 60}")
            print(f"\n  Action: {result.action.upper()}")
            print(f"  TX Hash: {result.tx_hash}")
            print(f"  {result.token_in} spent: {result.amount_in}")
            print(f"  {result.token_out} received: {result.amount_out}")
            print(f"  Gas Used: {result.gas_used:,}")
            print(f"\n{'=' * 60}")
            print("UNISWAP RSI STRATEGY EXECUTED SUCCESSFULLY!")
            print(f"{'=' * 60}\n")
        else:
            print("\nStrategy execution did not produce a trade")
            print("(This is expected if strategy decided to HOLD)")

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
