#!/usr/bin/env python3
"""
Run MA Crossover Strategy on Anvil Fork

Tests the MA crossover strategy through the full execution stack:
1. Starts Anvil fork of Arbitrum
2. Funds wallet with USDC and LINK
3. Forces a buy action via config
4. Executes the strategy and prints results

Usage:
    python strategies/tests/ma_crossover/run_anvil.py
"""

import os
import subprocess
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

load_dotenv(project_root / ".env")


# Anvil settings
ANVIL_PRIVATE_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
ANVIL_WALLET = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
ANVIL_PORT = 8545
ANVIL_RPC = f"http://127.0.0.1:{ANVIL_PORT}"

# Arbitrum token addresses
LINK_ADDRESS = "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4"
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"  # Native USDC on Arbitrum

# USDC whale for funding (Arbitrum - Aave V3 pool)
USDC_WHALE = "0x489ee077994B6658eAfA855C308275EAd8097C4A"

# Strategy parameters
TRADE_SIZE_USD = Decimal("5")
FUND_AMOUNT_USDC = 100
FUND_AMOUNT_LINK = Decimal("10")  # ~$100 worth of LINK


@dataclass
class SwapResult:
    """Track swap execution results."""

    tx_hash: str
    action: str
    token_in: str
    token_out: str
    amount_in: Decimal
    amount_out: Decimal
    gas_used: int


class AnvilManager:
    """Manages Anvil fork lifecycle."""

    def __init__(self, fork_url: str, port: int = 8545):
        self.fork_url = fork_url
        self.port = port
        self.process: subprocess.Popen | None = None

    def start(self) -> bool:
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
            "42161",
            "--timeout",
            "60000",
        ]

        try:
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            print("Waiting for Anvil to fork Arbitrum...")
            time.sleep(8)

            if self.process.poll() is not None:
                stderr = self.process.stderr.read().decode() if self.process.stderr else ""
                print(f"ERROR: Anvil failed to start: {stderr[:500]}")
                return False

            print(f"Anvil started on port {self.port}")
            return True

        except FileNotFoundError:
            print("ERROR: 'anvil' command not found. Install Foundry first.")
            return False
        except Exception as e:
            print(f"ERROR: Failed to start Anvil: {e}")
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


def run_cast(args: list[str], check: bool = True) -> str:
    """Run a cast command and return output."""
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
    output = output.replace(",", "")
    return int(output)


def fund_wallet_with_usdc(wallet: str, amount_usdc: int) -> bool:
    """Fund wallet with USDC via whale impersonation."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_usdc} USDC")
    print(f"{'=' * 60}")

    amount_wei = amount_usdc * 10**6

    try:
        # Check whale balance
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


def fund_wallet_with_link(wallet: str, amount_link: Decimal) -> bool:
    """Fund wallet with LINK via whale impersonation."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_link} LINK")
    print(f"{'=' * 60}")

    # LINK whale on Arbitrum - Chainlink token holder
    LINK_WHALE = "0x191c10Aa4AF7C30e871E70C95dB0E4eb77237530"  # Chainlink staking pool

    amount_wei = int(amount_link * 10**18)

    try:
        # Check whale balance
        balance = run_cast(
            [
                "call",
                LINK_ADDRESS,
                "balanceOf(address)(uint256)",
                LINK_WHALE,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        whale_balance = parse_cast_uint(balance)
        print(f"Whale LINK balance: {whale_balance / 10**18:,.2f}")

        if whale_balance < amount_wei:
            print("WARNING: Primary whale has insufficient LINK, trying alternative...")
            # Try alternative whale - another large holder
            LINK_WHALE_ALT = "0x0B38c30A2C8B6b3f69aA84eb5d9d4fD88A77B7D5"
            balance = run_cast(
                [
                    "call",
                    LINK_ADDRESS,
                    "balanceOf(address)(uint256)",
                    LINK_WHALE_ALT,
                    "--rpc-url",
                    ANVIL_RPC,
                ]
            )
            whale_balance = parse_cast_uint(balance)
            print(f"Alt whale LINK balance: {whale_balance / 10**18:,.2f}")
            if whale_balance >= amount_wei:
                LINK_WHALE = LINK_WHALE_ALT
            else:
                # Try Aave pool
                AAVE_POOL = "0x489ee077994B6658eAfA855C308275EAd8097C4A"
                balance = run_cast(
                    [
                        "call",
                        LINK_ADDRESS,
                        "balanceOf(address)(uint256)",
                        AAVE_POOL,
                        "--rpc-url",
                        ANVIL_RPC,
                    ]
                )
                whale_balance = parse_cast_uint(balance)
                print(f"Aave pool LINK balance: {whale_balance / 10**18:,.2f}")
                if whale_balance >= amount_wei:
                    LINK_WHALE = AAVE_POOL
                else:
                    print("ERROR: No whale with sufficient LINK")
                    return False

        # Give whale ETH for gas
        run_cast(
            [
                "rpc",
                "anvil_setBalance",
                LINK_WHALE,
                "0x56BC75E2D63100000",
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
                LINK_WHALE,
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        # Transfer LINK
        run_cast(
            [
                "send",
                LINK_ADDRESS,
                "transfer(address,uint256)(bool)",
                wallet,
                str(amount_wei),
                "--from",
                LINK_WHALE,
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
                LINK_WHALE,
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        # Verify balance
        balance = run_cast(
            [
                "call",
                LINK_ADDRESS,
                "balanceOf(address)(uint256)",
                wallet,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        new_balance = parse_cast_uint(balance)
        print(f"Wallet LINK balance: {new_balance / 10**18:,.2f} LINK")
        return new_balance >= amount_wei

    except Exception as e:
        print(f"ERROR: Failed to fund wallet with LINK: {e}")
        return False


def calculate_sma(prices: list[float], period: int) -> float:
    """Calculate Simple Moving Average from price list."""
    if len(prices) < period:
        return 0.0
    return sum(prices[-period:]) / period


def run_strategy_on_anvil(force_action: str = "buy") -> SwapResult | None:
    """Run MA Crossover Strategy on Anvil fork."""
    print(f"\n{'=' * 60}")
    print(f"RUNNING MA CROSSOVER STRATEGY (force: {force_action})")
    print(f"{'=' * 60}")

    from web3 import Web3

    from almanak.framework.intents import IntentCompiler
    from almanak.framework.intents.compiler import CompilationStatus
    from almanak.framework.models.hot_reload_config import HotReloadableConfig
    from almanak.framework.strategies import MAData, MarketSnapshot
    from almanak.framework.strategies.intent_strategy import TokenBalance
    from strategies.tests.ma_crossover import MACrossoverStrategy

    w3 = Web3(Web3.HTTPProvider(ANVIL_RPC))
    if not w3.is_connected():
        print("ERROR: Cannot connect to Anvil")
        return None

    print(f"Connected to Anvil at block: {w3.eth.block_number}")

    # Create strategy with force_action
    print("\n--- Step 1: Create Strategy ---")
    config = HotReloadableConfig(
        trade_size_usd=TRADE_SIZE_USD,
        max_slippage=Decimal("0.01"),
    )
    config.sma_short = 9
    config.sma_long = 21
    config.max_slippage_bps = 300
    config.base_token = "LINK"
    config.quote_token = "USDC"
    config.protocol = "uniswap_v3"
    config.force_action = force_action

    strategy = MACrossoverStrategy(
        config=config,
        chain="arbitrum",
        wallet_address=ANVIL_WALLET,
    )

    print(f"Strategy: {strategy.STRATEGY_NAME}")
    print(f"Trade Size: ${TRADE_SIZE_USD}")

    # Create market snapshot
    print("\n--- Step 2: Create Market Snapshot ---")
    market = MarketSnapshot(
        chain="arbitrum",
        wallet_address=ANVIL_WALLET,
    )

    link_price = Decimal("12.50")  # LINK price in USD
    market.set_price("LINK", link_price)
    market.set_price("USDC", Decimal("1"))

    # Calculate MA from simulated price history
    # Simulate 30 periods of price data for MA calculation
    import random

    random.seed(42)  # For reproducibility
    base_price = float(link_price)
    prices = []
    for _ in range(30):
        # Simulate price movement with slight uptrend for bullish test
        change = random.uniform(-0.02, 0.025)
        base_price = base_price * (1 + change)
        prices.append(base_price)

    short_ma = calculate_sma(prices, 9)
    long_ma = calculate_sma(prices, 21)

    # Simulate previous values for crossover detection
    prev_prices = prices[:-1]
    prev_short_ma = calculate_sma(prev_prices, 9)
    prev_long_ma = calculate_sma(prev_prices, 21)

    print(f"LINK Price: ${link_price}")
    print(f"Short MA (9): {short_ma:.6f}")
    print(f"Long MA (21): {long_ma:.6f}")
    print(f"Prev Short MA: {prev_short_ma:.6f}")
    print(f"Prev Long MA: {prev_long_ma:.6f}")

    # Store MA data in market snapshot using the new API
    # Current MAs
    market.set_ma(
        "LINK",
        MAData(
            value=Decimal(str(short_ma)),
            ma_type="SMA",
            period=9,
            current_price=link_price,
        ),
        ma_type="SMA",
        period=9,
    )

    market.set_ma(
        "LINK",
        MAData(
            value=Decimal(str(long_ma)),
            ma_type="SMA",
            period=21,
            current_price=link_price,
        ),
        ma_type="SMA",
        period=21,
    )

    # Previous MAs (using offset period convention: period + 1000)
    market.set_ma(
        "LINK",
        MAData(
            value=Decimal(str(prev_short_ma)),
            ma_type="SMA",
            period=1009,  # 9 + 1000 to indicate previous value
            current_price=link_price,
        ),
        ma_type="SMA",
        period=1009,
    )

    market.set_ma(
        "LINK",
        MAData(
            value=Decimal(str(prev_long_ma)),
            ma_type="SMA",
            period=1021,  # 21 + 1000 to indicate previous value
            current_price=link_price,
        ),
        ma_type="SMA",
        period=1021,
    )

    # Get on-chain balances
    usdc_balance_raw = w3.eth.call(
        {
            "to": w3.to_checksum_address(USDC_ADDRESS),
            "data": bytes.fromhex("70a08231" + "000000000000000000000000" + ANVIL_WALLET[2:].lower()),
        }
    )
    usdc_balance = Decimal(int.from_bytes(usdc_balance_raw, "big")) / Decimal(10**6)

    link_balance_raw = w3.eth.call(
        {
            "to": w3.to_checksum_address(LINK_ADDRESS),
            "data": bytes.fromhex("70a08231" + "000000000000000000000000" + ANVIL_WALLET[2:].lower()),
        }
    )
    link_balance = Decimal(int.from_bytes(link_balance_raw, "big")) / Decimal(10**18)

    usdc_balance_obj = TokenBalance(
        symbol="USDC",
        balance=usdc_balance,
        balance_usd=usdc_balance,
        address=USDC_ADDRESS,
    )
    link_balance_obj = TokenBalance(
        symbol="LINK",
        balance=link_balance,
        balance_usd=link_balance * link_price,
        address=LINK_ADDRESS,
    )
    market.set_balance("USDC", usdc_balance_obj)
    market.set_balance("LINK", link_balance_obj)

    print(f"USDC Balance: ${usdc_balance:,.2f}")
    print(f"LINK Balance: {link_balance:.6f} (${link_balance * link_price:,.2f})")

    # Get intent from strategy
    print("\n--- Step 3: Strategy Decision ---")
    intent = strategy.decide(market)

    if intent is None:
        print("ERROR: Strategy returned None")
        return None

    print(f"Intent Type: {intent.intent_type.value}")

    if intent.intent_type.value == "HOLD":
        print(f"Reason: {getattr(intent, 'reason', 'No reason')}")
        return None

    if hasattr(intent, "from_token"):
        print(f"From: {intent.from_token}")
        print(f"To: {intent.to_token}")
        print(f"Amount: ${intent.amount_usd}")

    # Compile intent
    print("\n--- Step 4: Compile Intent ---")
    compiler = IntentCompiler(
        chain="arbitrum",
        wallet_address=ANVIL_WALLET,
        price_oracle={
            "LINK": link_price,
            "USDC": Decimal("1"),
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

    # Execute transactions
    print("\n--- Step 5: Execute Transactions ---")

    usdc_before = int.from_bytes(
        w3.eth.call(
            {
                "to": w3.to_checksum_address(USDC_ADDRESS),
                "data": bytes.fromhex("70a08231" + "000000000000000000000000" + ANVIL_WALLET[2:].lower()),
            }
        ),
        "big",
    )

    link_before = int.from_bytes(
        w3.eth.call(
            {
                "to": w3.to_checksum_address(LINK_ADDRESS),
                "data": bytes.fromhex("70a08231" + "000000000000000000000000" + ANVIL_WALLET[2:].lower()),
            }
        ),
        "big",
    )

    print(f"Before - USDC: {usdc_before / 10**6:,.2f}, LINK: {link_before / 10**18:.6f}")

    account = w3.eth.account.from_key(ANVIL_PRIVATE_KEY)
    nonce = w3.eth.get_transaction_count(account.address)
    gas_price = w3.eth.gas_price

    swap_receipt = None

    for i, tx_data in enumerate(action_bundle.transactions):
        tx_type = tx_data.get("tx_type", "unknown")
        description = tx_data.get("description", "Unknown")
        print(f"\n  TX {i + 1}: {description}")

        to_address = w3.to_checksum_address(tx_data["to"])
        value = int(tx_data.get("value", 0))

        try:
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

            signed_tx = account.sign_transaction(tx)
            # Handle both web3.py API versions
            raw_tx = getattr(signed_tx, "raw_transaction", None) or signed_tx.rawTransaction
            tx_hash = w3.eth.send_raw_transaction(raw_tx)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)

            status = "SUCCESS" if receipt["status"] == 1 else "REVERTED"
            print(f"    Status: {status}, Gas: {receipt['gasUsed']:,}")
            print(f"    TX Hash: {tx_hash.hex()}")

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

    # Verify results
    print("\n--- Step 6: Verify Results ---")

    usdc_after = int.from_bytes(
        w3.eth.call(
            {
                "to": w3.to_checksum_address(USDC_ADDRESS),
                "data": bytes.fromhex("70a08231" + "000000000000000000000000" + ANVIL_WALLET[2:].lower()),
            }
        ),
        "big",
    )

    link_after = int.from_bytes(
        w3.eth.call(
            {
                "to": w3.to_checksum_address(LINK_ADDRESS),
                "data": bytes.fromhex("70a08231" + "000000000000000000000000" + ANVIL_WALLET[2:].lower()),
            }
        ),
        "big",
    )

    usdc_delta = (usdc_after - usdc_before) / 10**6
    link_delta = (link_after - link_before) / 10**18

    print("\nBalance Changes:")
    print(f"  USDC: {usdc_before / 10**6:,.2f} -> {usdc_after / 10**6:,.2f} ({usdc_delta:+,.2f})")
    print(f"  LINK: {link_before / 10**18:.6f} -> {link_after / 10**18:.6f} ({link_delta:+.6f})")

    if force_action == "buy":
        return SwapResult(
            tx_hash=swap_receipt["transactionHash"].hex() if swap_receipt else "",
            action="buy",
            token_in="USDC",
            token_out="LINK",
            amount_in=Decimal(str(abs(usdc_delta))),
            amount_out=Decimal(str(link_delta)),
            gas_used=swap_receipt["gasUsed"] if swap_receipt else 0,
        )
    else:
        return SwapResult(
            tx_hash=swap_receipt["transactionHash"].hex() if swap_receipt else "",
            action="sell",
            token_in="LINK",
            token_out="USDC",
            amount_in=Decimal(str(abs(link_delta))),
            amount_out=Decimal(str(usdc_delta)),
            gas_used=swap_receipt["gasUsed"] if swap_receipt else 0,
        )


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Run MA Crossover Strategy on Anvil")
    parser.add_argument(
        "--action",
        choices=["buy", "sell"],
        default="buy",
        help="Force buy or sell action (default: buy)",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("MA CROSSOVER STRATEGY - ANVIL TEST")
    print("=" * 60)
    print(f"\nForced action: {args.action.upper()}")

    fork_url = os.getenv("ALMANAK_ARBITRUM_RPC_URL") or os.getenv("ALMANAK_RPC_URL")
    if not fork_url:
        print("ERROR: No RPC URL found in .env file")
        print("Add ALMANAK_ARBITRUM_RPC_URL or ALMANAK_RPC_URL to .env")
        sys.exit(1)

    anvil = AnvilManager(fork_url, ANVIL_PORT)
    if not anvil.start():
        sys.exit(1)

    try:
        if not fund_wallet_with_usdc(ANVIL_WALLET, FUND_AMOUNT_USDC):
            print("Failed to fund wallet with USDC")
            sys.exit(1)

        if not fund_wallet_with_link(ANVIL_WALLET, FUND_AMOUNT_LINK):
            print("Failed to fund wallet with LINK")
            sys.exit(1)

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
            print("MA CROSSOVER STRATEGY EXECUTED SUCCESSFULLY!")
            print(f"{'=' * 60}\n")
        else:
            print("\nStrategy execution did not produce a trade")

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
