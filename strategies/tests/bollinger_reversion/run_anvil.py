#!/usr/bin/env python3
"""
Run Bollinger Bands Reversion Strategy on Anvil Fork

Tests the Bollinger Bands reversion strategy through the full execution stack:
1. Starts Anvil fork of Arbitrum
2. Funds wallet with USDC and WETH
3. Forces a buy action via config
4. Executes the strategy and prints results

Usage:
    python strategies/tests/bollinger_reversion/run_anvil.py
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

# Arbitrum token addresses (from PRD notes)
WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"  # Native USDC on Arbitrum

# USDC whale for funding (Arbitrum)
USDC_WHALE = "0x489ee077994B6658eAfA855C308275EAd8097C4A"

# Strategy parameters
TRADE_SIZE_USD = Decimal("5")
FUND_AMOUNT_USDC = 100
FUND_AMOUNT_WETH = Decimal("0.1")


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


def fund_wallet_with_weth(wallet: str, amount_weth: Decimal) -> bool:
    """Fund wallet with WETH by wrapping ETH."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_weth} WETH")
    print(f"{'=' * 60}")

    amount_wei = int(amount_weth * 10**18)

    try:
        # Ensure wallet has ETH
        run_cast(
            [
                "rpc",
                "anvil_setBalance",
                wallet,
                hex(10 * 10**18),
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        # Wrap ETH to WETH
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


def calculate_bollinger_bands(prices: list[float], period: int, std_dev: float) -> dict:
    """
    Calculate Bollinger Bands from price list.

    Bollinger Bands:
    - Middle Band = SMA(period)
    - Upper Band = Middle Band + (std_dev * standard deviation)
    - Lower Band = Middle Band - (std_dev * standard deviation)
    """
    if len(prices) < period:
        return {"upper_band": 0, "middle_band": 0, "lower_band": 0}

    # Get the last 'period' prices
    recent_prices = prices[-period:]

    # Calculate SMA (middle band)
    middle_band = sum(recent_prices) / period

    # Calculate standard deviation
    variance = sum((p - middle_band) ** 2 for p in recent_prices) / period
    std = variance**0.5

    # Calculate bands
    upper_band = middle_band + (std_dev * std)
    lower_band = middle_band - (std_dev * std)

    return {
        "upper_band": upper_band,
        "middle_band": middle_band,
        "lower_band": lower_band,
    }


def run_strategy_on_anvil(force_action: str = "buy") -> SwapResult | None:
    """Run Bollinger Bands Reversion Strategy on Anvil fork."""
    print(f"\n{'=' * 60}")
    print(f"RUNNING BOLLINGER BANDS REVERSION STRATEGY (force: {force_action})")
    print(f"{'=' * 60}")

    from web3 import Web3

    from almanak.framework.intents import IntentCompiler
    from almanak.framework.intents.compiler import CompilationStatus
    from almanak.framework.models.hot_reload_config import HotReloadableConfig
    from almanak.framework.strategies import BollingerBandsData, MarketSnapshot, TokenBalance
    from strategies.tests.bollinger_reversion import BollingerReversionStrategy

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
    config.bb_period = 20
    config.bb_std_dev = 2.0
    config.max_slippage_bps = 300
    config.base_token = "WETH"
    config.quote_token = "USDC"
    config.protocol = "uniswap_v3"
    config.force_action = force_action

    strategy = BollingerReversionStrategy(
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

    eth_price = Decimal("3400")
    market.set_price("WETH", eth_price)
    market.set_price("ETH", eth_price)
    market.set_price("USDC", Decimal("1"))

    # Calculate Bollinger Bands from simulated price history
    # Simulate 25 periods of price data for BB calculation (need at least 20)
    import random

    random.seed(42)  # For reproducibility
    base_price = float(eth_price)
    prices = []
    for _ in range(25):
        # Simulate price movement with slight volatility
        change = random.uniform(-0.02, 0.02)
        base_price = base_price * (1 + change)
        prices.append(base_price)

    bb_data = calculate_bollinger_bands(prices, period=20, std_dev=2.0)

    print(f"WETH Price: ${eth_price}")
    print(f"Upper Band: ${bb_data['upper_band']:.2f}")
    print(f"Middle Band (SMA20): ${bb_data['middle_band']:.2f}")
    print(f"Lower Band: ${bb_data['lower_band']:.2f}")

    # Store BB data in market snapshot using the new API
    market.set_bollinger_bands(
        "WETH",
        BollingerBandsData(
            upper_band=Decimal(str(bb_data["upper_band"])),
            middle_band=Decimal(str(bb_data["middle_band"])),
            lower_band=Decimal(str(bb_data["lower_band"])),
            period=20,
            std_dev=2.0,
        ),
    )

    # Get on-chain balances
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

    usdc_balance_obj = TokenBalance(
        symbol="USDC",
        balance=usdc_balance,
        balance_usd=usdc_balance,
        address=USDC_ADDRESS,
    )
    weth_balance_obj = TokenBalance(
        symbol="WETH",
        balance=weth_balance,
        balance_usd=weth_balance * eth_price,
        address=WETH_ADDRESS,
    )
    market.set_balance("USDC", usdc_balance_obj)
    market.set_balance("WETH", weth_balance_obj)

    print(f"USDC Balance: ${usdc_balance:,.2f}")
    print(f"WETH Balance: {weth_balance:.6f} (${weth_balance * eth_price:,.2f})")

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
            "ETH": eth_price,
            "WETH": eth_price,
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

    if force_action == "buy":
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
        return SwapResult(
            tx_hash=swap_receipt["transactionHash"].hex() if swap_receipt else "",
            action="sell",
            token_in="WETH",
            token_out="USDC",
            amount_in=Decimal(str(abs(weth_delta))),
            amount_out=Decimal(str(usdc_delta)),
            gas_used=swap_receipt["gasUsed"] if swap_receipt else 0,
        )


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Run Bollinger Bands Reversion Strategy on Anvil")
    parser.add_argument(
        "--action",
        choices=["buy", "sell"],
        default="buy",
        help="Force buy or sell action (default: buy)",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("BOLLINGER BANDS REVERSION STRATEGY - ANVIL TEST")
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

        if not fund_wallet_with_weth(ANVIL_WALLET, FUND_AMOUNT_WETH):
            print("Failed to fund wallet with WETH")
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
            print("BOLLINGER BANDS REVERSION STRATEGY EXECUTED SUCCESSFULLY!")
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
