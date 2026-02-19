#!/usr/bin/env python3
"""
Run Stochastic Reversion Strategy on Anvil Fork

Tests the Stochastic oscillator strategy through the full execution stack:
1. Starts Anvil fork of Arbitrum
2. Funds wallet with USDC and ARB
3. Forces a buy action via config
4. Executes the strategy and prints results

Usage:
    python strategies/tests/stochastic_reversion/run_anvil.py
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
ARB_ADDRESS = "0x912CE59144191C1204E64559FE8253a0e49E6548"
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"  # Native USDC on Arbitrum

# USDC whale for funding (Arbitrum - Aave V3 pool)
USDC_WHALE = "0x489ee077994B6658eAfA855C308275EAd8097C4A"

# Strategy parameters
TRADE_SIZE_USD = Decimal("5")
FUND_AMOUNT_USDC = 100
FUND_AMOUNT_ARB = Decimal("50")  # ~$25-50 worth of ARB


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


def fund_wallet_with_arb(wallet: str, amount_arb: Decimal) -> bool:
    """Fund wallet with ARB via whale impersonation."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_arb} ARB")
    print(f"{'=' * 60}")

    # ARB whale: Arbitrum Foundation multisig
    ARB_WHALE = "0xF3FC178157fb3c87548bAA86F9d24BA38E649B58"

    amount_wei = int(amount_arb * 10**18)

    try:
        # Check whale balance
        balance = run_cast(
            [
                "call",
                ARB_ADDRESS,
                "balanceOf(address)(uint256)",
                ARB_WHALE,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        whale_balance = parse_cast_uint(balance)
        print(f"Whale ARB balance: {whale_balance / 10**18:,.2f}")

        if whale_balance < amount_wei:
            print("WARNING: Primary whale has insufficient ARB, trying alternative...")
            # Try alternative whale - GMX treasury
            ARB_WHALE_ALT = "0x908C4D94D34924765f1eDc22A1DD098397c59dD4"
            balance = run_cast(
                [
                    "call",
                    ARB_ADDRESS,
                    "balanceOf(address)(uint256)",
                    ARB_WHALE_ALT,
                    "--rpc-url",
                    ANVIL_RPC,
                ]
            )
            whale_balance = parse_cast_uint(balance)
            print(f"Alt whale ARB balance: {whale_balance / 10**18:,.2f}")
            if whale_balance >= amount_wei:
                ARB_WHALE = ARB_WHALE_ALT
            else:
                print("ERROR: No whale with sufficient ARB")
                return False

        # Give whale ETH for gas
        run_cast(
            [
                "rpc",
                "anvil_setBalance",
                ARB_WHALE,
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
                ARB_WHALE,
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        # Transfer ARB
        run_cast(
            [
                "send",
                ARB_ADDRESS,
                "transfer(address,uint256)(bool)",
                wallet,
                str(amount_wei),
                "--from",
                ARB_WHALE,
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
                ARB_WHALE,
                "--rpc-url",
                ANVIL_RPC,
            ],
            check=False,
        )

        # Verify balance
        balance = run_cast(
            [
                "call",
                ARB_ADDRESS,
                "balanceOf(address)(uint256)",
                wallet,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        new_balance = parse_cast_uint(balance)
        print(f"Wallet ARB balance: {new_balance / 10**18:,.2f} ARB")
        return new_balance >= amount_wei

    except Exception as e:
        print(f"ERROR: Failed to fund wallet with ARB: {e}")
        return False


def calculate_stochastic(
    prices_high: list[float],
    prices_low: list[float],
    prices_close: list[float],
    fast_k: int = 14,
    slow_k: int = 3,
    slow_d: int = 3,
) -> dict:
    """
    Calculate Stochastic oscillator.

    Stochastic oscillator formula:
    %K = (Current Close - Lowest Low) / (Highest High - Lowest Low) * 100
    Slow %K = SMA of %K over slow_k periods
    %D = SMA of Slow %K over slow_d periods
    """
    if len(prices_close) < fast_k + slow_k + slow_d:
        return {"percent_k": 50.0, "percent_d": 50.0, "prev_k": 50.0, "prev_d": 50.0}

    # Calculate raw %K values
    raw_k_values = []
    for i in range(fast_k - 1, len(prices_close)):
        highest_high = max(prices_high[i - fast_k + 1 : i + 1])
        lowest_low = min(prices_low[i - fast_k + 1 : i + 1])
        if highest_high == lowest_low:
            raw_k = 50.0  # Avoid division by zero
        else:
            raw_k = ((prices_close[i] - lowest_low) / (highest_high - lowest_low)) * 100
        raw_k_values.append(raw_k)

    if len(raw_k_values) < slow_k + slow_d:
        return {"percent_k": 50.0, "percent_d": 50.0, "prev_k": 50.0, "prev_d": 50.0}

    # Calculate Slow %K (SMA of raw %K)
    slow_k_values = []
    for i in range(slow_k - 1, len(raw_k_values)):
        sma = sum(raw_k_values[i - slow_k + 1 : i + 1]) / slow_k
        slow_k_values.append(sma)

    if len(slow_k_values) < slow_d + 1:
        return {"percent_k": 50.0, "percent_d": 50.0, "prev_k": 50.0, "prev_d": 50.0}

    # Calculate %D (SMA of Slow %K)
    slow_d_values = []
    for i in range(slow_d - 1, len(slow_k_values)):
        sma = sum(slow_k_values[i - slow_d + 1 : i + 1]) / slow_d
        slow_d_values.append(sma)

    if len(slow_d_values) < 2:
        return {
            "percent_k": slow_k_values[-1],
            "percent_d": slow_k_values[-1],
            "prev_k": slow_k_values[-1],
            "prev_d": slow_k_values[-1],
        }

    return {
        "percent_k": slow_k_values[-1],
        "percent_d": slow_d_values[-1],
        "prev_k": slow_k_values[-2],
        "prev_d": slow_d_values[-2] if len(slow_d_values) >= 2 else slow_d_values[-1],
    }


def run_strategy_on_anvil(force_action: str = "buy") -> SwapResult | None:
    """Run Stochastic Reversion Strategy on Anvil fork."""
    print(f"\n{'=' * 60}")
    print(f"RUNNING STOCHASTIC REVERSION STRATEGY (force: {force_action})")
    print(f"{'=' * 60}")

    from web3 import Web3

    from almanak.framework.intents import IntentCompiler
    from almanak.framework.intents.compiler import CompilationStatus
    from almanak.framework.models.hot_reload_config import HotReloadableConfig
    from almanak.framework.strategies import MarketSnapshot, StochasticData
    from almanak.framework.strategies.intent_strategy import TokenBalance
    from strategies.tests.stochastic_reversion import StochasticReversionStrategy

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
    config.stoch_fast_k = 14
    config.stoch_slow_k = 3
    config.stoch_slow_d = 3
    config.overbought = 80
    config.oversold = 20
    config.max_slippage_bps = 300
    config.base_token = "ARB"
    config.quote_token = "USDC"
    config.protocol = "uniswap_v3"
    config.force_action = force_action

    strategy = StochasticReversionStrategy(
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

    arb_price = Decimal("0.50")  # ARB price in USD
    market.set_price("ARB", arb_price)
    market.set_price("USDC", Decimal("1"))

    # Calculate Stochastic from simulated price history
    # Simulate 40 periods of price data for Stochastic calculation
    import random

    random.seed(42)  # For reproducibility
    base_price = float(arb_price)
    prices_high = []
    prices_low = []
    prices_close = []
    for _ in range(40):
        # Simulate OHLC data
        open_price = base_price
        change = random.uniform(-0.03, 0.035)
        close_price = open_price * (1 + change)
        high_price = max(open_price, close_price) * (1 + random.uniform(0, 0.015))
        low_price = min(open_price, close_price) * (1 - random.uniform(0, 0.015))
        prices_high.append(high_price)
        prices_low.append(low_price)
        prices_close.append(close_price)
        base_price = close_price

    stoch_data = calculate_stochastic(prices_high, prices_low, prices_close, fast_k=14, slow_k=3, slow_d=3)

    print(f"ARB Price: ${arb_price}")
    print(f"%K (Slow): {stoch_data['percent_k']:.2f}")
    print(f"%D: {stoch_data['percent_d']:.2f}")
    print(f"Previous %K: {stoch_data['prev_k']:.2f}")
    print(f"Previous %D: {stoch_data['prev_d']:.2f}")

    # Store Stochastic data in market snapshot
    market.set_stochastic(
        "ARB",
        StochasticData(
            k_value=Decimal(str(stoch_data["percent_k"])),
            d_value=Decimal(str(stoch_data["percent_d"])),
            k_period=14,
            d_period=3,
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

    arb_balance_raw = w3.eth.call(
        {
            "to": w3.to_checksum_address(ARB_ADDRESS),
            "data": bytes.fromhex("70a08231" + "000000000000000000000000" + ANVIL_WALLET[2:].lower()),
        }
    )
    arb_balance = Decimal(int.from_bytes(arb_balance_raw, "big")) / Decimal(10**18)

    usdc_balance_obj = TokenBalance(
        symbol="USDC",
        balance=usdc_balance,
        balance_usd=usdc_balance,
        address=USDC_ADDRESS,
    )
    arb_balance_obj = TokenBalance(
        symbol="ARB",
        balance=arb_balance,
        balance_usd=arb_balance * arb_price,
        address=ARB_ADDRESS,
    )
    market.set_balance("USDC", usdc_balance_obj)
    market.set_balance("ARB", arb_balance_obj)

    print(f"USDC Balance: ${usdc_balance:,.2f}")
    print(f"ARB Balance: {arb_balance:.6f} (${arb_balance * arb_price:,.2f})")

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
            "ARB": arb_price,
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

    arb_before = int.from_bytes(
        w3.eth.call(
            {
                "to": w3.to_checksum_address(ARB_ADDRESS),
                "data": bytes.fromhex("70a08231" + "000000000000000000000000" + ANVIL_WALLET[2:].lower()),
            }
        ),
        "big",
    )

    print(f"Before - USDC: {usdc_before / 10**6:,.2f}, ARB: {arb_before / 10**18:.6f}")

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

    arb_after = int.from_bytes(
        w3.eth.call(
            {
                "to": w3.to_checksum_address(ARB_ADDRESS),
                "data": bytes.fromhex("70a08231" + "000000000000000000000000" + ANVIL_WALLET[2:].lower()),
            }
        ),
        "big",
    )

    usdc_delta = (usdc_after - usdc_before) / 10**6
    arb_delta = (arb_after - arb_before) / 10**18

    print("\nBalance Changes:")
    print(f"  USDC: {usdc_before / 10**6:,.2f} -> {usdc_after / 10**6:,.2f} ({usdc_delta:+,.2f})")
    print(f"  ARB: {arb_before / 10**18:.6f} -> {arb_after / 10**18:.6f} ({arb_delta:+.6f})")

    if force_action == "buy":
        return SwapResult(
            tx_hash=swap_receipt["transactionHash"].hex() if swap_receipt else "",
            action="buy",
            token_in="USDC",
            token_out="ARB",
            amount_in=Decimal(str(abs(usdc_delta))),
            amount_out=Decimal(str(arb_delta)),
            gas_used=swap_receipt["gasUsed"] if swap_receipt else 0,
        )
    else:
        return SwapResult(
            tx_hash=swap_receipt["transactionHash"].hex() if swap_receipt else "",
            action="sell",
            token_in="ARB",
            token_out="USDC",
            amount_in=Decimal(str(abs(arb_delta))),
            amount_out=Decimal(str(usdc_delta)),
            gas_used=swap_receipt["gasUsed"] if swap_receipt else 0,
        )


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Run Stochastic Reversion Strategy on Anvil")
    parser.add_argument(
        "--action",
        choices=["buy", "sell"],
        default="buy",
        help="Force buy or sell action (default: buy)",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("STOCHASTIC REVERSION STRATEGY - ANVIL TEST")
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

        if not fund_wallet_with_arb(ANVIL_WALLET, FUND_AMOUNT_ARB):
            print("Failed to fund wallet with ARB")
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
            print("STOCHASTIC REVERSION STRATEGY EXECUTED SUCCESSFULLY!")
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
