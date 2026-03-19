#!/usr/bin/env python3
"""
===============================================================================
TUTORIAL: Running a SushiSwap V3 LP Strategy on Anvil (Local Fork)
===============================================================================

This script demonstrates how to test a SushiSwap V3 LP strategy on Anvil.
It forks Arbitrum mainnet and tests concentrated liquidity LP operations.

WHAT THIS SCRIPT DOES:
----------------------
1. Starts an Anvil fork of Arbitrum mainnet
2. Funds the test wallet with WETH and USDC
3. Tests SWAP, LP_OPEN, and LP_CLOSE operations
4. Validates receipt parsing with transaction hashes

PREREQUISITES:
--------------
1. Foundry installed (provides anvil and cast)
   curl -L https://foundry.paradigm.xyz | bash && foundryup

2. ALCHEMY_API_KEY in environment or .env file

3. Python dependencies installed:
   uv sync

USAGE:
------
    python strategies/demo/sushiswap_lp/run_anvil.py

    # With custom options:
    python strategies/demo/sushiswap_lp/run_anvil.py --action open
    python strategies/demo/sushiswap_lp/run_anvil.py --action close
    python strategies/demo/sushiswap_lp/run_anvil.py --action swap

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

# Arbitrum token addresses
WETH_ADDRESS = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC_ADDRESS = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

# SushiSwap V3 addresses on Arbitrum
SUSHISWAP_V3_ROUTER = "0x8A21F6768C1f8075791D08546Dadf6daA0bE820c"
SUSHISWAP_V3_POSITION_MANAGER = "0xF0cBce1942A68BEB3d1b73F0dd86C8DCc363eF49"

# USDC whale on Arbitrum (Aave aUSDC)
USDC_WHALE = "0x724dc807b04555b71ed48a6896b6F41593b8C637"

# Amounts to fund
FUND_AMOUNT_USDC = 1000  # 1000 USDC
FUND_AMOUNT_ETH = Decimal("1")  # 1 ETH (for WETH)

# Anvil settings
ANVIL_PORT = 8545
ANVIL_RPC = f"http://127.0.0.1:{ANVIL_PORT}"

# Arbitrum Chain ID
ARBITRUM_CHAIN_ID = 42161


# =============================================================================
# ANVIL MANAGER
# =============================================================================


class AnvilManager:
    """Manages the Anvil fork lifecycle."""

    def __init__(self, fork_url: str, port: int = 8545, chain_id: int = 42161):
        self.fork_url = fork_url
        self.port = port
        self.chain_id = chain_id
        self.process: subprocess.Popen | None = None

    def start(self) -> bool:
        """Start Anvil fork."""
        print(f"\n{'=' * 60}")
        print("STARTING ANVIL FORK OF ARBITRUM MAINNET")
        print(f"{'=' * 60}")
        # Redact API key from fork URL for logging
        url_parts = self.fork_url.split("/")
        redacted_url = "/".join(url_parts[:3]) + "/...[REDACTED]" if len(url_parts) > 3 else self.fork_url
        print(f"Forking from: {redacted_url}")

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
# HELPER FUNCTIONS
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
    if output.startswith("0x"):
        return int(output, 16)
    return int(output.replace(",", ""))


def fund_native_token(wallet: str, amount_wei: int) -> None:
    """Fund a wallet with ETH using Anvil RPC."""
    amount_hex = hex(amount_wei)
    run_cast(
        ["rpc", "anvil_setBalance", wallet, amount_hex, "--rpc-url", ANVIL_RPC],
        check=False,
    )


def get_token_balance(token_address: str, wallet: str) -> int:
    """Get ERC20 token balance for a wallet."""
    balance = run_cast(
        [
            "call",
            token_address,
            "balanceOf(address)(uint256)",
            wallet,
            "--rpc-url",
            ANVIL_RPC,
        ],
        check=False,
    )
    if balance:
        return parse_cast_uint(balance)
    return 0


def format_token(amount_wei: int, decimals: int = 18) -> Decimal:
    """Convert wei to token units."""
    return Decimal(amount_wei) / Decimal(10**decimals)


def send_transaction(to: str, data: str, value: int = 0, gas: int = 500000) -> str:
    """Send a transaction using web3.py and return the tx hash."""
    from web3 import Web3

    w3 = Web3(Web3.HTTPProvider(ANVIL_RPC))

    # Build transaction
    tx = {
        "from": Web3.to_checksum_address(ANVIL_WALLET),
        "to": Web3.to_checksum_address(to),
        "data": data,
        "value": value,
        "gas": gas,
        "gasPrice": w3.eth.gas_price,
        "nonce": w3.eth.get_transaction_count(Web3.to_checksum_address(ANVIL_WALLET)),
        "chainId": ARBITRUM_CHAIN_ID,
    }

    # Sign and send
    signed_tx = w3.eth.account.sign_transaction(tx, ANVIL_PRIVATE_KEY)
    tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction)

    return tx_hash.hex()


# =============================================================================
# WALLET FUNDING
# =============================================================================


def fund_wallet_with_usdc(wallet: str, amount_usdc: int) -> bool:
    """Fund wallet with USDC by impersonating a whale."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_usdc} USDC")
    print(f"{'=' * 60}")

    amount_wei = amount_usdc * 10**6

    try:
        # Check whale balance
        whale_balance = get_token_balance(USDC_ADDRESS, USDC_WHALE)
        print(f"Whale USDC balance: {whale_balance / 10**6:,.2f}")

        if whale_balance < amount_wei:
            print("WARNING: Whale has insufficient USDC")
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
        new_balance = get_token_balance(USDC_ADDRESS, wallet)
        print(f"Wallet USDC balance after funding: {new_balance / 10**6:,.2f}")
        return new_balance >= amount_wei

    except Exception as e:
        print(f"ERROR: Failed to fund wallet with USDC: {e}")
        return False


def fund_wallet_with_weth(wallet: str, amount_eth: Decimal) -> bool:
    """Fund wallet with WETH by wrapping ETH."""
    print(f"\n{'=' * 60}")
    print(f"FUNDING WALLET WITH {amount_eth} WETH")
    print(f"{'=' * 60}")

    amount_wei = int(amount_eth * 10**18)

    try:
        # Ensure wallet has ETH (native token)
        fund_native_token(wallet, int(100 * 10**18))  # 100 ETH

        # Check ETH balance
        eth_balance = run_cast(["balance", wallet, "--rpc-url", ANVIL_RPC])
        print(f"Wallet ETH balance: {eth_balance}")

        # Wrap ETH to WETH
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
        weth_balance = get_token_balance(WETH_ADDRESS, wallet)
        print(f"Wallet WETH balance: {format_token(weth_balance):.6f}")
        return weth_balance >= amount_wei

    except Exception as e:
        print(f"ERROR: Failed to fund wallet with WETH: {e}")
        import traceback

        traceback.print_exc()
        return False


# =============================================================================
# TEST OPERATIONS
# =============================================================================


def test_swap() -> dict | None:
    """Test SWAP operation on SushiSwap V3."""
    print(f"\n{'=' * 60}")
    print("TEST: SWAP WETH -> USDC on SushiSwap V3")
    print(f"{'=' * 60}")

    try:
        from web3 import Web3

        from almanak.framework.connectors.sushiswap_v3 import (
            SushiSwapV3Adapter,
            SushiSwapV3Config,
            SushiSwapV3ReceiptParser,
        )

        # Create adapter with realistic price based on on-chain pool
        # SushiSwap V3 WETH/USDC pools on Arbitrum have ETH around $2300-2400
        price_provider = {"WETH": Decimal("2400"), "USDC": Decimal("1")}
        config = SushiSwapV3Config(
            chain="arbitrum",
            wallet_address=ANVIL_WALLET,
            price_provider=price_provider,
            default_slippage_bps=500,  # 5% slippage for testing on lower liquidity pools
            default_fee_tier=500,  # Use 0.05% fee tier which has more liquidity
        )
        adapter = SushiSwapV3Adapter(config)

        # Approve WETH for router
        swap_amount = Decimal("0.1")  # 0.1 WETH
        swap_amount_wei = int(swap_amount * Decimal(10**18))

        print(f"\n1. Approving {swap_amount} WETH for router...")
        run_cast(
            [
                "send",
                WETH_ADDRESS,
                "approve(address,uint256)(bool)",
                SUSHISWAP_V3_ROUTER,
                str(swap_amount_wei),
                "--from",
                ANVIL_WALLET,
                "--private-key",
                ANVIL_PRIVATE_KEY,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        print("   Approval successful")

        # Build swap transaction
        print(f"\n2. Building swap transaction: {swap_amount} WETH -> USDC...")
        result = adapter.swap_exact_input(
            token_in="WETH",
            token_out="USDC",
            amount_in=swap_amount,
        )

        if not result.success:
            print(f"   ERROR: Swap build failed: {result.error}")
            return None

        # Find swap transaction
        swap_tx_data = None
        for tx in result.transactions:
            if tx.tx_type == "swap":
                swap_tx_data = tx
                break

        if not swap_tx_data:
            print("   ERROR: No swap transaction found")
            return None

        # Get balances before
        weth_before = get_token_balance(WETH_ADDRESS, ANVIL_WALLET)
        usdc_before = get_token_balance(USDC_ADDRESS, ANVIL_WALLET)
        print(f"\n   WETH before: {format_token(weth_before):.6f}")
        print(f"   USDC before: {format_token(usdc_before, 6):.2f}")

        # Execute swap
        print("\n3. Executing swap transaction...")
        tx_hash = send_transaction(
            to=swap_tx_data.to,
            data=swap_tx_data.data,
            value=swap_tx_data.value,
            gas=500000,
        )
        print(f"   TX Hash: {tx_hash}")

        # Get receipt
        receipt_json = run_cast(
            [
                "receipt",
                tx_hash,
                "--json",
                "--rpc-url",
                ANVIL_RPC,
            ]
        )

        import json

        receipt = json.loads(receipt_json)

        # Check status
        status = int(receipt.get("status", "0x0"), 16)
        if status != 1:
            print(f"   ERROR: Transaction failed with status {status}")
            return None
        print("   Transaction successful!")

        # Get balances after
        weth_after = get_token_balance(WETH_ADDRESS, ANVIL_WALLET)
        usdc_after = get_token_balance(USDC_ADDRESS, ANVIL_WALLET)
        print(f"\n   WETH after: {format_token(weth_after):.6f}")
        print(f"   USDC after: {format_token(usdc_after, 6):.2f}")
        print(f"   WETH spent: {format_token(weth_before - weth_after):.6f}")
        print(f"   USDC received: {format_token(usdc_after - usdc_before, 6):.2f}")

        # Parse receipt
        print("\n4. Parsing receipt...")
        parser = SushiSwapV3ReceiptParser(
            chain="arbitrum",
            token0_address=USDC_ADDRESS,
            token1_address=WETH_ADDRESS,
        )

        # Convert receipt to web3 format
        w3 = Web3(Web3.HTTPProvider(ANVIL_RPC))
        web3_receipt = w3.eth.get_transaction_receipt(tx_hash)
        parse_result = parser.parse_receipt(dict(web3_receipt))

        if parse_result.success:
            print(f"   Parse success: {parse_result.success}")
            if parse_result.swap_events:
                swap_event = parse_result.swap_events[0]
                print("   Swap event found:")
                print(f"     - Amount in: {swap_event.amount_in}")
                print(f"     - Amount out: {swap_event.amount_out}")
                print(f"     - Token1 is input: {swap_event.token1_is_input}")

            # Extract swap amounts
            swap_amounts = parser.extract_swap_amounts(dict(web3_receipt))
            if swap_amounts:
                print("   Extracted swap amounts:")
                print(f"     - Amount in: {swap_amounts.amount_in_decimal}")
                print(f"     - Amount out: {swap_amounts.amount_out_decimal}")
                print(f"     - Effective price: {swap_amounts.effective_price}")
        else:
            print(f"   Parse error: {parse_result.error}")

        print(f"\n{'=' * 60}")
        print("SWAP TEST PASSED!")
        print(f"{'=' * 60}")

        return {
            "tx_hash": tx_hash,
            "weth_spent": format_token(weth_before - weth_after),
            "usdc_received": format_token(usdc_after - usdc_before, 6),
        }

    except Exception as e:
        print(f"\nERROR: Swap test failed: {e}")
        import traceback

        traceback.print_exc()
        return None


def test_lp_open() -> dict | None:
    """Test LP_OPEN operation on SushiSwap V3."""
    print(f"\n{'=' * 60}")
    print("TEST: LP_OPEN on SushiSwap V3 (WETH/USDC)")
    print(f"{'=' * 60}")

    try:
        from web3 import Web3

        from almanak.framework.connectors.sushiswap_v3 import (
            SushiSwapV3Adapter,
            SushiSwapV3Config,
            SushiSwapV3ReceiptParser,
        )

        # Create adapter
        price_provider = {"WETH": Decimal("2400"), "USDC": Decimal("1")}
        config = SushiSwapV3Config(
            chain="arbitrum",
            wallet_address=ANVIL_WALLET,
            price_provider=price_provider,
        )
        adapter = SushiSwapV3Adapter(config)

        # Amounts for LP
        amount_usdc = Decimal("100")  # 100 USDC
        amount_weth = Decimal("0.03")  # ~$100 worth of WETH

        # Approve tokens for position manager
        print("\n1. Approving tokens for Position Manager...")
        run_cast(
            [
                "send",
                USDC_ADDRESS,
                "approve(address,uint256)(bool)",
                SUSHISWAP_V3_POSITION_MANAGER,
                str(int(amount_usdc * 10**6)),
                "--from",
                ANVIL_WALLET,
                "--private-key",
                ANVIL_PRIVATE_KEY,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        run_cast(
            [
                "send",
                WETH_ADDRESS,
                "approve(address,uint256)(bool)",
                SUSHISWAP_V3_POSITION_MANAGER,
                str(int(amount_weth * 10**18)),
                "--from",
                ANVIL_WALLET,
                "--private-key",
                ANVIL_PRIVATE_KEY,
                "--rpc-url",
                ANVIL_RPC,
            ]
        )
        print("   Approvals successful")

        # Build LP open transaction
        print("\n2. Building LP_OPEN transaction...")
        print(f"   Amount USDC: {amount_usdc}")
        print(f"   Amount WETH: {amount_weth}")

        result = adapter.open_lp_position(
            token0="USDC",
            token1="WETH",
            amount0=amount_usdc,
            amount1=amount_weth,
            fee_tier=3000,  # 0.3%
            tick_lower=-887220,  # Full range
            tick_upper=887220,
            slippage_bps=9900,  # 99% slippage for testing (allows any deposit ratio)
        )

        if not result.success:
            print(f"   ERROR: LP open build failed: {result.error}")
            return None

        # Find mint transaction
        mint_tx_data = None
        for tx in result.transactions:
            if tx.tx_type == "mint":
                mint_tx_data = tx
                break

        if not mint_tx_data:
            print("   ERROR: No mint transaction found")
            return None

        print(f"   Tick range: {result.position_info.get('tick_lower')} to {result.position_info.get('tick_upper')}")

        # Get balances before
        weth_before = get_token_balance(WETH_ADDRESS, ANVIL_WALLET)
        usdc_before = get_token_balance(USDC_ADDRESS, ANVIL_WALLET)
        print(f"\n   WETH before: {format_token(weth_before):.6f}")
        print(f"   USDC before: {format_token(usdc_before, 6):.2f}")

        # Execute mint
        print("\n3. Executing LP_OPEN (mint) transaction...")
        tx_hash = send_transaction(
            to=mint_tx_data.to,
            data=mint_tx_data.data,
            value=mint_tx_data.value,
            gas=800000,
        )
        print(f"   TX Hash: {tx_hash}")

        # Get receipt
        receipt_json = run_cast(
            [
                "receipt",
                tx_hash,
                "--json",
                "--rpc-url",
                ANVIL_RPC,
            ]
        )

        import json

        receipt = json.loads(receipt_json)

        # Check status
        status = int(receipt.get("status", "0x0"), 16)
        if status != 1:
            print(f"   ERROR: Transaction failed with status {status}")
            return None
        print("   Transaction successful!")

        # Get balances after
        weth_after = get_token_balance(WETH_ADDRESS, ANVIL_WALLET)
        usdc_after = get_token_balance(USDC_ADDRESS, ANVIL_WALLET)
        print(f"\n   WETH after: {format_token(weth_after):.6f}")
        print(f"   USDC after: {format_token(usdc_after, 6):.2f}")
        print(f"   WETH deposited: {format_token(weth_before - weth_after):.6f}")
        print(f"   USDC deposited: {format_token(usdc_before - usdc_after, 6):.2f}")

        # Parse receipt to get position ID
        print("\n4. Parsing receipt for position ID...")
        parser = SushiSwapV3ReceiptParser(chain="arbitrum")

        # Convert receipt to web3 format
        w3 = Web3(Web3.HTTPProvider(ANVIL_RPC))
        web3_receipt = w3.eth.get_transaction_receipt(tx_hash)

        position_id = parser.extract_position_id(dict(web3_receipt))
        liquidity = parser.extract_liquidity(dict(web3_receipt))
        tick_lower = parser.extract_tick_lower(dict(web3_receipt))
        tick_upper = parser.extract_tick_upper(dict(web3_receipt))

        print(f"   Position ID: {position_id}")
        print(f"   Liquidity: {liquidity}")
        print(f"   Tick lower: {tick_lower}")
        print(f"   Tick upper: {tick_upper}")

        print(f"\n{'=' * 60}")
        print("LP_OPEN TEST PASSED!")
        print(f"{'=' * 60}")

        return {
            "tx_hash": tx_hash,
            "position_id": position_id,
            "liquidity": liquidity,
            "tick_lower": tick_lower,
            "tick_upper": tick_upper,
        }

    except Exception as e:
        print(f"\nERROR: LP_OPEN test failed: {e}")
        import traceback

        traceback.print_exc()
        return None


def test_lp_close(position_id: int, liquidity: int) -> dict | None:
    """Test LP_CLOSE operation on SushiSwap V3."""
    print(f"\n{'=' * 60}")
    print(f"TEST: LP_CLOSE position {position_id} on SushiSwap V3")
    print(f"{'=' * 60}")

    try:
        from web3 import Web3

        from almanak.framework.connectors.sushiswap_v3 import (
            SushiSwapV3Adapter,
            SushiSwapV3Config,
            SushiSwapV3ReceiptParser,
        )

        # Create adapter
        price_provider = {"WETH": Decimal("2400"), "USDC": Decimal("1")}
        config = SushiSwapV3Config(
            chain="arbitrum",
            wallet_address=ANVIL_WALLET,
            price_provider=price_provider,
        )
        adapter = SushiSwapV3Adapter(config)

        # Build LP close transaction
        print("\n1. Building LP_CLOSE transaction...")
        print(f"   Position ID: {position_id}")
        print(f"   Liquidity: {liquidity}")

        result = adapter.close_lp_position(
            token_id=position_id,
            liquidity=liquidity,
            amount0_min=0,
            amount1_min=0,
        )

        if not result.success:
            print(f"   ERROR: LP close build failed: {result.error}")
            return None

        print(f"   Built {len(result.transactions)} transactions")

        # Get balances before
        weth_before = get_token_balance(WETH_ADDRESS, ANVIL_WALLET)
        usdc_before = get_token_balance(USDC_ADDRESS, ANVIL_WALLET)
        print(f"\n   WETH before: {format_token(weth_before):.6f}")
        print(f"   USDC before: {format_token(usdc_before, 6):.2f}")

        # Execute all transactions
        tx_hashes = []
        for i, tx in enumerate(result.transactions):
            print(f"\n2.{i + 1}. Executing {tx.tx_type} transaction...")
            tx_hash = send_transaction(
                to=tx.to,
                data=tx.data,
                value=tx.value,
                gas=500000,
            )
            print(f"   TX Hash: {tx_hash}")
            tx_hashes.append(tx_hash)

            # Check receipt
            receipt_json = run_cast(
                [
                    "receipt",
                    tx_hash,
                    "--json",
                    "--rpc-url",
                    ANVIL_RPC,
                ]
            )

            import json

            receipt = json.loads(receipt_json)
            status = int(receipt.get("status", "0x0"), 16)
            if status != 1:
                print(f"   ERROR: Transaction failed with status {status}")
                return None
            print("   Transaction successful!")

        # Get balances after
        weth_after = get_token_balance(WETH_ADDRESS, ANVIL_WALLET)
        usdc_after = get_token_balance(USDC_ADDRESS, ANVIL_WALLET)
        print(f"\n   WETH after: {format_token(weth_after):.6f}")
        print(f"   USDC after: {format_token(usdc_after, 6):.2f}")
        print(f"   WETH withdrawn: {format_token(weth_after - weth_before):.6f}")
        print(f"   USDC withdrawn: {format_token(usdc_after - usdc_before, 6):.2f}")

        # Parse last receipt (collect) for LP close data
        if tx_hashes:
            print("\n3. Parsing receipt for LP close data...")
            parser = SushiSwapV3ReceiptParser(chain="arbitrum")

            w3 = Web3(Web3.HTTPProvider(ANVIL_RPC))
            web3_receipt = w3.eth.get_transaction_receipt(tx_hashes[-1])

            lp_close_data = parser.extract_lp_close_data(dict(web3_receipt))
            if lp_close_data:
                print(f"   Amount0 collected: {lp_close_data.amount0_collected}")
                print(f"   Amount1 collected: {lp_close_data.amount1_collected}")
                print(f"   Liquidity removed: {lp_close_data.liquidity_removed}")

        print(f"\n{'=' * 60}")
        print("LP_CLOSE TEST PASSED!")
        print(f"{'=' * 60}")

        return {
            "tx_hashes": tx_hashes,
            "weth_withdrawn": format_token(weth_after - weth_before),
            "usdc_withdrawn": format_token(usdc_after - usdc_before, 6),
        }

    except Exception as e:
        print(f"\nERROR: LP_CLOSE test failed: {e}")
        import traceback

        traceback.print_exc()
        return None


# =============================================================================
# MAIN
# =============================================================================


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Run SushiSwap V3 LP tests on Anvil")
    parser.add_argument(
        "--action",
        choices=["all", "swap", "open", "close"],
        default="all",
        help="Test action to run (default: all)",
    )
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("ALMANAK DEMO - SUSHISWAP V3 LP STRATEGY ON ANVIL")
    print("=" * 60)
    print("\nThis test validates SushiSwap V3 operations:")
    print("  1. SWAP: Exchange tokens")
    print("  2. LP_OPEN: Create LP position")
    print("  3. LP_CLOSE: Remove LP position")
    print(f"\nAction: {args.action.upper()}")
    print("")

    # Get RPC URL
    alchemy_key = os.getenv("ALCHEMY_API_KEY")
    if not alchemy_key:
        print("ERROR: ALCHEMY_API_KEY not set")
        print("Please set ALCHEMY_API_KEY in your environment or .env file")
        sys.exit(1)

    fork_url = f"https://arb-mainnet.g.alchemy.com/v2/{alchemy_key}"
    # Redact API key from fork URL for logging
    print("Fork URL: https://arb-mainnet.g.alchemy.com/v2/...[REDACTED]")

    # Start Anvil
    anvil = AnvilManager(fork_url, ANVIL_PORT, ARBITRUM_CHAIN_ID)
    if not anvil.start():
        sys.exit(1)

    try:
        # Fund wallet
        if not fund_wallet_with_weth(ANVIL_WALLET, FUND_AMOUNT_ETH):
            print("Failed to fund wallet with WETH")
            sys.exit(1)

        if not fund_wallet_with_usdc(ANVIL_WALLET, FUND_AMOUNT_USDC):
            print("Failed to fund wallet with USDC")
            sys.exit(1)

        results = {}

        # Run tests based on action
        if args.action in ["all", "swap"]:
            swap_result = test_swap()
            if swap_result:
                results["swap"] = swap_result
            else:
                print("\nSWAP test failed!")
                if args.action == "swap":
                    sys.exit(1)

        position_id = None
        liquidity = None

        if args.action in ["all", "open"]:
            lp_open_result = test_lp_open()
            if lp_open_result:
                results["lp_open"] = lp_open_result
                position_id = lp_open_result.get("position_id")
                liquidity = lp_open_result.get("liquidity")
            else:
                print("\nLP_OPEN test failed!")
                if args.action == "open":
                    sys.exit(1)

        if args.action in ["all", "close"]:
            if position_id and liquidity:
                lp_close_result = test_lp_close(position_id, liquidity)
                if lp_close_result:
                    results["lp_close"] = lp_close_result
                else:
                    print("\nLP_CLOSE test failed!")
                    if args.action == "close":
                        sys.exit(1)
            elif args.action == "close":
                print("\nERROR: No position to close (run 'open' first)")
                sys.exit(1)

        # Print summary
        print(f"\n{'=' * 60}")
        print("TEST SUMMARY")
        print(f"{'=' * 60}")

        if "swap" in results:
            print("\nSWAP: PASSED")
            print(f"  TX Hash: {results['swap']['tx_hash']}")
            print(f"  WETH spent: {results['swap']['weth_spent']}")
            print(f"  USDC received: {results['swap']['usdc_received']}")

        if "lp_open" in results:
            print("\nLP_OPEN: PASSED")
            print(f"  TX Hash: {results['lp_open']['tx_hash']}")
            print(f"  Position ID: {results['lp_open']['position_id']}")
            print(f"  Liquidity: {results['lp_open']['liquidity']}")

        if "lp_close" in results:
            print("\nLP_CLOSE: PASSED")
            print(f"  TX Hashes: {results['lp_close']['tx_hashes']}")
            print(f"  WETH withdrawn: {results['lp_close']['weth_withdrawn']}")
            print(f"  USDC withdrawn: {results['lp_close']['usdc_withdrawn']}")

        print(f"\n{'=' * 60}")
        print("ALL TESTS PASSED!")
        print(f"{'=' * 60}")

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
