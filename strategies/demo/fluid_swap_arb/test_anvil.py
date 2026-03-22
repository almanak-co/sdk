"""End-to-end Fluid DEX swap test on Anvil fork.

Tests the full pipeline: Intent -> Compile -> Execute -> Parse -> Verify
on a local Anvil fork of Arbitrum. No gateway required.

Usage:
    # Start anvil fork first:
    anvil --fork-url https://arbitrum-one-rpc.publicnode.com --port 8555 --chain-id 42161

    # Then run:
    cd strategies/demo/fluid_swap_arb
    uv run python test_anvil.py
"""

import sys
from decimal import Decimal
from pathlib import Path

from eth_account import Account
from web3 import Web3

# Add repo root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

# =============================================================================
# Config
# =============================================================================

ANVIL_RPC = "http://127.0.0.1:8555"
TEST_KEY = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
CHAIN = "arbitrum"
USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
USDT = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"
USDC_USDT_POOL = "0x3C0441B42195F4aD6aa9a0978E06096ea616CDa7"


def main():
    w3 = Web3(Web3.HTTPProvider(ANVIL_RPC))
    acct = Account.from_key(TEST_KEY)

    if not w3.is_connected():
        print("ERROR: Anvil not running. Start with:")
        print("  anvil --fork-url https://arbitrum-one-rpc.publicnode.com --port 8555 --chain-id 42161")
        return 1

    print(f"Connected to Anvil (chain_id={w3.eth.chain_id})")
    print(f"Wallet: {acct.address}")

    # -- Fund wallet --
    w3.provider.make_request("anvil_setBalance", [acct.address, hex(100 * 10**18)])
    key = Web3.keccak(bytes.fromhex(acct.address[2:].lower().zfill(64) + "{:064x}".format(9)))
    w3.provider.make_request("anvil_setStorageAt", [USDC, "0x" + key.hex(), "0x" + "{:064x}".format(10_000 * 10**6)])

    bal_abi = [{"inputs": [{"name": "a", "type": "address"}], "name": "balanceOf", "outputs": [{"type": "uint256"}], "type": "function", "stateMutability": "view"}]
    usdc_before = w3.eth.contract(address=Web3.to_checksum_address(USDC), abi=bal_abi).functions.balanceOf(acct.address).call()
    usdt_before = w3.eth.contract(address=Web3.to_checksum_address(USDT), abi=bal_abi).functions.balanceOf(acct.address).call()
    print(f"\n--- BEFORE ---")
    print(f"  USDC: {usdc_before / 1e6:.2f}")
    print(f"  USDT: {usdt_before / 1e6:.2f}")

    # -- Step 1: Compile SwapIntent --
    print(f"\n--- STEP 1: Compile ---")
    from almanak.framework.intents import IntentCompiler, SwapIntent
    from almanak.framework.intents.compiler import IntentCompilerConfig

    compiler = IntentCompiler(
        chain=CHAIN,
        wallet_address=acct.address,
        rpc_url=ANVIL_RPC,
        price_oracle={"USDC": Decimal("1.0"), "USDT": Decimal("1.0"), "ETH": Decimal("3500")},
        config=IntentCompilerConfig(allow_placeholder_prices=True),
    )

    intent = SwapIntent(
        from_token="USDC",
        to_token="USDT",
        amount=Decimal("100"),
        max_slippage=Decimal("0.005"),
        protocol="fluid",
    )

    result = compiler.compile(intent)
    print(f"  Status: {result.status.value}")
    if result.status.value != "SUCCESS":
        print(f"  Error: {result.error}")
        return 1
    print(f"  Transactions: {len(result.transactions)}")
    print(f"  Gas estimate: {result.total_gas_estimate}")

    # -- Step 2: Execute on Anvil --
    print(f"\n--- STEP 2: Execute ---")
    for i, tx_data in enumerate(result.transactions):
        nonce = w3.eth.get_transaction_count(acct.address)
        tx = {
            "to": Web3.to_checksum_address(tx_data.to),
            "data": tx_data.data,
            "value": tx_data.value,
            "gas": max(tx_data.gas_estimate, 500_000),
            "gasPrice": w3.eth.gas_price,
            "nonce": nonce,
            "chainId": w3.eth.chain_id,
        }
        signed = acct.sign_transaction(tx)
        raw = getattr(signed, "rawTransaction", None) or getattr(signed, "raw_transaction", None)
        tx_hash = w3.eth.send_raw_transaction(raw)
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash)
        status_str = "OK" if receipt["status"] == 1 else "REVERTED"
        print(f"  tx[{i}] ({tx_data.tx_type}): {status_str}, gas={receipt['gasUsed']}")

        if receipt["status"] != 1:
            print(f"  Transaction {i} REVERTED!")
            return 1

    # -- Step 3: Parse receipt --
    print(f"\n--- STEP 3: Parse receipt ---")
    from almanak.framework.connectors.fluid.receipt_parser import FluidReceiptParser

    parser = FluidReceiptParser()
    last_receipt = dict(receipt)
    parse_result = parser.parse_receipt(last_receipt)
    print(f"  Parsed: success={parse_result.success}")
    if parse_result.swap_events:
        swap = parse_result.swap_events[0]
        print(f"  Swap: {'token0->token1' if swap.swap0to1 else 'token1->token0'}")
        print(f"  Amount in: {swap.amount_in}")
        print(f"  Amount out: {swap.amount_out}")

    swap_amounts = parser.extract_swap_amounts(last_receipt)
    if swap_amounts:
        print(f"  SwapAmounts: in={swap_amounts.amount_in}, out={swap_amounts.amount_out}, price={swap_amounts.effective_price}")

    # -- Step 4: Verify balance deltas --
    print(f"\n--- STEP 4: Balance deltas ---")
    usdc_after = w3.eth.contract(address=Web3.to_checksum_address(USDC), abi=bal_abi).functions.balanceOf(acct.address).call()
    usdt_after = w3.eth.contract(address=Web3.to_checksum_address(USDT), abi=bal_abi).functions.balanceOf(acct.address).call()
    print(f"  USDC: {usdc_after / 1e6:.2f} (delta: {(usdc_after - usdc_before) / 1e6:.2f})")
    print(f"  USDT: {usdt_after / 1e6:.2f} (delta: {(usdt_after - usdt_before) / 1e6:.2f})")

    usdc_spent = usdc_before - usdc_after
    usdt_received = usdt_after - usdt_before

    assert usdc_spent > 0, f"Expected USDC to decrease, got delta={usdc_spent}"
    assert usdt_received > 0, f"Expected USDT to increase, got delta={usdt_received}"
    assert abs(usdc_spent - 100 * 10**6) < 10**6, f"Expected ~100 USDC spent, got {usdc_spent / 1e6:.2f}"

    print(f"\n=== ALL CHECKS PASSED ===")
    print(f"  Swapped {usdc_spent / 1e6:.2f} USDC -> {usdt_received / 1e6:.4f} USDT on Fluid DEX")
    return 0


if __name__ == "__main__":
    sys.exit(main())
