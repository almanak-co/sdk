"""Production-grade SwapIntent tests for Uniswap V3 on Polygon.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using UniswapV3ReceiptParser
5. Verify balances changed correctly

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

To run:
    uv run pytest tests/intents/polygon/test_uniswap_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    SWAP_MAX_SLIPPAGE,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)
from tests.intents.pool_helpers import fail_if_v3_pool_missing

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "polygon"


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.polygon
@pytest.mark.swap
class TestUniswapV3SwapIntent:
    """Test Uniswap V3 swaps using SwapIntent.

    These tests verify the full Intent flow:
    - SwapIntent creation with proper parameters
    - IntentCompiler generates correct Uniswap V3 transactions
    - Transactions execute successfully on-chain
    - UniswapV3ReceiptParser correctly interprets results
    - Balance changes match expected amounts
    """

    @pytest.mark.asyncio
    async def test_swap_usdc_to_weth_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC -> WETH swap using SwapIntent.

        Flow:
        1. Create SwapIntent for USDC -> WETH
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Verify balances changed correctly
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "uniswap_v3", token_in, token_out, 500)

        # Get decimals
        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        # Amount to swap
        swap_amount = Decimal("100")  # 100 USDC

        print(f"\n{'='*80}")
        print("Test: USDC -> WETH Swap via SwapIntent")
        print(f"{'='*80}")
        print(f"Swap amount: {swap_amount} USDC")

        # Record balances before
        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)

        print(f"USDC before: {format_token_amount(usdc_before, in_decimals)}")
        print(f"WETH before: {format_token_amount(weth_before, out_decimals)}")

        # Create SwapIntent
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )

        print(f"\nCreated SwapIntent: {intent.from_token} -> {intent.to_token}, amount={intent.amount}")

        # Compile intent with real prices from CoinGecko
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        print("Compiling intent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # Execute via ExecutionOrchestrator
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Parse receipts
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            # Parse swap receipt
            if tx_result.receipt:
                from almanak.framework.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser

                parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.swap_result:
                    print(f"  Amount in:  {parse_result.swap_result.amount_in_decimal}")
                    print(f"  Amount out: {parse_result.swap_result.amount_out_decimal}")
                    print(f"  Price:      {parse_result.swap_result.effective_price}")

        # Verify balance changes
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        weth_received = weth_after - weth_before

        print("\n--- Results ---")
        print(f"USDC spent:    {format_token_amount(usdc_spent, in_decimals)}")
        print(f"WETH received: {format_token_amount(weth_received, out_decimals)}")

        # Verify USDC was spent
        expected_usdc_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must equal swap amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # Verify WETH was received
        assert weth_received > 0, "Must receive positive WETH"

        print("\nALL CHECKS PASSED")

    @pytest.mark.asyncio
    async def test_swap_weth_to_usdc_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test WETH -> USDC swap using SwapIntent (reverse direction)."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["WETH"]
        token_out = tokens["USDC"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "uniswap_v3", token_in, token_out, 500)

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("0.05")  # 0.05 WETH

        print(f"\n{'='*80}")
        print("Test: WETH -> USDC Swap via SwapIntent")
        print(f"{'='*80}")

        weth_before = get_token_balance(web3, token_in, funded_wallet)
        usdc_before = get_token_balance(web3, token_out, funded_wallet)

        # Create intent
        intent = SwapIntent(
            from_token="WETH",
            to_token="USDC",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )

        # Compile with real prices from CoinGecko
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        # Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success

        # Verify
        weth_after = get_token_balance(web3, token_in, funded_wallet)
        usdc_after = get_token_balance(web3, token_out, funded_wallet)

        weth_spent = weth_before - weth_after
        usdc_received = usdc_after - usdc_before

        expected_weth_spent = int(swap_amount * Decimal(10**in_decimals))
        assert weth_spent == expected_weth_spent
        assert usdc_received > 0

        print(f"WETH spent:    {format_token_amount(weth_spent, in_decimals)}")
        print(f"USDC received: {format_token_amount(usdc_received, out_decimals)}")
        print("\nALL CHECKS PASSED")

    @pytest.mark.asyncio
    async def test_swap_native_matic_to_usdc_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test native MATIC -> USDC swap via SwapIntent (VIB-3135).

        Regression guard for the native-in allowance bug: the compiler must
        emit a single value-bearing swap tx (no ERC20 approve) when the
        input is the chain's native gas token, even when the registry uses
        a chain-specific precompile address (Polygon's native at
        ``0x0000000000000000000000000000000000001010``) rather than the
        shared sentinel.

        4-layer verification:
          1. Compilation -> SUCCESS, no approve tx, exactly one swap tx
          2. Execution   -> on-chain success
          3. Receipt     -> Uniswap V3 Swap event parsed with positive amounts
          4. Balance     -> native MATIC decreases by ~swap_amount + gas;
                            USDC increases by a positive amount
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_out = tokens["USDC"]
        wmatic_address = "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270"  # WMATIC on Polygon
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "uniswap_v3", wmatic_address, token_out, 500)

        out_decimals = get_token_decimals(web3, token_out)

        # Use a small native amount to keep gas-buffer math comfortable
        # (test wallet was funded with 100 MATIC).
        swap_amount = Decimal("1")  # 1 MATIC

        print(f"\n{'='*80}")
        print("Test: native MATIC -> USDC Swap via SwapIntent (VIB-3135)")
        print(f"{'='*80}")
        print(f"Swap amount: {swap_amount} MATIC")

        # Record balances before
        matic_before = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdc_before = get_token_balance(web3, token_out, funded_wallet)

        print(f"MATIC before: {format_token_amount(matic_before, 18)}")
        print(f"USDC  before: {format_token_amount(usdc_before, out_decimals)}")
        assert matic_before >= int(swap_amount * Decimal(10**18)), (
            "Test wallet must have at least swap_amount native MATIC"
        )

        # Create SwapIntent with native MATIC as input
        intent = SwapIntent(
            from_token="MATIC",
            to_token="USDC",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )

        # The shared polygon price oracle covers ERC20 tokens only (USDC,
        # WETH, USDT, WBTC). Inject native MATIC/POL so slippage protection
        # can compute. We fetch the live MATIC price from CoinGecko so the
        # compiler's price-impact guard doesn't reject the swap when the
        # forked pool quote diverges from a hardcoded estimate.
        try:
            import urllib.request
            import json as _json

            req = urllib.request.Request(
                "https://api.coingecko.com/api/v3/simple/price?ids=matic-network&vs_currencies=usd",
                headers={"User-Agent": "almanak-sdk-tests"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:  # noqa: S310
                data = _json.loads(resp.read().decode())
            matic_price = Decimal(str(data["matic-network"]["usd"]))
        except Exception:
            # Fallback: a low-but-realistic MATIC price keeps the price-
            # impact guard satisfied even if CoinGecko is unreachable.
            matic_price = Decimal("0.10")

        prices_with_native = {
            **price_oracle,
            "MATIC": matic_price,
            "POL": matic_price,
            "WMATIC": matic_price,
        }
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=prices_with_native,
        )

        # ---- Layer 1: Compilation ------------------------------------------
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        txs = compilation_result.transactions or []
        approve_txs = [t for t in txs if t.tx_type.startswith("approve")]
        swap_txs = [t for t in txs if t.tx_type == "swap"]
        assert approve_txs == [], (
            "Native-in swap MUST NOT emit ERC20 approve/allowance txs against "
            "the Polygon native precompile address — this is the VIB-3135 bug."
        )
        assert len(swap_txs) == 1, "Native-in swap must emit exactly one swap tx"
        assert swap_txs[0].value == int(swap_amount * Decimal(10**18)), (
            "Swap tx must carry the native amount as msg.value"
        )

        # ---- Layer 2: Execution --------------------------------------------
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"
        assert len(execution_result.transaction_results) == 1, (
            "Native-in swap should execute exactly one tx (no approve)"
        )

        # ---- Layer 3: Receipt parsing --------------------------------------
        tx_result = execution_result.transaction_results[0]
        assert tx_result.receipt is not None, "Receipt must be present after execution"

        from almanak.framework.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser

        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
        assert parse_result.success, f"Receipt parse failed: {parse_result.error}"
        assert parse_result.swap_result is not None, "Swap event must be parsed"
        assert parse_result.swap_result.amount_in > 0
        assert parse_result.swap_result.amount_out > 0

        # ---- Layer 4: Balance deltas ---------------------------------------
        matic_after = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdc_after = get_token_balance(web3, token_out, funded_wallet)

        matic_spent = matic_before - matic_after
        usdc_received = usdc_after - usdc_before
        swap_amount_wei = int(swap_amount * Decimal(10**18))

        # Compute exact gas fee from the framework receipt. The
        # TransactionReceipt dataclass exposes both ``gas_used`` and
        # ``effective_gas_price`` — multiplied together they give the
        # actual gas paid, which lets us turn the prior range-based
        # tolerance into strict equality.
        gas_fee = int(tx_result.receipt.gas_used) * int(tx_result.receipt.effective_gas_price)

        print(f"MATIC spent:    {format_token_amount(matic_spent, 18)}  (incl. gas)")
        print(f"Gas fee:        {format_token_amount(gas_fee, 18)} MATIC")
        print(f"USDC  received: {format_token_amount(usdc_received, out_decimals)}")

        # Exact balance conservation: native spent == swap value + gas fee.
        # The receipt-derived gas figure removes the previous range-based
        # tolerance and turns this into a strict 4-layer-verification check.
        assert matic_spent == swap_amount_wei + gas_fee, (
            f"MATIC delta mismatch. "
            f"Expected {swap_amount_wei + gas_fee} (swap {swap_amount_wei} + gas {gas_fee}), "
            f"got {matic_spent}."
        )
        # Receiver must get exactly what the parsed Swap event reports.
        assert usdc_received == parse_result.swap_result.amount_out, (
            f"USDC delta must equal parsed amount_out. "
            f"Expected {parse_result.swap_result.amount_out}, got {usdc_received}."
        )

        print("\nALL CHECKS PASSED")

    @pytest.mark.xfail(reason="flaky: needs more investigation", strict=False)
    @pytest.mark.asyncio
    async def test_swap_intent_with_insufficient_balance_fails(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test that SwapIntent with insufficient balance fails gracefully."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]

        # Get current balance
        usdc_balance = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)
        assert usdc_balance > 0, "funded_wallet failed to fund USDC (balance is 0)"
        in_decimals = get_token_decimals(web3, token_in)
        balance_decimal = Decimal(usdc_balance) / Decimal(10**in_decimals)

        # Try to swap more than we have
        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'='*80}")
        print("Test: SwapIntent with Insufficient Balance")
        print(f"{'='*80}")
        print(f"Balance:   {balance_decimal} USDC")
        print(f"Trying:    {excessive_amount} USDC")

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=excessive_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None

        # Try to execute - should fail
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert not execution_result.success, "Execution should fail with insufficient balance"
        print(f"Execution failed as expected: {execution_result.error}")

        # Verify balances unchanged (bilateral conservation check)
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)
        assert usdc_after == usdc_balance, "Input token balance must be unchanged after failed swap"
        assert weth_after == weth_before, "Output token balance must be unchanged after failed swap"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
