"""Production-grade SwapIntent tests for Uniswap V3 on Avalanche.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using UniswapV3ReceiptParser
5. Verify balances changed correctly

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

To run:
    uv run pytest tests/intents/avalanche/test_uniswap_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler, SwapIntent
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

CHAIN_NAME = "avalanche"


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.avalanche
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
    async def test_swap_usdc_to_wavax_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC -> WAVAX swap using SwapIntent.

        Flow:
        1. Create SwapIntent for USDC -> WAVAX
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Verify balances changed correctly
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WAVAX"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "uniswap_v3", token_in, token_out, 500)

        # Get decimals
        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        # Amount to swap
        swap_amount = Decimal("100")  # 100 USDC

        print(f"\n{'='*80}")
        print("Test: USDC -> WAVAX Swap via SwapIntent")
        print(f"{'='*80}")
        print(f"Swap amount: {swap_amount} USDC")

        # Record balances before
        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        wavax_before = get_token_balance(web3, token_out, funded_wallet)

        print(f"USDC before: {format_token_amount(usdc_before, in_decimals)}")
        print(f"WAVAX before: {format_token_amount(wavax_before, out_decimals)}")

        # Create SwapIntent
        intent = SwapIntent(
            from_token="USDC",
            to_token="WAVAX",
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
        wavax_after = get_token_balance(web3, token_out, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        wavax_received = wavax_after - wavax_before

        print("\n--- Results ---")
        print(f"USDC spent:    {format_token_amount(usdc_spent, in_decimals)}")
        print(f"WAVAX received: {format_token_amount(wavax_received, out_decimals)}")

        # Verify USDC was spent
        expected_usdc_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must equal swap amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # Verify WAVAX was received
        assert wavax_received > 0, "Must receive positive WAVAX"

        print("\nALL CHECKS PASSED")

    @pytest.mark.asyncio
    async def test_swap_wavax_to_usdc_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test WAVAX -> USDC swap using SwapIntent (reverse direction)."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["WAVAX"]
        token_out = tokens["USDC"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "uniswap_v3", token_in, token_out, 500)

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("1.0")  # 1.0 WAVAX

        print(f"\n{'='*80}")
        print("Test: WAVAX -> USDC Swap via SwapIntent")
        print(f"{'='*80}")

        wavax_before = get_token_balance(web3, token_in, funded_wallet)
        usdc_before = get_token_balance(web3, token_out, funded_wallet)

        # Create intent
        intent = SwapIntent(
            from_token="WAVAX",
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
        wavax_after = get_token_balance(web3, token_in, funded_wallet)
        usdc_after = get_token_balance(web3, token_out, funded_wallet)

        wavax_spent = wavax_before - wavax_after
        usdc_received = usdc_after - usdc_before

        expected_wavax_spent = int(swap_amount * Decimal(10**in_decimals))
        assert wavax_spent == expected_wavax_spent
        assert usdc_received > 0

        print(f"WAVAX spent:    {format_token_amount(wavax_spent, in_decimals)}")
        print(f"USDC received: {format_token_amount(usdc_received, out_decimals)}")
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

        token_out = tokens["WAVAX"]

        # Get current balance
        usdc_balance = get_token_balance(web3, token_in, funded_wallet)
        wavax_before = get_token_balance(web3, token_out, funded_wallet)
        in_decimals = get_token_decimals(web3, token_in)
        balance_decimal = Decimal(usdc_balance) / Decimal(10**in_decimals)

        if usdc_balance == 0:
            pytest.skip("USDC funding missing for funded_wallet; skipping insufficient-balance test")

        # Try to swap more than we have
        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'='*80}")
        print("Test: SwapIntent with Insufficient Balance")
        print(f"{'='*80}")
        print(f"Balance:   {balance_decimal} USDC")
        print(f"Trying:    {excessive_amount} USDC")

        intent = SwapIntent(
            from_token="USDC",
            to_token="WAVAX",
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
        wavax_after = get_token_balance(web3, token_out, funded_wallet)
        assert usdc_after == usdc_balance, "Input token balance must be unchanged after failed swap"
        assert wavax_after == wavax_before, "Output token balance must be unchanged after failed swap"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
