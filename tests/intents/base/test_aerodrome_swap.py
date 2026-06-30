"""Production-grade SwapIntent tests for Aerodrome on Base.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using AerodromeReceiptParser
5. Verify balances changed correctly

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

To run:
    uv run pytest tests/intents/base/test_aerodrome_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    assert_swap_semantic_match,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)
from tests.intents.pool_helpers import fail_if_aerodrome_cl_pool_missing

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "base"


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.base
@pytest.mark.swap
class TestAerodromeSwapIntent:
    """Test Aerodrome swaps using SwapIntent.

    These tests verify the full Intent flow:
    - SwapIntent creation with proper parameters
    - IntentCompiler generates correct Aerodrome transactions
    - Transactions execute successfully on-chain
    - AerodromeReceiptParser correctly interprets results
    - Balance changes match expected amounts
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_usdc_to_weth_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
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

        # Validate pool exists before running test
        fail_if_aerodrome_cl_pool_missing(web3, CHAIN_NAME, token_in, token_out, 100)

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
            max_slippage=Decimal("0.20"),  # 20% slippage for oracle-based quoting
            protocol="aerodrome",
            chain=CHAIN_NAME,
        )

        print(f"\nCreated SwapIntent: {intent.from_token} -> {intent.to_token}, amount={intent.amount}")

        # Compile intent with real prices from CoinGecko
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
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

        # Layer 3: parse receipts — enforce that at least one swap event was
        # successfully parsed with positive amounts. The for-loop walks every tx
        # in the bundle (approve + swap); only the swap tx carries a Swap event,
        # so we require >= 1 parsed swap result across the bundle rather than per
        # tx. A conditional parse that silently passes on zero parsed events
        # would leave the 4-layer receipt verification uncovered.
        swap_results_parsed = 0
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt is None:
                continue

            from almanak.connectors.aerodrome.receipt_parser import AerodromeReceiptParser

            # Pass token0/token1 (sorted ascending by address) so the parser can
            # resolve decimals and build a high-level swap_result for the CL
            # (SwapCL) event — base Aerodrome routes USDC/WETH through a
            # Slipstream CL pool, whose Swap event carries signed amount0/amount1
            # that need the token mapping to decode. Mirrors production wiring
            # and the optimism test.
            token0_addr, token1_addr = sorted([token_in.lower(), token_out.lower()])
            parser = AerodromeReceiptParser(
                chain=CHAIN_NAME,
                token0_address=token0_addr,
                token1_address=token1_addr,
            )
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

            if parse_result.success and parse_result.swap_result:
                swap_results_parsed += 1
                print(f"  Amount in:  {parse_result.swap_result.amount_in_decimal}")
                print(f"  Amount out: {parse_result.swap_result.amount_out_decimal}")
                print(f"  Price:      {parse_result.swap_result.effective_price}")

                assert parse_result.swap_result.amount_in_decimal > 0, (
                    "Receipt parser: amount_in_decimal must be positive"
                )
                assert parse_result.swap_result.amount_out_decimal > 0, (
                    "Receipt parser: amount_out_decimal must be positive"
                )
                assert parse_result.swap_result.effective_price > 0, (
                    "Receipt parser: effective_price must be positive"
                )

                # L3 semantic verification
                assert_swap_semantic_match(
                    intent_amount=swap_amount,
                    intent_from_token="USDC",
                    intent_to_token="WETH",
                    swap_result=parse_result.swap_result,
                    chain=CHAIN_NAME,
                )
                print("  L3 semantic check: PASSED")

        assert swap_results_parsed >= 1, (
            "Layer 3 (receipt parsing) must parse at least one swap event across "
            f"the executed bundle. Got {swap_results_parsed} parsed swap results."
        )

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

        print("\nALL CHECKS PASSED ✓")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_weth_to_usdc_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test WETH -> USDC swap using SwapIntent (reverse direction)."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["WETH"]
        token_out = tokens["USDC"]

        # Validate pool exists before running test
        fail_if_aerodrome_cl_pool_missing(web3, CHAIN_NAME, token_in, token_out, 100)

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
            max_slippage=Decimal("0.20"),  # 20% slippage for oracle-based quoting (VIB-2297)
            protocol="aerodrome",
            chain=CHAIN_NAME,
        )

        # Compile with real prices from CoinGecko
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
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
        print("\nALL CHECKS PASSED ✓")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_intent_with_insufficient_balance_fails(
        self,
        web3: Web3,
        anvil_rpc_url: str,
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
        in_decimals = get_token_decimals(web3, token_in)
        balance_decimal = Decimal(usdc_balance) / Decimal(10**in_decimals)

        # Try to swap more than we have.
        excessive_amount = balance_decimal * Decimal("2")

        print(f"\n{'='*80}")
        print("Test: SwapIntent with Insufficient Balance")
        print(f"{'='*80}")
        print(f"Balance:   {balance_decimal} USDC")
        print(f"Trying:    {excessive_amount} USDC")

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=excessive_amount,
            max_slippage=Decimal("0.01"),
            # This test exercises execution-level balance failure, not the
            # ALM-2890 price-impact guard. An oversized swap (here, larger than
            # the funded balance) would otherwise be rejected at compile time by
            # the guard; allow any impact (1 = 100%) so compilation succeeds and
            # the swap fails at execution on insufficient balance instead.
            max_price_impact=Decimal("1"),
            protocol="aerodrome",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
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

        print("\nALL CHECKS PASSED ✓")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
