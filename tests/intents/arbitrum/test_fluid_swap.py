"""Production-grade SwapIntent tests for Fluid DEX on Arbitrum.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using FluidReceiptParser
5. Verify balances changed correctly

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

To run:
    uv run pytest tests/intents/arbitrum/test_fluid_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.fluid.receipt_parser import FluidReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "arbitrum"


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.swap
class TestFluidSwapIntent:
    """Test Fluid DEX swaps using SwapIntent.

    These tests verify the full Intent flow:
    - SwapIntent creation with proper parameters
    - IntentCompiler generates correct Fluid transactions
    - Transactions execute successfully on-chain
    - FluidReceiptParser correctly interprets results
    - Balance changes match expected amounts

    Uses USDC/USDT pair (Pool 2) — known working pair on Fluid DEX Arbitrum.
    """

    @pytest.mark.xfail(reason="Fluid DEX pool liquidity is block-dependent on Anvil forks", strict=False)
    @pytest.mark.asyncio
    async def test_swap_usdc_to_usdt_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC -> USDT swap using SwapIntent on Fluid DEX.

        4-layer verification:
        1. Compilation: SwapIntent -> ActionBundle (SUCCESS)
        2. Execution: ActionBundle -> on-chain transactions (success)
        3. Receipt Parsing: FluidReceiptParser extracts swap amounts
        4. Balance Deltas: USDC decreased, USDT increased
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["USDT"]

        # Get decimals
        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        # Small amount to stay within Fluid pool limits
        swap_amount = Decimal("1")  # 1 USDC

        print(f"\n{'='*80}")
        print("Test: USDC -> USDT Swap via SwapIntent (Fluid DEX)")
        print(f"{'='*80}")
        print(f"Swap amount: {swap_amount} USDC")

        # Layer 4 (before): Record balances
        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        usdt_before = get_token_balance(web3, token_out, funded_wallet)

        # Precondition: wallet must be funded
        expected_usdc_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdc_before >= expected_usdc_spent, (
            f"funded_wallet must hold at least {expected_usdc_spent} USDC base units before the swap"
        )

        print(f"USDC before: {format_token_amount(usdc_before, in_decimals)}")
        print(f"USDT before: {format_token_amount(usdt_before, out_decimals)}")

        # Layer 1: Compilation
        intent = SwapIntent(
            from_token="USDC",
            to_token="USDT",
            amount=swap_amount,
            max_slippage=Decimal("0.05"),  # 5% slippage for stablecoin pair
            protocol="fluid",
            chain=CHAIN_NAME,
        )

        print(f"\nCreated SwapIntent: {intent.from_token} -> {intent.to_token}, amount={intent.amount}")

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

        # Layer 2: Execution
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: Receipt Parsing
        parsed_receipt_found = False
        swap_amounts_found = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parser = FluidReceiptParser(chain=CHAIN_NAME)
                receipt_dict = tx_result.receipt.to_dict()

                # Verify parse_receipt() succeeds
                parse_result = parser.parse_receipt(receipt_dict)
                if parse_result.success and parse_result.swap_events:
                    parsed_receipt_found = True
                    print(f"  Parsed {len(parse_result.swap_events)} swap event(s)")

                # Verify extract_swap_amounts() returns valid data
                swap_amounts = parser.extract_swap_amounts(receipt_dict)

                if swap_amounts:
                    swap_amounts_found = True
                    print(f"  Amount in:  {swap_amounts.amount_in_decimal}")
                    print(f"  Amount out: {swap_amounts.amount_out_decimal}")
                    print(f"  Price:      {swap_amounts.effective_price}")

                    # Verify parsed amounts are positive
                    assert swap_amounts.amount_in > 0, "Parsed amount_in must be positive"
                    assert swap_amounts.amount_out > 0, "Parsed amount_out must be positive"
                    assert swap_amounts.effective_price > 0, "Effective price must be positive"

        assert parsed_receipt_found, "Layer 3: At least one receipt must parse swap events via parse_receipt()"
        assert swap_amounts_found, "Layer 3: At least one transaction must contain parseable swap amounts"

        # Layer 4 (after): Verify balance changes
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        usdt_after = get_token_balance(web3, token_out, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        usdt_received = usdt_after - usdt_before

        print("\n--- Results ---")
        print(f"USDC spent:    {format_token_amount(usdc_spent, in_decimals)}")
        print(f"USDT received: {format_token_amount(usdt_received, out_decimals)}")

        # Verify USDC was spent (exact match for the swap amount)
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must equal swap amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # Verify USDT was received (stablecoins, so close to 1:1)
        assert usdt_received > 0, "Must receive positive USDT"

        print("\nALL CHECKS PASSED")

    @pytest.mark.xfail(reason="Fluid DEX pool liquidity is block-dependent on Anvil forks", strict=False)
    @pytest.mark.asyncio
    async def test_swap_usdt_to_usdc_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDT -> USDC swap using SwapIntent (reverse direction).

        4-layer verification:
        1. Compilation: SwapIntent -> ActionBundle (SUCCESS)
        2. Execution: ActionBundle -> on-chain transactions (success)
        3. Receipt Parsing: FluidReceiptParser extracts swap amounts
        4. Balance Deltas: USDT decreased, USDC increased
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDT"]
        token_out = tokens["USDC"]

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("1")  # 1 USDT

        print(f"\n{'='*80}")
        print("Test: USDT -> USDC Swap via SwapIntent (Fluid DEX)")
        print(f"{'='*80}")

        # Layer 4 (before): Record balances
        usdt_before = get_token_balance(web3, token_in, funded_wallet)
        usdc_before = get_token_balance(web3, token_out, funded_wallet)

        # Precondition: wallet must be funded
        expected_usdt_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdt_before >= expected_usdt_spent, (
            f"funded_wallet must hold at least {expected_usdt_spent} USDT base units before the swap"
        )

        print(f"USDT before: {format_token_amount(usdt_before, in_decimals)}")
        print(f"USDC before: {format_token_amount(usdc_before, out_decimals)}")

        # Layer 1: Compilation
        intent = SwapIntent(
            from_token="USDT",
            to_token="USDC",
            amount=swap_amount,
            max_slippage=Decimal("0.05"),
            protocol="fluid",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        # Layer 2: Execution
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: Receipt Parsing
        parsed_receipt_found = False
        swap_amounts_found = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parser = FluidReceiptParser(chain=CHAIN_NAME)
                receipt_dict = tx_result.receipt.to_dict()

                # Verify parse_receipt() succeeds
                parse_result = parser.parse_receipt(receipt_dict)
                if parse_result.success and parse_result.swap_events:
                    parsed_receipt_found = True

                # Verify extract_swap_amounts() returns valid data
                swap_amounts = parser.extract_swap_amounts(receipt_dict)

                if swap_amounts:
                    swap_amounts_found = True
                    assert swap_amounts.amount_in > 0, "Parsed amount_in must be positive"
                    assert swap_amounts.amount_out > 0, "Parsed amount_out must be positive"
                    assert swap_amounts.effective_price > 0, "Effective price must be positive"

        assert parsed_receipt_found, "Layer 3: At least one receipt must parse swap events via parse_receipt()"
        assert swap_amounts_found, "Layer 3: At least one transaction must contain parseable swap amounts"

        # Layer 4 (after): Verify balance changes
        usdt_after = get_token_balance(web3, token_in, funded_wallet)
        usdc_after = get_token_balance(web3, token_out, funded_wallet)

        usdt_spent = usdt_before - usdt_after
        usdc_received = usdc_after - usdc_before

        assert usdt_spent == expected_usdt_spent, (
            f"USDT spent must equal swap amount. "
            f"Expected: {expected_usdt_spent}, Got: {usdt_spent}"
        )
        assert usdc_received > 0, "Must receive positive USDC"

        print(f"USDT spent:    {format_token_amount(usdt_spent, in_decimals)}")
        print(f"USDC received: {format_token_amount(usdc_received, out_decimals)}")
        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
