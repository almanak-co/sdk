"""Production-grade SwapIntent tests for Enso DEX aggregator on Base.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler (direct Enso API)
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using EnsoReceiptParser
5. Verify balances changed correctly

NO MOCKING. All tests execute real on-chain swaps via Enso routing on Base.

This is the first Enso intent test on Base (3rd chain after Arbitrum and Sonic).

To run:
    uv run pytest tests/intents/base/test_enso_swap.py -v -s
"""

import os
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

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "base"

# Enso API requires an API key -- skip gracefully when not available
pytestmark = [
    pytest.mark.no_zodiac(reason="Aggregator routes non-deterministically; plan excludes from Zodiac coverage"),
    pytest.mark.skipif(not os.environ.get('ENSO_API_KEY'), reason='ENSO_API_KEY not set -- Enso intent tests require API access'),
]


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.base
@pytest.mark.swap
class TestEnsoSwapIntent:
    """Test Enso aggregator swaps using SwapIntent on Base.

    These tests verify the full Intent flow:
    - SwapIntent creation with protocol="enso"
    - IntentCompiler generates correct Enso routed transactions
    - Transactions execute successfully on-chain
    - EnsoReceiptParser correctly interprets results
    - Balance changes match expected amounts
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        strict=False,
        reason="VIB-4307: Enso aggregator route flake on Anvil base fork — Enso's quote sometimes routes through a sub-pool whose state on the fork differs from mainnet at quote time, causing execution revert. Needs 10/10 run validation per intent-tests rule #12 (as of 2026-05-13)",
    )
    async def test_swap_usdc_to_weth_via_enso(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test BUY direction: USDC -> WETH swap via Enso on Base.

        Flow:
        1. Create SwapIntent for USDC -> WETH with protocol="enso"
        2. Compile to ActionBundle (Enso finds optimal route)
        3. Execute via ExecutionOrchestrator
        4. Parse receipt with EnsoReceiptParser
        5. Verify balance deltas
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("100")  # 100 USDC

        print(f"\n{'='*80}")
        print("Test: USDC -> WETH Swap via Enso on Base (BUY direction)")
        print(f"{'='*80}")
        print(f"Swap amount: {swap_amount} USDC")

        # L4: Record balances before
        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)

        print(f"USDC before: {format_token_amount(usdc_before, in_decimals)}")
        print(f"WETH before: {format_token_amount(weth_before, out_decimals)}")

        # Fail fast if funded_wallet did not seed the input token
        expected_usdc_raw = int(swap_amount * Decimal(10**in_decimals))
        assert usdc_before >= expected_usdc_raw, (
            f"Insufficient USDC balance for test: have {format_token_amount(usdc_before, in_decimals)}, "
            f"need {swap_amount} — check Anvil wallet funding"
        )

        # L1: Create SwapIntent with Enso protocol
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=swap_amount,
            max_slippage=Decimal("0.02"),  # 2% slippage for aggregator routing
            protocol="enso",
            chain=CHAIN_NAME,
        )

        print(f"\nCreated SwapIntent: {intent.from_token} -> {intent.to_token}, amount={intent.amount}, protocol=enso")

        # L1: Compile intent
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )

        print("Compiling intent to ActionBundle via Enso routing...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # L2: Execute via ExecutionOrchestrator
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # L3: Parse receipts with EnsoReceiptParser
        l3_verified = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                from almanak.connectors.enso.receipt_parser import EnsoReceiptParser

                parser = EnsoReceiptParser(chain=CHAIN_NAME)
                swap_result = parser.parse_swap_receipt(
                    receipt=tx_result.receipt.to_dict(),
                    wallet_address=funded_wallet,
                    token_out=token_out,
                    token_in=token_in,
                )

                if swap_result.success and swap_result.amount_out > 0:
                    print(f"  Amount in:  {swap_result.amount_in}")
                    print(f"  Amount out: {swap_result.amount_out}")

                # Also test extract_swap_amounts (used by ResultEnricher)
                swap_amounts = parser.extract_swap_amounts(tx_result.receipt.to_dict())
                if swap_amounts:
                    print(f"  SwapAmounts.amount_in_decimal:  {swap_amounts.amount_in_decimal}")
                    print(f"  SwapAmounts.amount_out_decimal: {swap_amounts.amount_out_decimal}")
                    print(f"  SwapAmounts.effective_price:    {swap_amounts.effective_price}")

                    assert_swap_semantic_match(
                        intent_amount=swap_amount,
                        intent_from_token="USDC",
                        intent_to_token="WETH",
                        swap_result=swap_amounts,
                        chain=CHAIN_NAME,
                    )
                    l3_verified = True
                    print("  L3 semantic check: PASSED")

        assert l3_verified, "Must verify at least one swap receipt via EnsoReceiptParser"

        # L4: Verify balance deltas
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        weth_received = weth_after - weth_before

        print("\n--- Balance Deltas ---")
        print(f"USDC spent:    {format_token_amount(usdc_spent, in_decimals)}")
        print(f"WETH received: {format_token_amount(weth_received, out_decimals)}")

        # Verify USDC was spent (exact amount)
        expected_usdc_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must equal swap amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # Verify WETH was received
        assert weth_received > 0, "Must receive positive WETH from Enso swap"

        print("\nALL CHECKS PASSED - BUY (USDC -> WETH) via Enso on Base")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        strict=False,
        reason="VIB-4307: Enso WETH->USDC route consistently reverts on Anvil base fork (4/4 in local validation 2026-05-13) — reverse-direction quote appears broken; pending Enso-side investigation. Needs 10/10 run validation per intent-tests rule #12 (as of 2026-05-13)",
    )
    async def test_swap_weth_to_usdc_via_enso(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test SELL direction: WETH -> USDC swap via Enso on Base.

        This validates the SELL direction which failed on Arbitrum in iter 36.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["WETH"]
        token_out = tokens["USDC"]

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("0.05")  # 0.05 WETH

        print(f"\n{'='*80}")
        print("Test: WETH -> USDC Swap via Enso on Base (SELL direction)")
        print(f"{'='*80}")
        print(f"Swap amount: {swap_amount} WETH")

        # L4: Record balances before
        weth_before = get_token_balance(web3, token_in, funded_wallet)
        usdc_before = get_token_balance(web3, token_out, funded_wallet)

        print(f"WETH before: {format_token_amount(weth_before, in_decimals)}")
        print(f"USDC before: {format_token_amount(usdc_before, out_decimals)}")

        # Fail fast if funded_wallet did not seed the input token
        expected_weth_raw = int(swap_amount * Decimal(10**in_decimals))
        assert weth_before >= expected_weth_raw, (
            f"Insufficient WETH balance for test: have {format_token_amount(weth_before, in_decimals)}, "
            f"need {swap_amount} — check Anvil wallet funding"
        )

        # L1: Create SwapIntent with Enso protocol (SELL direction)
        intent = SwapIntent(
            from_token="WETH",
            to_token="USDC",
            amount=swap_amount,
            max_slippage=Decimal("0.02"),  # 2% slippage
            protocol="enso",
            chain=CHAIN_NAME,
        )

        # L1: Compile
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

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # L2: Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # L3: Parse receipts
        l3_verified = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")

            if tx_result.receipt:
                from almanak.connectors.enso.receipt_parser import EnsoReceiptParser

                parser = EnsoReceiptParser(chain=CHAIN_NAME)
                swap_result = parser.parse_swap_receipt(
                    receipt=tx_result.receipt.to_dict(),
                    wallet_address=funded_wallet,
                    token_out=token_out,
                    token_in=token_in,
                )

                if swap_result.success and swap_result.amount_out > 0:
                    print(f"  Amount in:  {swap_result.amount_in}")
                    print(f"  Amount out: {swap_result.amount_out}")

                swap_amounts = parser.extract_swap_amounts(tx_result.receipt.to_dict())
                if swap_amounts:
                    print(f"  SwapAmounts.amount_out_decimal: {swap_amounts.amount_out_decimal}")
                    print(f"  SwapAmounts.effective_price:    {swap_amounts.effective_price}")

                    assert_swap_semantic_match(
                        intent_amount=swap_amount,
                        intent_from_token="WETH",
                        intent_to_token="USDC",
                        swap_result=swap_amounts,
                        chain=CHAIN_NAME,
                    )
                    l3_verified = True
                    print("  L3 semantic check: PASSED")

        assert l3_verified, "Must verify at least one swap receipt via EnsoReceiptParser"

        # L4: Verify balance deltas
        weth_after = get_token_balance(web3, token_in, funded_wallet)
        usdc_after = get_token_balance(web3, token_out, funded_wallet)

        weth_spent = weth_before - weth_after
        usdc_received = usdc_after - usdc_before

        print("\n--- Balance Deltas ---")
        print(f"WETH spent:    {format_token_amount(weth_spent, in_decimals)}")
        print(f"USDC received: {format_token_amount(usdc_received, out_decimals)}")

        # Verify WETH was spent (exact amount)
        expected_weth_spent = int(swap_amount * Decimal(10**in_decimals))
        assert weth_spent == expected_weth_spent, (
            f"WETH spent must equal swap amount. "
            f"Expected: {expected_weth_spent}, Got: {weth_spent}"
        )

        # Verify USDC was received
        assert usdc_received > 0, "Must receive positive USDC from Enso swap"

        print("\nALL CHECKS PASSED - SELL (WETH -> USDC) via Enso on Base")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_enso_swap_insufficient_balance_fails(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test that Enso SwapIntent with insufficient balance fails gracefully.

        Failure-mode test: 3 layers required (compilation, execution, balance conservation).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]

        # Get current balance
        usdc_balance = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)
        in_decimals = get_token_decimals(web3, token_in)
        balance_decimal = Decimal(usdc_balance) / Decimal(10**in_decimals)

        # Try to swap more than we have
        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'='*80}")
        print("Test: Enso SwapIntent with Insufficient Balance")
        print(f"{'='*80}")
        print(f"Balance:   {balance_decimal} USDC")
        print(f"Trying:    {excessive_amount} USDC")

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=excessive_amount,
            max_slippage=Decimal("0.01"),
            protocol="enso",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)

        # Enso may reject at compile time (API rejects) or execution time (on-chain revert).
        # Either layer rejecting is valid -- but at least one MUST reject.
        failed_at_compilation = compilation_result.status.value != "SUCCESS" or compilation_result.action_bundle is None
        failed_at_execution = False

        if not failed_at_compilation:
            execution_result = await orchestrator.execute(compilation_result.action_bundle)
            failed_at_execution = not execution_result.success
            assert not execution_result.success, "Execution should fail with insufficient balance"
            print(f"Execution failed as expected: {execution_result.error}")
        else:
            print(f"Compilation failed as expected: {compilation_result.error}")

        assert failed_at_compilation or failed_at_execution, (
            "Excessive swap must be rejected at compilation or execution layer"
        )

        # Verify balances unchanged (conservation check)
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)
        assert usdc_after == usdc_balance, "Input token balance must be unchanged after failed swap"
        assert weth_after == weth_before, "Output token balance must be unchanged after failed swap"

        print("\nALL CHECKS PASSED - Balance conservation verified")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
