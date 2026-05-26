"""Production-grade SwapIntent tests for Aerodrome (Velodrome V2) on Optimism.

Aerodrome on Optimism is the Velodrome V2 alias: ``protocol_aliases.py:42-43``
maps ``("optimism", "velodrome") -> "aerodrome"``.  Intents use
``protocol="aerodrome"`` even on Optimism.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using AerodromeReceiptParser
5. Verify balances changed correctly

Only Solidly-fork V2 (volatile/stable) pools exist on Optimism — Slipstream
CL pools are Base-only.  Pool existence is validated via
``fail_if_aerodrome_pool_missing`` (Classic factory).

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

To run:
    uv run pytest tests/intents/optimism/test_aerodrome_swap.py -v -s

VIB-4389
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.aerodrome.receipt_parser import AerodromeReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    SWAP_MAX_SLIPPAGE,
    assert_swap_semantic_match,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)
from tests.intents.pool_helpers import fail_if_aerodrome_pool_missing

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "optimism"


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.optimism
@pytest.mark.swap
class TestAerodromeSwapIntentOnOptimism:
    """Test Aerodrome (Velodrome V2) swaps using SwapIntent on Optimism.

    These tests verify the full Intent flow:
    - SwapIntent creation with proper parameters (protocol="aerodrome")
    - IntentCompiler generates correct Velodrome V2 transactions via the
      aerodrome alias (compiler_constants.py:130, 200)
    - Transactions execute successfully on-chain against Solidly-fork volatile pools
    - AerodromeReceiptParser correctly interprets results
    - Balance changes match expected amounts (bilateral)
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
        """Test USDC -> WETH swap using SwapIntent on Optimism.

        Flow:
        1. Validate volatile USDC/WETH pool exists
        2. Create SwapIntent for USDC -> WETH (protocol="aerodrome")
        3. Compile to ActionBundle using IntentCompiler
        4. Execute via ExecutionOrchestrator
        5. Parse receipts via AerodromeReceiptParser (Layer 3)
        6. Verify bilateral balance deltas (Layer 4)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]

        # Layer 0: validate volatile pool exists before running test
        # Optimism has a USDC/WETH volatile pool on Velodrome V2
        fail_if_aerodrome_pool_missing(web3, CHAIN_NAME, token_in, token_out, stable=False)

        # Get decimals
        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        # Amount to swap
        swap_amount = Decimal("100")  # 100 USDC

        print(f"\n{'='*80}")
        print("Test: USDC -> WETH Swap via SwapIntent (Aerodrome/Velodrome V2 on Optimism)")
        print(f"{'='*80}")
        print(f"Swap amount: {swap_amount} USDC")

        # Layer 4 (pre): record balances BEFORE
        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)

        print(f"USDC before: {format_token_amount(usdc_before, in_decimals)}")
        print(f"WETH before: {format_token_amount(weth_before, out_decimals)}")

        # Create SwapIntent — protocol="aerodrome" is correct even on Optimism
        # (protocol_aliases.py maps ("optimism","velodrome") -> "aerodrome")
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="aerodrome",
            chain=CHAIN_NAME,
        )

        print(f"\nCreated SwapIntent: {intent.from_token} -> {intent.to_token}, amount={intent.amount}")

        # Layer 1: compile intent
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

        # Layer 2: execute via ExecutionOrchestrator
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: parse receipts — enforce that at least one swap event
        # was successfully parsed. The for-loop walks every tx in the bundle
        # (approval + swap), but only the swap call should produce a swap
        # event; we still require >= 1 across the bundle so a parser bug
        # that swallows the event fails the test rather than silently
        # skipping Layer 3.
        swap_results_parsed = 0
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt is None:
                continue

            # Pass token0/token1 (sorted ascending by address) so the parser
            # can resolve decimals and build a high-level swap_result.  This
            # mirrors how the parser is wired in production (the connector
            # constructs it with pool metadata after looking up the pool).
            t_a, t_b = token_in.lower(), token_out.lower()
            token0_addr, token1_addr = sorted([t_a, t_b])
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

                # L3 semantic cross-check
                assert_swap_semantic_match(
                    intent_amount=swap_amount,
                    intent_from_token="USDC",
                    intent_to_token="WETH",
                    swap_result=parse_result.swap_result,
                    chain=CHAIN_NAME,
                )
                print("  L3 semantic check: PASSED")

        assert swap_results_parsed >= 1, (
            "Layer 3 verification: AerodromeReceiptParser must successfully "
            "parse at least one swap event across the executed bundle. "
            f"Got {swap_results_parsed} parsed swap results."
        )

        # Layer 4 (post): verify bilateral balance deltas
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        weth_received = weth_after - weth_before

        print("\n--- Results ---")
        print(f"USDC spent:    {format_token_amount(usdc_spent, in_decimals)}")
        print(f"WETH received: {format_token_amount(weth_received, out_decimals)}")

        expected_usdc_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must equal swap amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )
        assert weth_received > 0, "Must receive positive WETH (no-op guard)"

        print("\nALL CHECKS PASSED")

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
        """Test WETH -> USDC swap using SwapIntent on Optimism (reverse direction)."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["WETH"]
        token_out = tokens["USDC"]

        fail_if_aerodrome_pool_missing(web3, CHAIN_NAME, token_in, token_out, stable=False)

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("0.05")  # 0.05 WETH

        print(f"\n{'='*80}")
        print("Test: WETH -> USDC Swap via SwapIntent (Aerodrome/Velodrome V2 on Optimism)")
        print(f"{'='*80}")

        # Layer 4 (pre): record balances BEFORE
        weth_before = get_token_balance(web3, token_in, funded_wallet)
        usdc_before = get_token_balance(web3, token_out, funded_wallet)

        # Layer 1: compile
        intent = SwapIntent(
            from_token="WETH",
            to_token="USDC",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
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
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        # Layer 2: execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: parse receipts — enforce that at least one swap event
        # was successfully parsed across the bundle (approval + swap tx);
        # a parser that silently fails must fail Layer 3, not skip it.
        swap_results_parsed = 0
        t_a, t_b = token_in.lower(), token_out.lower()
        token0_addr, token1_addr = sorted([t_a, t_b])
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt is None:
                continue

            parser = AerodromeReceiptParser(
                chain=CHAIN_NAME,
                token0_address=token0_addr,
                token1_address=token1_addr,
            )
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

            if parse_result.success and parse_result.swap_result:
                swap_results_parsed += 1
                assert parse_result.swap_result.amount_in_decimal > 0
                assert parse_result.swap_result.amount_out_decimal > 0
                assert parse_result.swap_result.effective_price > 0

                assert_swap_semantic_match(
                    intent_amount=swap_amount,
                    intent_from_token="WETH",
                    intent_to_token="USDC",
                    swap_result=parse_result.swap_result,
                    chain=CHAIN_NAME,
                )

        assert swap_results_parsed >= 1, (
            "Layer 3 verification: AerodromeReceiptParser must successfully "
            "parse at least one swap event across the executed bundle. "
            f"Got {swap_results_parsed} parsed swap results."
        )

        # Layer 4 (post): verify bilateral balance deltas
        weth_after = get_token_balance(web3, token_in, funded_wallet)
        usdc_after = get_token_balance(web3, token_out, funded_wallet)

        weth_spent = weth_before - weth_after
        usdc_received = usdc_after - usdc_before

        expected_weth_spent = int(swap_amount * Decimal(10**in_decimals))
        assert weth_spent == expected_weth_spent, (
            f"WETH spent must equal swap amount. "
            f"Expected: {expected_weth_spent}, Got: {weth_spent}"
        )
        assert usdc_received > 0, "Must receive positive USDC (no-op guard)"

        print(f"WETH spent:    {format_token_amount(weth_spent, in_decimals)}")
        print(f"USDC received: {format_token_amount(usdc_received, out_decimals)}")
        print("\nALL CHECKS PASSED")

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
        """Test that SwapIntent with insufficient balance fails gracefully.

        Bilateral conservation check: both input and output token balances
        must be unchanged after a failed swap.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]

        # Validate volatile pool exists before running test so the test
        # doesn't pass for the wrong reason (e.g., compilation revert from
        # a missing pool would also make execution fail, masking whether
        # the failure was due to insufficient balance or pool absence).
        fail_if_aerodrome_pool_missing(web3, CHAIN_NAME, token_in, token_out, stable=False)

        # Get current balances (record BOTH before)
        usdc_balance = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)
        in_decimals = get_token_decimals(web3, token_in)
        balance_decimal = Decimal(usdc_balance) / Decimal(10**in_decimals)

        # Try to swap 100x the available balance
        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'='*80}")
        print("Test: SwapIntent with Insufficient Balance (Aerodrome/Velodrome V2 on Optimism)")
        print(f"{'='*80}")
        print(f"Balance:   {balance_decimal} USDC")
        print(f"Trying:    {excessive_amount} USDC")

        # Layer 1: compile (should succeed — compiler does not check balance)
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=excessive_amount,
            max_slippage=Decimal("0.01"),
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

        # Layer 2: execute — must fail due to insufficient balance
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert not execution_result.success, "Execution should fail with insufficient balance"
        print(f"Execution failed as expected: {execution_result.error}")

        # Layer 4 (bilateral conservation): BOTH balances must be unchanged
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)
        assert usdc_after == usdc_balance, (
            "Input token balance must be unchanged after failed swap"
        )
        assert weth_after == weth_before, (
            "Output token balance must be unchanged after failed swap"
        )

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
