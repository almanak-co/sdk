"""Production-grade SwapIntent tests for LiFi on Arbitrum.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler (protocol="lifi")
3. Execute via ExecutionOrchestrator (full production pipeline, including deferred refresh)
4. Parse receipts using LiFiReceiptParser
5. Verify balances changed correctly

NO MOCKING. All tests execute real on-chain swaps using the real LiFi API
and verify state changes on an Anvil fork.

To run:
    uv run pytest tests/intents/arbitrum/test_lifi_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

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
class TestLiFiSwap:
    """Test LiFi same-chain swaps using SwapIntent on Arbitrum.

    These tests verify the full Intent flow using LiFi as the routing
    aggregator. LiFi routes through multiple DEXs (1inch, 0x, Paraswap,
    etc.) to find optimal prices.

    Key differences from Uniswap/other DEX tests:
    - No pool validation needed (LiFi routes through multiple DEXs)
    - Uses deferred refresh pattern (fresh calldata fetched at execution)
    - Higher slippage tolerance (5%) due to API-sourced routes
    """

    @pytest.mark.xfail(reason="LiFi KyberSwap routing reverts with TRANSFER_FROM_FAILED on Anvil fork", strict=False)
    @pytest.mark.asyncio
    async def test_swap_usdc_to_weth_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC -> WETH swap using SwapIntent with LiFi.

        Flow:
        1. Create SwapIntent for USDC -> WETH with protocol="lifi"
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator (deferred refresh fetches fresh route)
        4. Verify balances changed correctly
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]

        # Get decimals
        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        # Amount to swap
        swap_amount = Decimal("100")  # 100 USDC

        print(f"\n{'='*80}")
        print("Test: USDC -> WETH Swap via LiFi SwapIntent")
        print(f"{'='*80}")
        print(f"Swap amount: {swap_amount} USDC")

        # Record balances before
        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)
        assert usdc_before > 0, "funded_wallet must have USDC for this test"

        print(f"USDC before: {format_token_amount(usdc_before, in_decimals)}")
        print(f"WETH before: {format_token_amount(weth_before, out_decimals)}")

        # Create SwapIntent with LiFi
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=swap_amount,
            max_slippage=Decimal("0.05"),  # 5% slippage for aggregator routes
            protocol="lifi",
            chain=CHAIN_NAME,
        )

        print(f"\nCreated SwapIntent: {intent.from_token} -> {intent.to_token}, amount={intent.amount}")

        # Compile intent
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

        # Verify deferred swap metadata
        metadata = compilation_result.action_bundle.metadata
        assert metadata.get("deferred_swap") is True, "LiFi bundles must be deferred"
        assert metadata.get("protocol") == "lifi"

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")
        print(f"Protocol: {metadata.get('protocol')}, Tool: {metadata.get('tool')}")

        # Execute via ExecutionOrchestrator (deferred refresh happens automatically)
        print("\nExecuting via ExecutionOrchestrator (with deferred refresh)...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Parse receipts
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                from almanak.framework.connectors.lifi.receipt_parser import LiFiReceiptParser

                parser = LiFiReceiptParser()
                parse_result = parser.parse_swap_receipt(
                    receipt=tx_result.receipt.to_dict(),
                    wallet_address=funded_wallet,
                    token_out=token_out,
                    token_in=token_in,
                )
                if parse_result.success:
                    print(f"  Amount in:  {parse_result.amount_in}")
                    print(f"  Amount out: {parse_result.amount_out}")
                    # LiFi routes through varying DEXs, so Transfer event patterns
                    # differ per route. Parser extraction is best-effort; balance
                    # deltas (below) are the authoritative verification.
                    if parse_result.amount_in == 0:
                        print("  WARNING: Parser could not extract amount_in from receipt")
                    if parse_result.amount_out == 0:
                        print("  WARNING: Parser could not extract amount_out from receipt")

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

    @pytest.mark.xfail(reason="Reverse direction (WETH -> USDC) may have different routing and slippage, needs separate test",strict=False)
    @pytest.mark.asyncio
    async def test_swap_weth_to_usdc_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test WETH -> USDC swap using SwapIntent with LiFi (reverse direction)."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["WETH"]
        token_out = tokens["USDC"]

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("0.05")  # 0.05 WETH

        print(f"\n{'='*80}")
        print("Test: WETH -> USDC Swap via LiFi SwapIntent")
        print(f"{'='*80}")

        weth_before = get_token_balance(web3, token_in, funded_wallet)
        usdc_before = get_token_balance(web3, token_out, funded_wallet)
        assert weth_before > 0, "funded_wallet must have WETH for this test"

        # Create intent
        intent = SwapIntent(
            from_token="WETH",
            to_token="USDC",
            amount=swap_amount,
            max_slippage=Decimal("0.05"),  # 5% slippage for aggregator routes
            protocol="lifi",
            chain=CHAIN_NAME,
        )

        # Compile with real prices
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        # Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Verify
        weth_after = get_token_balance(web3, token_in, funded_wallet)
        usdc_after = get_token_balance(web3, token_out, funded_wallet)

        weth_spent = weth_before - weth_after
        usdc_received = usdc_after - usdc_before

        expected_weth_spent = int(swap_amount * Decimal(10**in_decimals))
        assert weth_spent == expected_weth_spent, (
            f"WETH spent must equal swap amount. "
            f"Expected: {expected_weth_spent}, Got: {weth_spent}"
        )
        assert usdc_received > 0, "Must receive positive USDC"

        print(f"WETH spent:    {format_token_amount(weth_spent, in_decimals)}")
        print(f"USDC received: {format_token_amount(usdc_received, out_decimals)}")
        print("\nALL CHECKS PASSED")

    @pytest.mark.asyncio
    async def test_swap_insufficient_balance_fails_safely(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test that SwapIntent with insufficient balance fails gracefully."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]

        # Get current balance
        usdc_balance = get_token_balance(web3, token_in, funded_wallet)
        in_decimals = get_token_decimals(web3, token_in)
        balance_decimal = Decimal(usdc_balance) / Decimal(10**in_decimals)
        assert usdc_balance > 0, "funded_wallet must have USDC for this test"

        # Try to swap more than we have
        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'='*80}")
        print("Test: LiFi SwapIntent with Insufficient Balance")
        print(f"{'='*80}")
        print(f"Balance:   {balance_decimal} USDC")
        print(f"Trying:    {excessive_amount} USDC")

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=excessive_amount,
            max_slippage=Decimal("0.05"),
            protocol="lifi",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        # Try to execute - should fail
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert not execution_result.success, "Execution should fail with insufficient balance"
        print(f"Execution failed as expected: {execution_result.error}")

        # Verify balance unchanged (conservation check)
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        assert usdc_after == usdc_balance, "Balance must be unchanged after failed swap"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
