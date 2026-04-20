"""Production-grade SwapIntent tests for Pendle on Arbitrum.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent for Pendle token -> PT swaps
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using PendleReceiptParser
5. Verify balances changed correctly

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

NOTE: PT -> token (sell PT) path is blocked on Arbitrum (VIB-568).
Tests cover the working token -> PT (buy PT) path.

To run:
    uv run pytest tests/intents/arbitrum/test_pendle_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.pendle.receipt_parser import PendleReceiptParser
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

# Pendle market addresses on Arbitrum
# PT-wstETH-25JUN2026 market
PENDLE_WSTETH_MARKET = "0xf78452e0f5c0b95fc5dc8353b8cd1e06e53fa25b"
# PT-wstETH-25JUN2026 token address
PT_WSTETH_ADDRESS = "0x71fbf40651e9d4278a74586afc99f307f369ce9a"


def _enrich_oracle_with_wsteth(price_oracle: dict[str, Decimal]) -> dict[str, Decimal]:
    """Add WSTETH price to oracle if missing (needed for pre-swap estimation).

    wstETH trades at ~1.17x ETH due to accumulated staking rewards.
    The compiler needs WSTETH price to estimate the pre-swap output
    when the input token differs from the SY-minting token (wstETH).
    """
    enriched = dict(price_oracle)
    if "WSTETH" not in enriched and "WETH" in enriched:
        enriched["WSTETH"] = enriched["WETH"] * Decimal("1.17")
    return enriched


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.swap
class TestPendleSwapIntent:
    """Test Pendle swaps using SwapIntent.

    These tests verify the full Intent flow:
    - SwapIntent creation for token -> PT swaps (buy PT)
    - IntentCompiler generates correct Pendle router transactions
    - Transactions execute successfully on-chain
    - PendleReceiptParser correctly interprets Swap events
    - Balance changes match expected amounts

    NOTE: PT -> token (sell PT) path is blocked on Arbitrum (VIB-568).
    """

    @pytest.mark.xfail(
        reason="Pre-submit balance check blocks multi-step bundles: WSTETH balance is 0 before "
        "the WETH->WSTETH pre-swap runs. Needs framework fix to skip intermediate token checks.",
        strict=False,
    )
    @pytest.mark.asyncio
    async def test_swap_weth_to_pt_wsteth_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test WETH -> PT-wstETH-25JUN2026 swap using SwapIntent.

        This tests the token -> PT (buy PT) path in _compile_pendle_swap.
        The compiler may insert a pre-swap step to convert WETH to wstETH (SY underlying)
        before executing the Pendle swap.

        Flow:
        1. Create SwapIntent for WETH -> PT-WSTETH-25JUN2026
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Parse receipt for Pendle Swap event
        5. Verify WETH decreased and PT tokens received
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        weth = tokens["WETH"]
        weth_decimals = get_token_decimals(web3, weth)

        # Swap a small amount of WETH to PT
        swap_amount = Decimal("0.1")  # 0.1 WETH

        print(f"\n{'='*80}")
        print("Test: WETH -> PT-wstETH-25JUN2026 Swap via SwapIntent (Pendle)")
        print(f"{'='*80}")
        print(f"Swap amount: {swap_amount} WETH")

        # Record balances BEFORE
        weth_before = get_token_balance(web3, weth, funded_wallet)
        pt_before = get_token_balance(web3, PT_WSTETH_ADDRESS, funded_wallet)

        print(f"WETH before: {format_token_amount(weth_before, weth_decimals)}")
        print(f"PT before: {format_token_amount(pt_before, 18)}")  # PT has 18 decimals

        # Create SwapIntent
        intent = SwapIntent(
            from_token="WETH",
            to_token="PT-WSTETH-25JUN2026",
            amount=swap_amount,
            max_slippage=Decimal("0.20"),  # 20% slippage for oracle-based quoting + pre-swap
            protocol="pendle",
            chain=CHAIN_NAME,
        )

        print(f"\nCreated SwapIntent: {intent.from_token} -> {intent.to_token}, amount={intent.amount}")

        # Compile intent with real prices from CoinGecko
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=_enrich_oracle_with_wsteth(price_oracle),
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

        # Parse receipts
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parser = PendleReceiptParser(
                    chain=CHAIN_NAME,
                    token_in_decimals=weth_decimals,
                    token_out_decimals=18,  # PT decimals
                )
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.swap_result:
                    print(f"  Swap type: {parse_result.swap_result.swap_type}")
                    print(f"  Amount in:  {parse_result.swap_result.amount_in_decimal}")
                    print(f"  Amount out: {parse_result.swap_result.amount_out_decimal}")
                    print(f"  Price:      {parse_result.swap_result.effective_price}")
                    assert parse_result.swap_result.amount_in_decimal > 0
                    assert parse_result.swap_result.amount_out_decimal > 0
                    assert parse_result.swap_result.effective_price > 0

        # Verify balance changes
        weth_after = get_token_balance(web3, weth, funded_wallet)
        pt_after = get_token_balance(web3, PT_WSTETH_ADDRESS, funded_wallet)

        weth_spent = weth_before - weth_after
        pt_received = pt_after - pt_before

        print("\n--- Results ---")
        print(f"WETH spent:    {format_token_amount(weth_spent, weth_decimals)}")
        print(f"PT received:   {format_token_amount(pt_received, 18)}")

        # Verify WETH was spent
        expected_weth_spent = int(swap_amount * Decimal(10**weth_decimals))
        assert weth_spent == expected_weth_spent, (
            f"WETH spent must EXACTLY equal swap amount. "
            f"Expected: {expected_weth_spent}, Got: {weth_spent}"
        )

        # Verify PT tokens were received
        assert pt_received > 0, "Must receive positive PT tokens"

        print("\nALL CHECKS PASSED")

    @pytest.mark.xfail(
        reason="Pre-submit balance check blocks multi-step bundles: WSTETH balance is 0 before "
        "the USDC->WSTETH pre-swap runs. Needs framework fix to skip intermediate token checks.",
        strict=False,
    )
    @pytest.mark.asyncio
    async def test_swap_usdc_to_pt_wsteth_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC -> PT-wstETH-25JUN2026 swap using SwapIntent.

        This tests the token_mint_sy pre-swap path in _compile_pendle_swap.
        USDC is not the SY underlying (wstETH), so the compiler must insert
        a pre-swap from USDC -> wstETH before the Pendle swap.

        Flow:
        1. Create SwapIntent for USDC -> PT-WSTETH-25JUN2026
        2. Compile to ActionBundle (includes pre-swap)
        3. Execute via ExecutionOrchestrator
        4. Parse receipt for Pendle Swap event
        5. Verify USDC decreased and PT tokens received
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        usdc = tokens["USDC"]
        usdc_decimals = get_token_decimals(web3, usdc)

        # Swap 100 USDC to PT
        swap_amount = Decimal("100")  # 100 USDC

        print(f"\n{'='*80}")
        print("Test: USDC -> PT-wstETH-25JUN2026 Swap via SwapIntent (Pendle with pre-swap)")
        print(f"{'='*80}")
        print(f"Swap amount: {swap_amount} USDC")

        # Record balances BEFORE
        usdc_before = get_token_balance(web3, usdc, funded_wallet)
        pt_before = get_token_balance(web3, PT_WSTETH_ADDRESS, funded_wallet)

        print(f"USDC before: {format_token_amount(usdc_before, usdc_decimals)}")
        print(f"PT before: {format_token_amount(pt_before, 18)}")

        # Create SwapIntent
        intent = SwapIntent(
            from_token="USDC",
            to_token="PT-WSTETH-25JUN2026",
            amount=swap_amount,
            max_slippage=Decimal("0.20"),  # 20% slippage for multi-hop (pre-swap + Pendle swap)
            protocol="pendle",
            chain=CHAIN_NAME,
        )

        print(f"\nCreated SwapIntent: {intent.from_token} -> {intent.to_token}, amount={intent.amount}")

        # Compile intent with real prices from CoinGecko
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=_enrich_oracle_with_wsteth(price_oracle),
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

        # Parse receipts
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parser = PendleReceiptParser(
                    chain=CHAIN_NAME,
                    token_in_decimals=usdc_decimals,
                    token_out_decimals=18,  # PT decimals
                )
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.swap_result:
                    print(f"  Swap type: {parse_result.swap_result.swap_type}")
                    print(f"  Amount in:  {parse_result.swap_result.amount_in_decimal}")
                    print(f"  Amount out: {parse_result.swap_result.amount_out_decimal}")
                    print(f"  Price:      {parse_result.swap_result.effective_price}")
                    assert parse_result.swap_result.amount_in_decimal > 0
                    assert parse_result.swap_result.amount_out_decimal > 0
                    assert parse_result.swap_result.effective_price > 0

        # Verify balance changes
        usdc_after = get_token_balance(web3, usdc, funded_wallet)
        pt_after = get_token_balance(web3, PT_WSTETH_ADDRESS, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        pt_received = pt_after - pt_before

        print("\n--- Results ---")
        print(f"USDC spent:    {format_token_amount(usdc_spent, usdc_decimals)}")
        print(f"PT received:   {format_token_amount(pt_received, 18)}")

        # Verify USDC was spent
        expected_usdc_spent = int(swap_amount * Decimal(10**usdc_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal swap amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # Verify PT tokens were received
        assert pt_received > 0, "Must receive positive PT tokens"

        print("\nALL CHECKS PASSED")

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
        weth = tokens["WETH"]

        # Get current balance and guard against zero
        weth_balance = get_token_balance(web3, weth, funded_wallet)
        pt_before = get_token_balance(web3, PT_WSTETH_ADDRESS, funded_wallet)
        assert weth_balance > 0, "Funded wallet must have positive WETH balance for this test"
        weth_decimals = get_token_decimals(web3, weth)
        balance_decimal = Decimal(weth_balance) / Decimal(10**weth_decimals)

        # Try to swap more than we have
        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'='*80}")
        print("Test: SwapIntent with Insufficient Balance (Pendle)")
        print(f"{'='*80}")
        print(f"Balance:   {balance_decimal} WETH")
        print(f"Trying:    {excessive_amount} WETH")

        intent = SwapIntent(
            from_token="WETH",
            to_token="PT-WSTETH-25JUN2026",
            amount=excessive_amount,
            max_slippage=Decimal("0.20"),
            protocol="pendle",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=_enrich_oracle_with_wsteth(price_oracle),
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
        weth_after = get_token_balance(web3, weth, funded_wallet)
        pt_after = get_token_balance(web3, PT_WSTETH_ADDRESS, funded_wallet)
        assert weth_after == weth_balance, "Input token balance must be unchanged after failed swap"
        assert pt_after == pt_before, "Output token balance must be unchanged after failed swap"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
