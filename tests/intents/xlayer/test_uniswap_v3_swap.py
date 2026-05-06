"""Production-grade SwapIntent tests for Uniswap V3 on X-Layer.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using UniswapV3ReceiptParser
5. Verify balances changed correctly

Token-pair selection (issue #2106):

    Verified on-chain 2026-05-06 against the canonical xlayer Uniswap V3
    factory (0x4B2ab38DBF28D31D467aA8993f6c2585981D6804): USDC/WETH has no
    pool at any fee tier; USDT0/WETH @ 100, USDC/USDT0 @ 500, and
    USDC/WOKB @ all tiers exist but exhaust their concentrated-liquidity
    range below 0.001-USDC swap-in (≥99% price impact).

    The only liquid stablecoin pair is **USDT0/USDG @ fee=100**:
    - pool: 0x0cBe0dBE1400e57f371a38BD3b9bC80F7C3676dA
    - liquidity: 3.87e14 raw
    - 100 USDT0 → 99.98 USDG (≈0.02% PI), bilateral

    These tests exercise that pair so the matrix shard reflects real
    on-chain swap behaviour rather than a synthetic skip.

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

To run:
    uv run pytest tests/intents/xlayer/test_uniswap_v3_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    assert_swap_semantic_match,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)
from tests.intents.pool_helpers import fail_if_v3_pool_missing

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "xlayer"
SWAP_FEE_TIER = 100  # USDT0/USDG liquid pool is at fee=100


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.xlayer
@pytest.mark.swap
class TestUniswapV3SwapIntent:
    """Test Uniswap V3 swaps on X-Layer using SwapIntent.

    These tests verify the full Intent flow:
    - SwapIntent creation with proper parameters
    - IntentCompiler generates correct Uniswap V3 transactions
    - Transactions execute successfully on-chain
    - UniswapV3ReceiptParser correctly interprets results
    - Balance changes match expected amounts
    """

    @pytest.mark.asyncio
    async def test_swap_usdt0_to_usdg_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDT0 -> USDG swap using SwapIntent (xlayer's only liquid pair).

        Flow:
        1. Create SwapIntent for USDT0 -> USDG
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Verify balances changed correctly
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDT0"]
        token_out = tokens["USDG"]

        # Validate pool exists before running test
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "uniswap_v3", token_in, token_out, SWAP_FEE_TIER)

        # Get decimals
        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        # Amount to swap
        swap_amount = Decimal("100")  # 100 USDT0

        print(f"\n{'='*80}")
        print("Test: USDT0 -> USDG Swap via SwapIntent (Uniswap V3 on X-Layer)")
        print(f"{'='*80}")
        print(f"Swap amount: {swap_amount} USDT0")

        # Record balances before
        usdt0_before = get_token_balance(web3, token_in, funded_wallet)
        usdg_before = get_token_balance(web3, token_out, funded_wallet)
        assert usdt0_before > 0, (
            "funded_wallet must have USDT0 seeded; zero balance indicates a "
            "fixture / Safe-funding regression rather than a real test scenario"
        )

        print(f"USDT0 before: {format_token_amount(usdt0_before, in_decimals)}")
        print(f"USDG before:  {format_token_amount(usdg_before, out_decimals)}")

        # Create SwapIntent. 1% slippage is generous for a stablecoin pair on a
        # ~0.02%-PI pool but tolerates the 0.01% pool fee plus tiny oracle
        # divergence (CoinGecko maps USDG → "usd-coin" so both legs price at $1).
        intent = SwapIntent(
            from_token="USDT0",
            to_token="USDG",
            amount=swap_amount,
            max_slippage=Decimal("0.01"),
            protocol="uniswap_v3",
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

        # Parse receipts (Layer 3)
        swap_parsed = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i+1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                from almanak.framework.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser

                parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.swap_result:
                    print(f"  Amount in:  {parse_result.swap_result.amount_in_decimal}")
                    print(f"  Amount out: {parse_result.swap_result.amount_out_decimal}")
                    print(f"  Price:      {parse_result.swap_result.effective_price}")

                    # L3 semantic verification
                    assert_swap_semantic_match(
                        intent_amount=swap_amount,
                        intent_from_token="USDT0",
                        intent_to_token="USDG",
                        swap_result=parse_result.swap_result,
                        chain=CHAIN_NAME,
                    )
                    swap_parsed = True
                    print("  L3 semantic check: PASSED")

        assert swap_parsed, "At least one swap must be parsed from receipts"

        # Verify balance changes (Layer 4 — bilateral)
        usdt0_after = get_token_balance(web3, token_in, funded_wallet)
        usdg_after = get_token_balance(web3, token_out, funded_wallet)

        usdt0_spent = usdt0_before - usdt0_after
        usdg_received = usdg_after - usdg_before

        print("\n--- Results ---")
        print(f"USDT0 spent:    {format_token_amount(usdt0_spent, in_decimals)}")
        print(f"USDG received:  {format_token_amount(usdg_received, out_decimals)}")

        # Verify USDT0 was spent EXACTLY by the swap amount
        expected_usdt0_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdt0_spent == expected_usdt0_spent, (
            f"USDT0 spent must equal swap amount. "
            f"Expected: {expected_usdt0_spent}, Got: {usdt0_spent}"
        )

        # Verify USDG was received (no-op guard) — for stable pair we expect
        # near-1:1 modulo the 0.01% pool fee + ~0.02% PI.
        assert usdg_received > 0, "Must receive positive USDG"

        print("\nALL CHECKS PASSED")

    @pytest.mark.asyncio
    async def test_swap_usdg_to_usdt0_using_intent(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDG -> USDT0 swap using SwapIntent (reverse direction)."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDG"]
        token_out = tokens["USDT0"]

        # Validate pool exists before running test
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "uniswap_v3", token_in, token_out, SWAP_FEE_TIER)

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("100")  # 100 USDG

        print(f"\n{'='*80}")
        print("Test: USDG -> USDT0 Swap via SwapIntent (Uniswap V3 on X-Layer)")
        print(f"{'='*80}")

        usdg_before = get_token_balance(web3, token_in, funded_wallet)
        usdt0_before = get_token_balance(web3, token_out, funded_wallet)
        assert usdg_before > 0, (
            "funded_wallet must have USDG seeded; zero balance indicates a "
            "fixture / Safe-funding regression rather than a real test scenario"
        )

        # Create intent
        intent = SwapIntent(
            from_token="USDG",
            to_token="USDT0",
            amount=swap_amount,
            max_slippage=Decimal("0.01"),
            protocol="uniswap_v3",
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

        # Parse receipts (Layer 3)
        swap_parsed = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                from almanak.framework.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser

                parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.success and parse_result.swap_result:
                    assert parse_result.swap_result.amount_in_decimal > 0
                    assert parse_result.swap_result.amount_out_decimal > 0

                    assert_swap_semantic_match(
                        intent_amount=swap_amount,
                        intent_from_token="USDG",
                        intent_to_token="USDT0",
                        swap_result=parse_result.swap_result,
                        chain=CHAIN_NAME,
                    )
                    swap_parsed = True

        assert swap_parsed, "At least one swap must be parsed from receipts"

        # Verify balance deltas (Layer 4 — bilateral)
        usdg_after = get_token_balance(web3, token_in, funded_wallet)
        usdt0_after = get_token_balance(web3, token_out, funded_wallet)

        usdg_spent = usdg_before - usdg_after
        usdt0_received = usdt0_after - usdt0_before

        expected_usdg_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdg_spent == expected_usdg_spent
        assert usdt0_received > 0

        print(f"USDG spent:     {format_token_amount(usdg_spent, in_decimals)}")
        print(f"USDT0 received: {format_token_amount(usdt0_received, out_decimals)}")
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
        token_in = tokens["USDT0"]
        token_out = tokens["USDG"]

        # Pin the test to the live USDT0/USDG @ fee=100 route so that
        # pool/route regressions surface as a clear precondition failure
        # rather than masquerading as the insufficient-balance failure
        # this test exists to exercise.
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "uniswap_v3", token_in, token_out, SWAP_FEE_TIER)

        # Get current balances (precondition: must be funded — surfaces a
        # fixture/funding regression as a clear failure rather than silently
        # exercising zero-amount behaviour).
        usdt0_balance = get_token_balance(web3, token_in, funded_wallet)
        usdg_before = get_token_balance(web3, token_out, funded_wallet)
        assert usdt0_balance > 0, (
            "funded_wallet must have USDT0 seeded; zero balance indicates a "
            "fixture / Safe-funding regression rather than a real test scenario"
        )
        in_decimals = get_token_decimals(web3, token_in)
        balance_decimal = Decimal(usdt0_balance) / Decimal(10**in_decimals)

        # Try to swap exactly one base unit (1 / 10^decimals) over the wallet
        # balance. This decouples the test from the fixture's seeded amount —
        # any future change to the seeding (1k → 100k → 1M USDT0) keeps the
        # failure pinned to submission-time insufficient-balance rather than
        # accidentally tripping the compiler's price-impact guard at large
        # multiplied amounts.
        smallest_unit = Decimal(1) / Decimal(10**in_decimals)
        excessive_amount = balance_decimal + smallest_unit

        print(f"\n{'='*80}")
        print("Test: SwapIntent with Insufficient Balance")
        print(f"{'='*80}")
        print(f"Balance:   {balance_decimal} USDT0")
        print(f"Trying:    {excessive_amount} USDT0")

        intent = SwapIntent(
            from_token="USDT0",
            to_token="USDG",
            amount=excessive_amount,
            max_slippage=Decimal("0.01"),
            protocol="uniswap_v3",
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
        usdt0_after = get_token_balance(web3, token_in, funded_wallet)
        usdg_after = get_token_balance(web3, token_out, funded_wallet)
        assert usdt0_after == usdt0_balance, "Input token balance must be unchanged after failed swap"
        assert usdg_after == usdg_before, "Output token balance must be unchanged after failed swap"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
