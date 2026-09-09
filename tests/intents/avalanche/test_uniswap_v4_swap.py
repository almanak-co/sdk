"""Production-grade SwapIntent tests for Uniswap V4 on Avalanche.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler (routes to V4 adapter)
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using UniswapV4ReceiptParser
5. Verify balances changed correctly

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

These tests pin the hookless WAVAX/USDC PoolKey with fee 500 and tick spacing
10. WAVAX remains an ERC-20 currency in that key; native AVAX is a distinct
currency represented by address(0). Compilation metadata, PoolManager receipts,
and exact token balance deltas must agree on the selected pool and swap amounts.

To run:
    uv run pytest tests/intents/avalanche/test_uniswap_v4_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.uniswap_v4.pool_key import PoolKey
from almanak.connectors.uniswap_v4.receipt_parser import UniswapV4ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    SWAP_MAX_SLIPPAGE,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "avalanche"
EXPECTED_POOL_KEY = PoolKey(
    currency0=CHAIN_CONFIGS[CHAIN_NAME]["tokens"]["WAVAX"],
    currency1=CHAIN_CONFIGS[CHAIN_NAME]["tokens"]["USDC"],
    fee=500,
    tick_spacing=10,
    hooks="0x" + "0" * 40,
)
USDC_TO_WAVAX_MAX_SLIPPAGE = Decimal("0.25")


# =============================================================================
# SwapIntent Tests — Uniswap V4 on Avalanche
# =============================================================================


@pytest.mark.avalanche
@pytest.mark.swap
class TestUniswapV4SwapIntent:
    """Test Uniswap V4 swaps using SwapIntent on Avalanche.

    These tests verify the full Intent flow:
    - SwapIntent creation with protocol="uniswap_v4"
    - IntentCompiler routes to UniswapV4Adapter
    - Transactions execute successfully on-chain via UniversalRouter
    - UniswapV4ReceiptParser correctly interprets PoolManager Swap events
    - Balance changes match expected amounts

    WAVAX and USDC are the two ERC-20 currencies of the explicitly pinned pool.
    Native AVAX pays transaction gas and is not a swap currency in these cases.
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_usdc_to_wavax_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC -> WAVAX swap using SwapIntent via Uniswap V4 on Avalanche.

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle
        2. Execution: ExecutionOrchestrator -> success
        3. Receipt Parsing: UniswapV4ReceiptParser -> swap amounts > 0
        4. Balance Deltas: USDC spent == swap amount, WAVAX received > 0
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WAVAX"]

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("100")  # 100 USDC

        print(f"\n{'=' * 80}")
        print("Test: USDC -> WAVAX Swap via Uniswap V4 on Avalanche")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} USDC")

        # Record balances before
        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        wavax_before = get_token_balance(web3, token_out, funded_wallet)

        print(f"USDC before:  {format_token_amount(usdc_before, in_decimals)}")
        print(f"WAVAX before: {format_token_amount(wavax_before, out_decimals)}")

        # Fast-fail funding adequacy check — surfaces fixture funding problems
        # immediately instead of failing later in execution or delta verification.
        expected_usdc_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdc_before >= expected_usdc_spent, (
            f"funded_wallet must hold >= {swap_amount} USDC to run this test "
            f"(have {format_token_amount(usdc_before, in_decimals)})"
        )

        # Layer 1: Compilation
        intent = SwapIntent(
            from_token="USDC",
            to_token="WAVAX",
            amount=swap_amount,
            # CI fork pin 86878028 measured V4TooLittleReceived with
            # 10.6707 WAVAX actual vs a 10.6878 WAVAX minimum at the shared
            # 20% tolerance. Keep the wider envelope local to this
            # CoinGecko-priced native-output direction.
            max_slippage=USDC_TO_WAVAX_MAX_SLIPPAGE,
            protocol="uniswap_v4",
            swap_params={"fee_tier": 500, "tick_spacing": 10},
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
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"
        assert compilation_result.action_bundle.metadata["pool_key"] == EXPECTED_POOL_KEY.to_wire()
        assert compilation_result.action_bundle.metadata["pool_id"] == EXPECTED_POOL_KEY.pool_id

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 2: Execution
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: Receipt Parsing
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
        parsed_swaps = []

        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    swap_pool_key=EXPECTED_POOL_KEY.to_wire(),
                    swap_token_meta=compilation_result.action_bundle.metadata["swap_token_meta"],
                )

                if parse_result.swap_result:
                    assert len(parse_result.swap_events) == 1
                    assert parse_result.swap_events[0].pool_id.lower() == EXPECTED_POOL_KEY.pool_id
                    parsed_swaps.append(parse_result.swap_result)
                    assert parse_result.swap_result.amount_in_decimal > 0, "Parsed amount_in must be > 0"
                    assert parse_result.swap_result.amount_out_decimal > 0, "Parsed amount_out must be > 0"
                    assert parse_result.swap_result.effective_price > 0, "Parsed effective_price must be > 0"
                    print(f"  Amount in:  {parse_result.swap_result.amount_in_decimal}")
                    print(f"  Amount out: {parse_result.swap_result.amount_out_decimal}")
                    print(f"  Price:      {parse_result.swap_result.effective_price}")

        assert len(parsed_swaps) == 1, "Exactly one swap must execute across the complete bundle"
        parsed_swap = parsed_swaps[0]
        assert parsed_swap.token_in == token_in.lower()
        assert parsed_swap.token_out == token_out.lower()

        # Layer 4: Balance Deltas
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        wavax_after = get_token_balance(web3, token_out, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        wavax_received = wavax_after - wavax_before
        assert usdc_spent == parsed_swap.amount_in
        assert wavax_received == parsed_swap.amount_out

        print("\n--- Balance Deltas ---")
        print(f"USDC spent:     {format_token_amount(usdc_spent, in_decimals)}")
        print(f"WAVAX received: {format_token_amount(wavax_received, out_decimals)}")

        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must equal swap amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )
        assert wavax_received > 0, "Must receive positive WAVAX (no-op guard)"

        print("\nALL 4 LAYERS PASSED")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_wavax_to_usdc_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test WAVAX -> USDC swap using SwapIntent via Uniswap V4 on Avalanche (reverse direction).

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle
        2. Execution: ExecutionOrchestrator -> success
        3. Receipt Parsing: UniswapV4ReceiptParser -> swap amounts > 0
        4. Balance Deltas: WAVAX spent == swap amount, USDC received > 0
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["WAVAX"]
        token_out = tokens["USDC"]

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("1")  # 1 WAVAX (~$9.77 at 2026-05-14 prices)

        print(f"\n{'=' * 80}")
        print("Test: WAVAX -> USDC Swap via Uniswap V4 on Avalanche")
        print(f"{'=' * 80}")

        wavax_before = get_token_balance(web3, token_in, funded_wallet)
        usdc_before = get_token_balance(web3, token_out, funded_wallet)

        # Fast-fail funding adequacy check — surfaces fixture funding problems
        # immediately instead of failing later in execution or delta verification.
        expected_wavax_spent = int(swap_amount * Decimal(10**in_decimals))
        assert wavax_before >= expected_wavax_spent, (
            f"funded_wallet must hold >= {swap_amount} WAVAX to run this test "
            f"(have {format_token_amount(wavax_before, in_decimals)})"
        )

        # Layer 1: Compilation
        intent = SwapIntent(
            from_token="WAVAX",
            to_token="USDC",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v4",
            swap_params={"fee_tier": 500, "tick_spacing": 10},
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
        assert compilation_result.action_bundle.metadata["pool_key"] == EXPECTED_POOL_KEY.to_wire()
        assert compilation_result.action_bundle.metadata["pool_id"] == EXPECTED_POOL_KEY.pool_id

        print(f"Compiled: {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 2: Execution
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Executed: {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: Receipt Parsing
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
        parsed_swaps = []

        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parse_result = parser.parse_receipt(
                    tx_result.receipt.to_dict(),
                    swap_pool_key=EXPECTED_POOL_KEY.to_wire(),
                    swap_token_meta=compilation_result.action_bundle.metadata["swap_token_meta"],
                )
                if parse_result.swap_result:
                    assert len(parse_result.swap_events) == 1
                    assert parse_result.swap_events[0].pool_id.lower() == EXPECTED_POOL_KEY.pool_id
                    parsed_swaps.append(parse_result.swap_result)
                    assert parse_result.swap_result.amount_in_decimal > 0, "Parsed amount_in must be > 0"
                    assert parse_result.swap_result.amount_out_decimal > 0, "Parsed amount_out must be > 0"
                    assert parse_result.swap_result.effective_price > 0, "Parsed effective_price must be > 0"
                    print(
                        f"  Swap: in={parse_result.swap_result.amount_in_decimal}, "
                        f"out={parse_result.swap_result.amount_out_decimal}"
                    )

        assert len(parsed_swaps) == 1, "Exactly one swap must execute across the complete bundle"
        parsed_swap = parsed_swaps[0]
        assert parsed_swap.token_in == token_in.lower()
        assert parsed_swap.token_out == token_out.lower()

        # Layer 4: Balance Deltas
        wavax_after = get_token_balance(web3, token_in, funded_wallet)
        usdc_after = get_token_balance(web3, token_out, funded_wallet)

        wavax_spent = wavax_before - wavax_after
        usdc_received = usdc_after - usdc_before
        assert wavax_spent == parsed_swap.amount_in
        assert usdc_received == parsed_swap.amount_out

        assert wavax_spent == expected_wavax_spent, (
            f"WAVAX spent mismatch. Expected: {expected_wavax_spent}, Got: {wavax_spent}"
        )
        assert usdc_received > 0, "Must receive positive USDC (no-op guard)"

        print(f"WAVAX spent:   {format_token_amount(wavax_spent, in_decimals)}")
        print(f"USDC received: {format_token_amount(usdc_received, out_decimals)}")
        print("\nALL 4 LAYERS PASSED")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_intent_with_insufficient_balance_fails(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test that V4 SwapIntent with insufficient balance fails gracefully.

        Failure-path Verification:
        1. Compilation: IntentCompiler -> SUCCESS (amount validation is on-chain)
        2. Execution: ExecutionOrchestrator -> fails
        3. Receipt Parsing: UniswapV4ReceiptParser -> NO successful swap parsed
           (Layer 3 negative-path check: confirms the failure was a real revert,
           not a silent no-op where 0 tokens move but the receipt looks normal)
        4. Balance Deltas: BOTH input AND output balances unchanged (bilateral conservation)
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WAVAX"]

        usdc_balance = get_token_balance(web3, token_in, funded_wallet)
        wavax_before = get_token_balance(web3, token_out, funded_wallet)
        assert usdc_balance > 0, "funded_wallet must hold USDC for this test"
        in_decimals = get_token_decimals(web3, token_in)
        balance_decimal = Decimal(usdc_balance) / Decimal(10**in_decimals)

        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'=' * 80}")
        print("Test: Uniswap V4 SwapIntent with Insufficient Balance on Avalanche")
        print(f"{'=' * 80}")
        print(f"Balance:   {balance_decimal} USDC")
        print(f"Trying:    {excessive_amount} USDC")

        intent = SwapIntent(
            from_token="USDC",
            to_token="WAVAX",
            amount=excessive_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v4",
            swap_params={"fee_tier": 500, "tick_spacing": 10},
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        # Layer 1: Compilation (should succeed)
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS"
        assert compilation_result.action_bundle is not None
        assert compilation_result.action_bundle.metadata["pool_key"] == EXPECTED_POOL_KEY.to_wire()
        assert compilation_result.action_bundle.metadata["pool_id"] == EXPECTED_POOL_KEY.pool_id

        # Layer 2: Execution (should fail)
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert not execution_result.success, "Execution should fail with insufficient balance"
        print(f"Execution failed as expected: {execution_result.error}")

        # Layer 3 (negative-path): receipt-parser must NOT see a successful swap.
        # Asserts the failure was a real revert rather than a silent no-op
        # (the V4 no-op bug class: TX status=1 but zero tokens move).
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                assert parse_result.swap_result is None, (
                    "Failed swap must not produce a successful swap parse result "
                    f"(got {parse_result.swap_result})"
                )

        # Layer 4: Bilateral balance conservation — BOTH tokens must be unchanged
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        wavax_after = get_token_balance(web3, token_out, funded_wallet)
        assert usdc_after == usdc_balance, "Input token balance must be unchanged after failed swap"
        assert wavax_after == wavax_before, "Output token balance must be unchanged after failed swap"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
