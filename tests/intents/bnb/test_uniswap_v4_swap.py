"""Production-grade SwapIntent tests for Uniswap V4 on BNB Chain.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler (routes to V4 adapter)
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using UniswapV4ReceiptParser
5. Verify balances changed correctly

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

Pool verification (probed against BSC mainnet 2026-05-14):
- NATIVE/USDT fee=500  ts=10 sqrtPriceX96=2.054e30 tick=65110 (initialized)
- NATIVE/USDT fee=3000 ts=60 sqrtPriceX96=2.054e30 tick=65102 (initialized)
- NATIVE/USDC fee=500  ts=10 sqrtPriceX96=2.054e30 tick=65109 (initialized)
- V4 Quoter `quoteExactInputSingle` returns 0.672 USDT for 0.001 BNB against
  the fee=500 NATIVE/USDT pool, and 0.0149 BNB for 10 USDT (round-trip sanity).
  fee=500 is the deepest BNB native-key pool on BSC.

This file uses **WBNB/USDT** rather than USDT/USDC (or any ERC20<>ERC20 pair)
because WBNB is the wrapped native on BSC. ``UniswapV4SDK._is_wrapped_native(WBNB)``
returns True, so the SDK substitutes the NATIVE_CURRENCY sentinel
(address(0)) for the pool key — the same code path that works on
Base/Optimism/Arbitrum/Avalanche where the wrapped-native/USDC test routes
through the NATIVE/USDC pool. VIB-4413 documents that the ERC20<>ERC20 V4
swap path reverts via UniversalRouter (it succeeds on chains where the
wrapped native is one side because the SDK rewrites the pool key to NATIVE).
Picking the wrapped-native side here avoids that bug entirely and exercises
the known-good native-key path.

bsc/bnb alias note: the IntentCompiler / connectors key off ``chain="bsc"``
(the SDK canonical name). The directory is ``tests/intents/bnb/`` to match
the framework's user-facing chain alias ``bnb``. ``resolve_chain_name``
handles both. The conftest in this directory pins ``CHAIN_NAME = "bsc"``.

To run:
    uv run pytest tests/intents/bnb/test_uniswap_v4_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

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

pytestmark = pytest.mark.no_zodiac(
    reason="VIB-4343: uniswap_v4 not yet in synthetic_intents matrix"
)

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "bsc"


# =============================================================================
# SwapIntent Tests — Uniswap V4 on BNB Chain
# =============================================================================


@pytest.mark.bsc
@pytest.mark.swap
class TestUniswapV4SwapIntent:
    """Test Uniswap V4 swaps using SwapIntent on BNB Chain.

    These tests verify the full Intent flow:
    - SwapIntent creation with protocol="uniswap_v4"
    - IntentCompiler routes to UniswapV4Adapter
    - Transactions execute successfully on-chain via UniversalRouter
    - UniswapV4ReceiptParser correctly interprets PoolManager Swap events
    - Balance changes match expected amounts

    Pair choice: WBNB/USDT routes through the NATIVE/USDT V4 pool (WBNB is
    the wrapped native on BSC, so the SDK substitutes address(0) for the
    pool key). This avoids the VIB-4413 ERC20<>ERC20 UR-mediated bug that
    affects pairs where neither side is the wrapped native.
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_usdt_to_wbnb_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDT -> WBNB swap using SwapIntent via Uniswap V4 on BNB Chain.

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle
        2. Execution: ExecutionOrchestrator -> success
        3. Receipt Parsing: UniswapV4ReceiptParser -> swap amounts > 0
        4. Balance Deltas: USDT spent == swap amount, WBNB received > 0
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDT"]
        token_out = tokens["WBNB"]

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("100")  # 100 USDT

        print(f"\n{'=' * 80}")
        print("Test: USDT -> WBNB Swap via Uniswap V4 on BNB Chain")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} USDT")

        # Record balances before
        usdt_before = get_token_balance(web3, token_in, funded_wallet)
        wbnb_before = get_token_balance(web3, token_out, funded_wallet)

        print(f"USDT before:  {format_token_amount(usdt_before, in_decimals)}")
        print(f"WBNB before: {format_token_amount(wbnb_before, out_decimals)}")

        # Fast-fail funding adequacy check — surfaces fixture funding problems
        # immediately instead of failing later in execution or delta verification.
        expected_usdt_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdt_before >= expected_usdt_spent, (
            f"funded_wallet must hold >= {swap_amount} USDT to run this test "
            f"(have {format_token_amount(usdt_before, in_decimals)})"
        )

        # Layer 1: Compilation
        intent = SwapIntent(
            from_token="USDT",
            to_token="WBNB",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v4",
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

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 2: Execution
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: Receipt Parsing
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
        parsed_swap = False

        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())

                if parse_result.swap_result:
                    parsed_swap = True
                    assert parse_result.swap_result.amount_in_decimal > 0, "Parsed amount_in must be > 0"
                    assert parse_result.swap_result.amount_out_decimal > 0, "Parsed amount_out must be > 0"
                    assert parse_result.swap_result.effective_price > 0, "Parsed effective_price must be > 0"
                    print(f"  Amount in:  {parse_result.swap_result.amount_in_decimal}")
                    print(f"  Amount out: {parse_result.swap_result.amount_out_decimal}")
                    print(f"  Price:      {parse_result.swap_result.effective_price}")

        assert parsed_swap, "Must find at least one Swap event in transaction receipts"

        # Layer 4: Balance Deltas
        usdt_after = get_token_balance(web3, token_in, funded_wallet)
        wbnb_after = get_token_balance(web3, token_out, funded_wallet)

        usdt_spent = usdt_before - usdt_after
        wbnb_received = wbnb_after - wbnb_before

        print("\n--- Balance Deltas ---")
        print(f"USDT spent:    {format_token_amount(usdt_spent, in_decimals)}")
        print(f"WBNB received: {format_token_amount(wbnb_received, out_decimals)}")

        assert usdt_spent == expected_usdt_spent, (
            f"USDT spent must equal swap amount. "
            f"Expected: {expected_usdt_spent}, Got: {usdt_spent}"
        )
        assert wbnb_received > 0, "Must receive positive WBNB (no-op guard)"

        print("\nALL 4 LAYERS PASSED")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_wbnb_to_usdt_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test WBNB -> USDT swap using SwapIntent via Uniswap V4 on BNB Chain (reverse direction).

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle
        2. Execution: ExecutionOrchestrator -> success
        3. Receipt Parsing: UniswapV4ReceiptParser -> swap amounts > 0
        4. Balance Deltas: WBNB spent == swap amount, USDT received > 0
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["WBNB"]
        token_out = tokens["USDT"]

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("1")  # 1 WBNB (~$670 at 2026-05-14 prices)

        print(f"\n{'=' * 80}")
        print("Test: WBNB -> USDT Swap via Uniswap V4 on BNB Chain")
        print(f"{'=' * 80}")

        wbnb_before = get_token_balance(web3, token_in, funded_wallet)
        usdt_before = get_token_balance(web3, token_out, funded_wallet)

        # Fast-fail funding adequacy check — surfaces fixture funding problems
        # immediately instead of failing later in execution or delta verification.
        expected_wbnb_spent = int(swap_amount * Decimal(10**in_decimals))
        assert wbnb_before >= expected_wbnb_spent, (
            f"funded_wallet must hold >= {swap_amount} WBNB to run this test "
            f"(have {format_token_amount(wbnb_before, in_decimals)})"
        )

        # Layer 1: Compilation
        intent = SwapIntent(
            from_token="WBNB",
            to_token="USDT",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v4",
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

        print(f"Compiled: {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 2: Execution
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Executed: {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: Receipt Parsing
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
        parsed_swap = False

        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                if parse_result.swap_result:
                    parsed_swap = True
                    assert parse_result.swap_result.amount_in_decimal > 0, "Parsed amount_in must be > 0"
                    assert parse_result.swap_result.amount_out_decimal > 0, "Parsed amount_out must be > 0"
                    assert parse_result.swap_result.effective_price > 0, "Parsed effective_price must be > 0"
                    print(
                        f"  Swap: in={parse_result.swap_result.amount_in_decimal}, "
                        f"out={parse_result.swap_result.amount_out_decimal}"
                    )

        assert parsed_swap, "Must find at least one Swap event in transaction receipts"

        # Layer 4: Balance Deltas
        wbnb_after = get_token_balance(web3, token_in, funded_wallet)
        usdt_after = get_token_balance(web3, token_out, funded_wallet)

        wbnb_spent = wbnb_before - wbnb_after
        usdt_received = usdt_after - usdt_before

        assert wbnb_spent == expected_wbnb_spent, (
            f"WBNB spent mismatch. Expected: {expected_wbnb_spent}, Got: {wbnb_spent}"
        )
        assert usdt_received > 0, "Must receive positive USDT (no-op guard)"

        print(f"WBNB spent:   {format_token_amount(wbnb_spent, in_decimals)}")
        print(f"USDT received: {format_token_amount(usdt_received, out_decimals)}")
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
        token_in = tokens["USDT"]
        token_out = tokens["WBNB"]

        usdt_balance = get_token_balance(web3, token_in, funded_wallet)
        wbnb_before = get_token_balance(web3, token_out, funded_wallet)
        assert usdt_balance > 0, "funded_wallet must hold USDT for this test"
        in_decimals = get_token_decimals(web3, token_in)
        balance_decimal = Decimal(usdt_balance) / Decimal(10**in_decimals)

        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'=' * 80}")
        print("Test: Uniswap V4 SwapIntent with Insufficient Balance on BNB Chain")
        print(f"{'=' * 80}")
        print(f"Balance:   {balance_decimal} USDT")
        print(f"Trying:    {excessive_amount} USDT")

        intent = SwapIntent(
            from_token="USDT",
            to_token="WBNB",
            amount=excessive_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v4",
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

        # Layer 2: Execution (should fail)
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert not execution_result.success, "Execution should fail with insufficient balance"
        print(f"Execution failed as expected: {execution_result.error}")

        # Layer 3 (negative-path): receipt-parser must NOT see a successful swap.
        # Asserts the failure was a real revert rather than a silent no-op
        # (the V4 no-op bug class: TX status=1 but zero tokens move).
        #
        # Note: V4 insufficient-balance failures are caught at preflight (the
        # DirectSimulator rejects the bundle before submission), so
        # transaction_results may be empty. The loop is conditional on a
        # receipt being present — if a receipt IS produced (on-chain revert
        # path), the parser must NOT decode a successful swap; if no receipt
        # is produced (preflight rejection path), the bilateral conservation
        # check below already proves zero token movement, which is the same
        # invariant Layer 3 protects on the on-chain path.
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                assert parse_result.swap_result is None, (
                    "Failed swap must not produce a successful swap parse result "
                    f"(got {parse_result.swap_result})"
                )

        # Layer 4: Bilateral balance conservation — BOTH tokens must be unchanged
        usdt_after = get_token_balance(web3, token_in, funded_wallet)
        wbnb_after = get_token_balance(web3, token_out, funded_wallet)
        assert usdt_after == usdt_balance, "Input token balance must be unchanged after failed swap"
        assert wbnb_after == wbnb_before, "Output token balance must be unchanged after failed swap"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
