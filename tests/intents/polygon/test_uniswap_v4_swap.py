"""Production-grade SwapIntent tests for Uniswap V4 on Polygon.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler (routes to V4 adapter)
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using UniswapV4ReceiptParser
5. Verify balances changed correctly

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

Pool verification (probed against polygon mainnet 2026-05-14):
- WETH/USDC fee=3000 ts=60 sqrtPriceX96=1.667e33 liquidity=2.9e14 (sufficient
  for 100 USDC swaps at default fee tier the V4 adapter selects).
- WETH/USDC fee=500 ts=10 also initialized with deeper liquidity (5.1e15)
  but the V4 adapter defaults to fee=3000, so the canonical 0.3% pool is
  what these tests will exercise.
- V4 Quoter `quoteExactInputSingle` returns a valid 0.0438 WETH quote for
  100 USDC against the same fee=3000 pool. LP_OPEN on the same pool passes
  (VIB-4363, ae8f29057). So the V4 pool is functional.

Status (2026-05-14): both happy-path tests are marked ``xfail(strict=True)``
under VIB-4413. The SDK's UR-mediated V4 swap path reverts on-chain for
ERC20<>ERC20 pairs on Polygon (USDC<>WETH, USDC<>USDT, ...). USDC<>WPOL
works because WPOL is the chain's wrapped native and the SDK routes
through the NATIVE-currency pool key (different code path). Once the SDK
fix lands the ``xfail`` flip to ``xpass`` and ``strict=True`` will turn
that into a CI failure, surfacing the fix.

To run:
    uv run pytest tests/intents/polygon/test_uniswap_v4_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.uniswap_v4.receipt_parser import UniswapV4ReceiptParser
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

pytestmark = pytest.mark.no_zodiac(reason="VIB-4343: uniswap_v4 not yet in synthetic_intents matrix")

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "polygon"


# =============================================================================
# SwapIntent Tests — Uniswap V4 on Polygon
# =============================================================================


@pytest.mark.polygon
@pytest.mark.swap
class TestUniswapV4SwapIntent:
    """Test Uniswap V4 swaps using SwapIntent on Polygon.

    These tests verify the full Intent flow:
    - SwapIntent creation with protocol="uniswap_v4"
    - IntentCompiler routes to UniswapV4Adapter
    - Transactions execute successfully on-chain via UniversalRouter
    - UniswapV4ReceiptParser correctly interprets PoolManager Swap events
    - Balance changes match expected amounts
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason=(
            "VIB-4413: UniswapV4SDK.build_swap_tx emits calldata that reverts "
            "on-chain for ERC20<>ERC20 V4 swaps via UniversalRouter on Polygon "
            "(WETH/USDC pool exists and works for LP_OPEN; Quoter returns a "
            "valid quote; only the UR swap path reverts). Same SDK path passes "
            "on optimism / arbitrum / base / ethereum. Trace shows UR's "
            "unlockCallback reverts immediately after reading "
            "currencyDelta[UR][NATIVE]=0 from PoolManager transient storage. "
            "Likely a missing UR command for the all-ERC20 case on a chain "
            "where WETH is not the wrapped native (as of 2026-05-14)."
        ),
        strict=True,
    )
    async def test_swap_usdc_to_weth_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC -> WETH swap using SwapIntent via Uniswap V4 on Polygon.

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle
        2. Execution: ExecutionOrchestrator -> success
        3. Receipt Parsing: UniswapV4ReceiptParser -> swap amounts > 0
        4. Balance Deltas: USDC spent == swap amount, WETH received > 0
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("100")  # 100 USDC

        print(f"\n{'=' * 80}")
        print("Test: USDC -> WETH Swap via Uniswap V4 on Polygon")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} USDC")

        # Record balances before
        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)

        print(f"USDC before: {format_token_amount(usdc_before, in_decimals)}")
        print(f"WETH before: {format_token_amount(weth_before, out_decimals)}")

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
            to_token="WETH",
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

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
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
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        weth_received = weth_after - weth_before

        print("\n--- Balance Deltas ---")
        print(f"USDC spent:    {format_token_amount(usdc_spent, in_decimals)}")
        print(f"WETH received: {format_token_amount(weth_received, out_decimals)}")

        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must equal swap amount. Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )
        assert weth_received > 0, "Must receive positive WETH (no-op guard)"

        print("\nALL 4 LAYERS PASSED")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        reason=(
            "VIB-4413: UniswapV4SDK.build_swap_tx emits calldata that reverts "
            "on-chain for ERC20<>ERC20 V4 swaps via UniversalRouter on Polygon "
            "(reverse direction same failure as USDC -> WETH) (as of 2026-05-14)."
        ),
        strict=True,
    )
    async def test_swap_weth_to_usdc_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test WETH -> USDC swap using SwapIntent via Uniswap V4 on Polygon (reverse direction).

        4-Layer Verification:
        1. Compilation: IntentCompiler -> SUCCESS with ActionBundle
        2. Execution: ExecutionOrchestrator -> success
        3. Receipt Parsing: UniswapV4ReceiptParser -> swap amounts > 0
        4. Balance Deltas: WETH spent == swap amount, USDC received > 0
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["WETH"]
        token_out = tokens["USDC"]

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("0.05")  # 0.05 WETH

        print(f"\n{'=' * 80}")
        print("Test: WETH -> USDC Swap via Uniswap V4 on Polygon")
        print(f"{'=' * 80}")

        weth_before = get_token_balance(web3, token_in, funded_wallet)
        usdc_before = get_token_balance(web3, token_out, funded_wallet)

        # Fast-fail funding adequacy check — surfaces fixture funding problems
        # immediately instead of failing later in execution or delta verification.
        expected_weth_spent = int(swap_amount * Decimal(10**in_decimals))
        assert weth_before >= expected_weth_spent, (
            f"funded_wallet must hold >= {swap_amount} WETH to run this test "
            f"(have {format_token_amount(weth_before, in_decimals)})"
        )

        # Layer 1: Compilation
        intent = SwapIntent(
            from_token="WETH",
            to_token="USDC",
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
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
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
        weth_after = get_token_balance(web3, token_in, funded_wallet)
        usdc_after = get_token_balance(web3, token_out, funded_wallet)

        weth_spent = weth_before - weth_after
        usdc_received = usdc_after - usdc_before

        assert weth_spent == expected_weth_spent, (
            f"WETH spent mismatch. Expected: {expected_weth_spent}, Got: {weth_spent}"
        )
        assert usdc_received > 0, "Must receive positive USDC (no-op guard)"

        print(f"WETH spent:    {format_token_amount(weth_spent, in_decimals)}")
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
        token_out = tokens["WETH"]

        usdc_balance = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)
        assert usdc_balance > 0, "funded_wallet must hold USDC for this test"
        in_decimals = get_token_decimals(web3, token_in)
        balance_decimal = Decimal(usdc_balance) / Decimal(10**in_decimals)

        excessive_amount = balance_decimal * Decimal("100")

        print(f"\n{'=' * 80}")
        print("Test: Uniswap V4 SwapIntent with Insufficient Balance on Polygon")
        print(f"{'=' * 80}")
        print(f"Balance:   {balance_decimal} USDC")
        print(f"Trying:    {excessive_amount} USDC")

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
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
        parser = UniswapV4ReceiptParser(chain=CHAIN_NAME)
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                assert parse_result.swap_result is None, (
                    f"Failed swap must not produce a successful swap parse result (got {parse_result.swap_result})"
                )

        # Layer 4: Bilateral balance conservation — BOTH tokens must be unchanged
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)
        assert usdc_after == usdc_balance, "Input token balance must be unchanged after failed swap"
        assert weth_after == weth_before, "Output token balance must be unchanged after failed swap"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
