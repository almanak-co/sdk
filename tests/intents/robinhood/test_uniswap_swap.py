"""Production-grade SwapIntent tests for Uniswap V3 on Robinhood Chain (4663).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline; default-on Zodiac
   routes through Safe + Roles + execTransactionWithRole)
4. Parse receipts using UniswapV3ReceiptParser
5. Verify balances changed correctly (bilateral deltas)

Pair: WETH/USDG fee 500 — the chain's primary liquid V3 pool
(0x69BfaF19C9f377BB306a89aEd9F6B07e2c1a8d9a, ~$3.5M TVL @ fork block 5,610,000).
USDG (Global Dollar, 6 dec) is the chain's canonical stable; there is NO
Circle-USDC / Tether-USDT with real liquidity on 4663.

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

To run:
    uv run pytest tests/intents/robinhood/test_uniswap_swap.py -v -s
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
    SWAP_MAX_SLIPPAGE,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)
from tests.intents.pool_helpers import fail_if_v3_pool_missing

# =============================================================================
# Test Configuration
# =============================================================================

CHAIN_NAME = "robinhood"
# Primary liquid pool: WETH/USDG fee tier 500.
POOL_FEE_TIER = 500


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.robinhood
@pytest.mark.swap
class TestUniswapV3SwapIntent:
    """Test Uniswap V3 swaps using SwapIntent on Robinhood Chain.

    Verifies the full Intent flow:
    - SwapIntent creation with proper parameters
    - IntentCompiler generates correct Uniswap V3 transactions
    - Transactions execute successfully on-chain
    - UniswapV3ReceiptParser correctly interprets results
    - Balance changes match expected amounts
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_usdg_to_weth_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDG -> WETH swap using SwapIntent."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDG"]
        token_out = tokens["WETH"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "uniswap_v3", token_in, token_out, POOL_FEE_TIER)

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("100")  # 100 USDG

        print(f"\n{'=' * 80}")
        print("Test: USDG -> WETH Swap via SwapIntent (Robinhood)")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} USDG")

        # Record balances before
        usdg_before = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)

        print(f"USDG before: {format_token_amount(usdg_before, in_decimals)}")
        print(f"WETH before: {format_token_amount(weth_before, out_decimals)}")

        # Create SwapIntent
        intent = SwapIntent(
            from_token="USDG",
            to_token="WETH",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )

        # Compile
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        # Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Parse receipts (Layer 3) - assert decoded swap amounts > 0
        from almanak.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser

        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        decoded_amount_in = Decimal("0")
        decoded_amount_out = Decimal("0")
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                assert parse_result.success, (
                    f"Receipt parser failed on tx {tx_result.tx_hash}: {parse_result.error}"
                )
                if parse_result.swap_result:
                    if parse_result.swap_result.amount_in_decimal > decoded_amount_in:
                        decoded_amount_in = parse_result.swap_result.amount_in_decimal
                    if parse_result.swap_result.amount_out_decimal > decoded_amount_out:
                        decoded_amount_out = parse_result.swap_result.amount_out_decimal

        assert decoded_amount_in > 0, (
            "Layer 3: UniswapV3ReceiptParser must decode amount_in > 0 from the swap receipt"
        )
        assert decoded_amount_out > 0, (
            "Layer 3: UniswapV3ReceiptParser must decode amount_out > 0 from the swap receipt"
        )

        # Verify balance changes (Layer 4 - bilateral)
        usdg_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)

        usdg_spent = usdg_before - usdg_after
        weth_received = weth_after - weth_before

        print(f"USDG spent:    {format_token_amount(usdg_spent, in_decimals)}")
        print(f"WETH received: {format_token_amount(weth_received, out_decimals)}")

        expected_usdg_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdg_spent == expected_usdg_spent, (
            f"USDG spent must equal swap amount. Expected: {expected_usdg_spent}, Got: {usdg_spent}"
        )
        assert weth_received > 0, "Must receive positive WETH (no-op guard)"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_weth_to_usdg_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test WETH -> USDG swap using SwapIntent (reverse direction)."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["WETH"]
        token_out = tokens["USDG"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "uniswap_v3", token_in, token_out, POOL_FEE_TIER)

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("0.05")  # 0.05 WETH

        print(f"\n{'=' * 80}")
        print("Test: WETH -> USDG Swap via SwapIntent (Robinhood)")
        print(f"{'=' * 80}")

        weth_before = get_token_balance(web3, token_in, funded_wallet)
        usdg_before = get_token_balance(web3, token_out, funded_wallet)

        intent = SwapIntent(
            from_token="WETH",
            to_token="USDG",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v3",
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

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: parse receipts and assert decoded swap amounts > 0
        from almanak.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser

        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        decoded_amount_in = Decimal("0")
        decoded_amount_out = Decimal("0")
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
                assert parse_result.success, (
                    f"Receipt parser failed on tx {tx_result.tx_hash}: {parse_result.error}"
                )
                if parse_result.swap_result:
                    if parse_result.swap_result.amount_in_decimal > decoded_amount_in:
                        decoded_amount_in = parse_result.swap_result.amount_in_decimal
                    if parse_result.swap_result.amount_out_decimal > decoded_amount_out:
                        decoded_amount_out = parse_result.swap_result.amount_out_decimal

        assert decoded_amount_in > 0, (
            "Layer 3: UniswapV3ReceiptParser must decode amount_in > 0 from the swap receipt"
        )
        assert decoded_amount_out > 0, (
            "Layer 3: UniswapV3ReceiptParser must decode amount_out > 0 from the swap receipt"
        )

        weth_after = get_token_balance(web3, token_in, funded_wallet)
        usdg_after = get_token_balance(web3, token_out, funded_wallet)

        weth_spent = weth_before - weth_after
        usdg_received = usdg_after - usdg_before

        expected_weth_spent = int(swap_amount * Decimal(10**in_decimals))
        assert weth_spent == expected_weth_spent, (
            f"WETH spent must equal swap amount. Expected: {expected_weth_spent}, Got: {weth_spent}"
        )
        assert usdg_received > 0, "Must receive positive USDG (no-op guard)"

        print(f"WETH spent:    {format_token_amount(weth_spent, in_decimals)}")
        print(f"USDG received: {format_token_amount(usdg_received, out_decimals)}")
        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_intent_with_insufficient_balance_fails(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test that SwapIntent with insufficient balance fails gracefully.

        Bilateral conservation: BOTH tokens must be unchanged after a failed
        swap. Accepts either compile-time OR execute-time failure, but narrowly:
        the failure message in either branch must be from the
        insufficient-balance / price-impact family — a permissive "any failure
        wins" check would mask unrelated regressions in what is meant to be a
        balance-guard test. This mirrors the Monad new-chain template.

        Direction note: this exercises the **WETH leg** (2x the seeded 10-WETH
        balance ≈ 20 WETH) rather than the USDG leg. USDG is seeded at 100,000
        and the WETH/USDG pool's quotable depth is far below a >100,000-USDG
        swap, so an over-balance USDG swap returns no quoter amount and the
        compiler emits a bundle that only reverts deep inside the pool's swap
        math — expensive to simulate against the rate-limited public-RPC fork.
        A ~20-WETH over-balance swap is quotable (or trips the price-impact
        guard at compile), so it fails fast and deterministically on either
        path. The scenario (insufficient balance → conservation) is identical
        across chains; only the leg is chain-appropriate.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["WETH"]
        token_out = tokens["USDG"]

        weth_balance = get_token_balance(web3, token_in, funded_wallet)
        usdg_before = get_token_balance(web3, token_out, funded_wallet)
        in_decimals = get_token_decimals(web3, token_in)
        balance_decimal = Decimal(weth_balance) / Decimal(10**in_decimals)

        # WETH funding produced a positive balance — fail loudly if the seeding
        # silently failed (would otherwise make this test vacuously pass).
        assert weth_balance > 0, (
            "WETH funding produced zero balance — investigate the WETH deposit()/"
            "storage-slot seeding in tests/intents/robinhood/conftest.py"
        )

        # Exceed balance by 2x so the swap fails as insufficient-balance. On the
        # deep WETH/USDG pool this either trips the compile-time price-impact
        # guard or reverts at execute time — both handled below.
        excessive_amount = balance_decimal * Decimal("2")

        print(f"\n{'=' * 80}")
        print("Test: SwapIntent with Insufficient Balance (Robinhood)")
        print(f"{'=' * 80}")
        print(f"Balance:   {balance_decimal} WETH")
        print(f"Trying:    {excessive_amount} WETH")

        intent = SwapIntent(
            from_token="WETH",
            to_token="USDG",
            amount=excessive_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )
        compilation_result = compiler.compile(intent)

        # Accept BOTH outcomes as valid insufficient-balance signals, but
        # narrowly — the failure message in EITHER branch must point to the
        # insufficient-balance / price-impact family (see docstring).
        _expected_phrases = (
            "insufficient balance",
            "insufficient funds",
            "insufficient",
            "transfer amount exceeds balance",
            "price impact",
            # Zodiac wraps inner reverts in ``ModuleTransactionFailed()``; the
            # inner reason isn't recoverable from the eth_call replay, so the
            # bilateral conservation check below is the load-bearing signal
            # under Zodiac.
            "exectransactionwithrole",
        )
        if compilation_result.status.value == "SUCCESS":
            assert compilation_result.action_bundle is not None
            execution_result = await orchestrator.execute(compilation_result.action_bundle)
            assert not execution_result.success, "Execution should fail with insufficient balance"
            exec_err = (execution_result.error or "").lower()
            assert any(p in exec_err for p in _expected_phrases), (
                f"Execution failed but not with an expected insufficient-balance signal: {execution_result.error!r}"
            )
            print(f"Execution failed as expected: {execution_result.error}")
        else:
            err = (compilation_result.error or "").lower()
            assert any(p in err for p in _expected_phrases), (
                f"Compilation failed but not with an expected insufficient-balance signal: {compilation_result.error!r}"
            )
            print(f"Compilation failed as expected: {compilation_result.error}")

        # Verify balances unchanged (bilateral conservation check — MANDATORY)
        weth_after = get_token_balance(web3, token_in, funded_wallet)
        usdg_after = get_token_balance(web3, token_out, funded_wallet)
        assert weth_after == weth_balance, "Input token balance must be unchanged after failed swap"
        assert usdg_after == usdg_before, "Output token balance must be unchanged after failed swap"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
