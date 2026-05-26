"""Production-grade SwapIntent tests for Uniswap V3 on Monad (VIB-4350).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler
3. Execute via ExecutionOrchestrator (full production pipeline)
4. Parse receipts using UniswapV3ReceiptParser
5. Verify balances changed correctly

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

Pool selection rationale:
- The funded Monad wallet holds WMON (wrapped, 10 WMON) and USDC (storage-slot, 100k).
- WMON/USDC at fee=3000 is the deepest Uniswap V3 pool on Monad
  (~$536k USDC + ~33M WMON in pool 0x659bD0...4a9da, verified 2026-05-13).
- Mirrors arbitrum (USDC/WETH) and avalanche (USDC/WAVAX) test patterns where
  the wrapped-native token is the counterparty to USDC.

Note: Registry edit (adding "monad" to uniswap_v3 register_connector chains tuple)
is deferred to a coordinated follow-up after sibling VIB-4351 (LP test) lands.
This file only adds the SWAP test.

To run:
    uv run pytest tests/intents/monad/test_uniswap_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import IntentCompiler, SwapIntent
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

CHAIN_NAME = "monad"


@pytest.fixture(scope="module")
def price_oracle_monad_local(price_oracle_monad: dict[str, Decimal]) -> dict[str, Decimal]:
    """Module-scope alias for the session-wide Monad price oracle.

    Mirrors the pattern used by ``test_morpho_blue_lending.py`` and
    ``test_curvance_lending.py`` so the test body can read ``price_oracle``
    without depending on the chain-routing logic in the shared
    ``price_oracle`` fixture.
    """
    return price_oracle_monad


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.monad
@pytest.mark.swap
class TestUniswapV3SwapIntent:
    """Test Uniswap V3 swaps using SwapIntent on Monad.

    These tests verify the full Intent flow:
    - SwapIntent creation with proper parameters
    - IntentCompiler generates correct Uniswap V3 transactions
    - Transactions execute successfully on-chain (Anvil fork)
    - UniswapV3ReceiptParser correctly interprets results
    - Balance changes match expected amounts (bilateral deltas)
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_usdc_to_wmon_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle_monad_local: dict[str, Decimal],
    ):
        """Test USDC -> WMON swap using SwapIntent.

        Flow:
        1. Create SwapIntent for USDC -> WMON
        2. Compile to ActionBundle using IntentCompiler
        3. Execute via ExecutionOrchestrator
        4. Verify balances changed correctly (bilateral)
        """
        price_oracle = price_oracle_monad_local
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WMON"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "uniswap_v3", token_in, token_out, 3000)

        # Get decimals
        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        # Amount to swap
        swap_amount = Decimal("100")  # 100 USDC

        print(f"\n{'=' * 80}")
        print("Test: USDC -> WMON Swap via SwapIntent")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} USDC")

        # Record balances before
        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        wmon_before = get_token_balance(web3, token_out, funded_wallet)

        print(f"USDC before: {format_token_amount(usdc_before, in_decimals)}")
        print(f"WMON before: {format_token_amount(wmon_before, out_decimals)}")

        # Create SwapIntent
        intent = SwapIntent(
            from_token="USDC",
            to_token="WMON",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )

        print(f"\nCreated SwapIntent: {intent.from_token} -> {intent.to_token}, amount={intent.amount}")

        # Compile intent with real prices from CoinGecko
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )

        print("Compiling intent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # Execute via ExecutionOrchestrator
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Parse receipts (Layer 3) — at least one Uniswap V3 swap receipt MUST
        # parse cleanly with positive amounts. Tracking it explicitly (vs the
        # arbitrum/avalanche-style ``if parse_result.success`` silent guard)
        # surfaces a parser regression on Monad's pool log shape rather than
        # masking it behind a passing balance-delta check.
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        swap_parses_seen = 0
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")
            if not tx_result.receipt:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"
            if parse_result.swap_result is None:
                # Approval-only tx — no swap_result, but parsing still succeeded.
                continue
            assert parse_result.swap_result.amount_in_decimal > 0, "Receipt parser amount_in must be > 0"
            assert parse_result.swap_result.amount_out_decimal > 0, "Receipt parser amount_out must be > 0"
            assert parse_result.swap_result.effective_price > 0, "Receipt parser effective_price must be > 0"
            print(f"  Amount in:  {parse_result.swap_result.amount_in_decimal}")
            print(f"  Amount out: {parse_result.swap_result.amount_out_decimal}")
            print(f"  Price:      {parse_result.swap_result.effective_price}")
            swap_parses_seen += 1
        assert swap_parses_seen >= 1, "Expected at least one parseable swap receipt with positive amounts"

        # Verify balance changes (Layer 4 — bilateral deltas)
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        wmon_after = get_token_balance(web3, token_out, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        wmon_received = wmon_after - wmon_before

        print("\n--- Results ---")
        print(f"USDC spent:    {format_token_amount(usdc_spent, in_decimals)}")
        print(f"WMON received: {format_token_amount(wmon_received, out_decimals)}")

        # Verify USDC was spent (exact)
        expected_usdc_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must equal swap amount. Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )

        # Verify WMON was received (no-op guard)
        assert wmon_received > 0, "Must receive positive WMON (no-op guard)"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_wmon_to_usdc_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle_monad_local: dict[str, Decimal],
    ):
        """Test WMON -> USDC swap using SwapIntent (reverse direction)."""
        price_oracle = price_oracle_monad_local
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["WMON"]
        token_out = tokens["USDC"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "uniswap_v3", token_in, token_out, 3000)

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        # Monad conftest wraps 10 WMON; use 1 WMON to stay well inside that budget.
        swap_amount = Decimal("1.0")  # 1.0 WMON

        print(f"\n{'=' * 80}")
        print("Test: WMON -> USDC Swap via SwapIntent")
        print(f"{'=' * 80}")

        wmon_before = get_token_balance(web3, token_in, funded_wallet)
        usdc_before = get_token_balance(web3, token_out, funded_wallet)

        expected_wmon_spent = int(swap_amount * Decimal(10**in_decimals))
        assert wmon_before >= expected_wmon_spent, (
            f"funded_wallet has only {wmon_before} WMON wei, need >= {expected_wmon_spent} "
            f"({swap_amount} WMON) — Monad fork funding regression"
        )

        # Create intent
        intent = SwapIntent(
            from_token="WMON",
            to_token="USDC",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )

        # Compile with real prices from CoinGecko
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None

        # Execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Parse receipts (Layer 3) — at least one Uniswap V3 swap receipt MUST
        # parse cleanly with positive amounts. Same rationale as the forward
        # test above.
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        swap_parses_seen = 0
        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parse_result.success, f"Receipt parsing failed: {parse_result.error}"
            if parse_result.swap_result is None:
                continue
            assert parse_result.swap_result.amount_in_decimal > 0
            assert parse_result.swap_result.amount_out_decimal > 0
            assert parse_result.swap_result.effective_price > 0
            swap_parses_seen += 1
        assert swap_parses_seen >= 1, "Expected at least one parseable swap receipt with positive amounts"

        # Verify (Layer 4 — bilateral deltas)
        wmon_after = get_token_balance(web3, token_in, funded_wallet)
        usdc_after = get_token_balance(web3, token_out, funded_wallet)

        wmon_spent = wmon_before - wmon_after
        usdc_received = usdc_after - usdc_before

        assert wmon_spent == expected_wmon_spent, (
            f"WMON spent must equal swap amount. Expected: {expected_wmon_spent}, Got: {wmon_spent}"
        )
        assert usdc_received > 0, "Must receive positive USDC (no-op guard)"

        print(f"WMON spent:    {format_token_amount(wmon_spent, in_decimals)}")
        print(f"USDC received: {format_token_amount(usdc_received, out_decimals)}")
        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_intent_with_insufficient_balance_fails(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle_monad_local: dict[str, Decimal],
    ):
        """Test that SwapIntent with insufficient balance fails gracefully.

        Bilateral conservation: BOTH tokens must be unchanged after a failed swap.
        Accepts either compile-time or execute-time failure, but narrowly: the
        failure message in either branch must be from the insufficient-balance
        / price-impact family — a permissive "any failure wins" check would
        mask unrelated regressions in what is meant to be a balance-guard test.
        """
        price_oracle = price_oracle_monad_local
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WMON"]

        # Get current balance
        usdc_balance = get_token_balance(web3, token_in, funded_wallet)
        wmon_before = get_token_balance(web3, token_out, funded_wallet)
        in_decimals = get_token_decimals(web3, token_in)
        balance_decimal = Decimal(usdc_balance) / Decimal(10**in_decimals)

        # USDC is in CHAIN_CONFIGS["monad"]["tokens"] at slot 9 (verified
        # 2026-05-26 via eth_getStorageAt probe). A zero balance here means the
        # funded_wallet seeding silently failed — fail loudly instead of skipping
        # (VIB-4823).
        assert usdc_balance > 0, (
            "USDC funding produced zero balance — investigate "
            "CHAIN_CONFIGS['monad']['balance_slots']['USDC'] or anvil_setStorageAt"
        )

        # Exceed balance by 2x so execution fails on-chain with insufficient balance,
        # but stay inside the compiler's price-impact guard (default 30%) so this
        # path exercises execution-level failure rather than compile-time rejection.
        excessive_amount = balance_decimal * Decimal("2")

        print(f"\n{'=' * 80}")
        print("Test: SwapIntent with Insufficient Balance")
        print(f"{'=' * 80}")
        print(f"Balance:   {balance_decimal} USDC")
        print(f"Trying:    {excessive_amount} USDC")

        intent = SwapIntent(
            from_token="USDC",
            to_token="WMON",
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

        # Accept BOTH outcomes as valid insufficient-balance signals, but narrowly
        # — the failure message in EITHER branch must point to the
        # insufficient-balance / price-impact family. Anything else (router
        # errors, slippage misconfig, liquidity gaps) would be masked by a
        # permissive "any failure wins" check, hiding unrelated regressions in
        # what is meant to be a balance-guard test.
        #
        #   1) Compilation SUCCESS -> execution must fail with an
        #      insufficient-balance error (balance check trips at execute time).
        #   2) Compilation FAILED -> compiler error must contain a price-impact
        #      or insufficient-balance phrase (the compiler guard trips first
        #      because an excessive amount also tips the price-impact guard).
        _expected_phrases = (
            "insufficient balance",
            "insufficient funds",
            "insufficient",
            "transfer amount exceeds balance",
            "price impact",
            # Zodiac wraps inner reverts in ``ModuleTransactionFailed()``;
            # the inner reason isn't recoverable from the eth_call replay.
            # The bilateral conservation check below is the load-bearing
            # signal under Zodiac. EOA-mode error messages don't contain
            # ``execTransactionWithRole``, so this stays strict for EOA.
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
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        wmon_after = get_token_balance(web3, token_out, funded_wallet)
        assert usdc_after == usdc_balance, "Input token balance must be unchanged after failed swap"
        assert wmon_after == wmon_before, "Output token balance must be unchanged after failed swap"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
