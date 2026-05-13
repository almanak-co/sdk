"""Production-grade SwapIntent tests for PancakeSwap V3 on Base.

VIB-4352 / VIB-4343 Phase 1a: backfill ``(pancakeswap_v3, SWAP, base)``
intent-test coverage. The registry edit (adding ``base`` to the
``pancakeswap_v3`` registry tuple in ``connectors/pancakeswap_v3/__init__.py``)
is deferred to a coordinated follow-up after sibling VIB-4353 also lands.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Layer 1: ``IntentCompiler.compile(intent)`` returns ``CompilationStatus.SUCCESS``
2. Layer 2: ``ExecutionOrchestrator.execute(bundle)`` succeeds on the Anvil fork
3. Layer 3: ``PancakeSwapV3ReceiptParser.parse_receipt(...)`` extracts swap event
4. Layer 4: ``from_token`` balance decreases by exactly ``amount``;
   ``to_token`` balance increases (positive, bilateral conservation)

Runs default-on Zodiac (Safe + Roles + ``execTransactionWithRole``) via the
per-chain conftest — no opt-out.

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

To run:
    uv run pytest tests/intents/base/test_pancakeswap_v3_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.pancakeswap_v3.receipt_parser import PancakeSwapV3ReceiptParser
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

CHAIN_NAME = "base"

# USDC/WETH on PancakeSwap V3 Base — fee=100 holds the deepest liquidity at
# the fork block and is the tier the compiler's USDC/wrapped-native heuristic
# selects (compiler_adapters.py:508). Validated 2026-05-13 against on-chain
# PCS V3 factory ``0x0BFb...91865`` on Base mainnet.
POOL_FEE_TIER = 100


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.base
@pytest.mark.swap
class TestPancakeSwapV3SwapIntent:
    """Test PancakeSwap V3 swaps using SwapIntent on Base.

    Verifies the full Intent flow:
    - SwapIntent creation with ``protocol="pancakeswap_v3"`` (canonical
      literal consumed by the AST-scanning intent-coverage gate).
    - IntentCompiler generates correct PancakeSwap V3 SmartRouter
      transactions (Base SmartRouter ``0x678A...fa86``).
    - Transactions execute successfully on-chain via the Anvil fork.
    - PancakeSwapV3ReceiptParser correctly interprets results.
    - Balance changes match expected amounts (bilateral conservation).
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_usdc_to_weth_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC -> WETH swap using SwapIntent on PancakeSwap V3 (Base).

        Flow:
        1. Layer 1: Compile SwapIntent to ActionBundle.
        2. Layer 2: Execute via ExecutionOrchestrator.
        3. Layer 3: Parse receipts via PancakeSwapV3ReceiptParser, assert
           ``parse_result.success`` and non-zero swap amounts on a real swap.
        4. Layer 4: USDC spent == swap amount exactly; WETH received > 0.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "pancakeswap_v3", token_in, token_out, POOL_FEE_TIER)

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("100")  # 100 USDC

        print(f"\n{'=' * 80}")
        print("Test: USDC -> WETH Swap via SwapIntent (PancakeSwap V3 / Base)")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} USDC")

        # Layer 4 setup: record balances BEFORE
        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)

        # Fail fast on funding regression — a zero input balance here means
        # the per-chain seeding fixture is broken, not that the SUT is.
        # Surface the infra failure here rather than later as a confusing
        # compile / execute error.
        assert usdc_before > 0, (
            f"Funding regression: funded_wallet {funded_wallet} has 0 USDC; "
            "check tests/intents/base/conftest.py seeding."
        )

        print(f"USDC before: {format_token_amount(usdc_before, in_decimals)}")
        print(f"WETH before: {format_token_amount(weth_before, out_decimals)}")

        # Build the SwapIntent — ``protocol="pancakeswap_v3"`` literal is the
        # AST-scanned constructor that satisfies the coverage gate.
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="pancakeswap_v3",
            chain=CHAIN_NAME,
        )

        # Layer 1: compile. ``rpc_url`` is load-bearing — the compiler uses it
        # for the on-chain quoter pass in ``auto`` pool-selection mode.
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=orchestrator.rpc_url,
        )

        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 2: execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful: {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: parse receipts. At least one tx in the bundle must emit a
        # PancakeSwap V3 Swap event for the parser to extract — anything less
        # masks the V4-style no-op bug class where tx succeeds but 0 tokens
        # move (see intent-tests.md §Bilateral Balance Delta Assertions).
        parser = PancakeSwapV3ReceiptParser(chain=CHAIN_NAME)
        saw_swap_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt is None:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parse_result.success, (
                f"Parser must succeed on a confirmed receipt; error={parse_result.error}"
            )
            if parse_result.swaps:
                saw_swap_event = True
                for swap_data in parse_result.swaps:
                    assert swap_data.amount0 != 0, "Amount0 must be non-zero in swap event"
                    assert swap_data.amount1 != 0, "Amount1 must be non-zero in swap event"

        assert saw_swap_event, (
            "Layer 3 contract: at least one transaction in the bundle must emit "
            "a PancakeSwap V3 Swap event for the parser to extract."
        )

        # Layer 4: balance deltas (bilateral conservation)
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        weth_received = weth_after - weth_before

        print("\n--- Results ---")
        print(f"USDC spent:    {format_token_amount(usdc_spent, in_decimals)}")
        print(f"WETH received: {format_token_amount(weth_received, out_decimals)}")

        expected_usdc_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal swap amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )
        assert weth_received > 0, "Must receive positive WETH (no-op guard)"

        print("\nALL CHECKS PASSED")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_weth_to_usdc_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test WETH -> USDC swap using SwapIntent on PancakeSwap V3 (reverse direction)."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["WETH"]
        token_out = tokens["USDC"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "pancakeswap_v3", token_in, token_out, POOL_FEE_TIER)

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("0.05")  # 0.05 WETH

        print(f"\n{'=' * 80}")
        print("Test: WETH -> USDC Swap via SwapIntent (PancakeSwap V3 / Base)")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} WETH")

        weth_before = get_token_balance(web3, token_in, funded_wallet)
        usdc_before = get_token_balance(web3, token_out, funded_wallet)

        # Fail fast on funding regression (mirror of the forward test).
        assert weth_before > 0, (
            f"Funding regression: funded_wallet {funded_wallet} has 0 WETH; "
            "check tests/intents/base/conftest.py seeding."
        )

        intent = SwapIntent(
            from_token="WETH",
            to_token="USDC",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="pancakeswap_v3",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=orchestrator.rpc_url,
        )

        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: parse receipts and require a Swap event to surface
        parser = PancakeSwapV3ReceiptParser(chain=CHAIN_NAME)
        saw_swap_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt is None:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parse_result.success, (
                f"Parser must succeed on a confirmed receipt; error={parse_result.error}"
            )
            if parse_result.swaps:
                saw_swap_event = True
                for swap_data in parse_result.swaps:
                    assert swap_data.amount0 != 0, "Amount0 must be non-zero in swap event"
                    assert swap_data.amount1 != 0, "Amount1 must be non-zero in swap event"

        assert saw_swap_event, (
            "Layer 3 contract: at least one transaction in the bundle must emit "
            "a PancakeSwap V3 Swap event for the parser to extract."
        )

        # Layer 4: balance deltas (bilateral conservation)
        weth_after = get_token_balance(web3, token_in, funded_wallet)
        usdc_after = get_token_balance(web3, token_out, funded_wallet)

        weth_spent = weth_before - weth_after
        usdc_received = usdc_after - usdc_before

        expected_weth_spent = int(swap_amount * Decimal(10**in_decimals))
        assert weth_spent == expected_weth_spent, (
            f"WETH spent must EXACTLY equal swap amount. "
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
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test that SwapIntent with insufficient balance fails gracefully."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]

        # Get current balance
        usdc_balance = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)
        in_decimals = get_token_decimals(web3, token_in)

        # Fail fast on funding regression — a zero seeded balance here would
        # make the "exceed by 2x" amount equal to zero and the test would pass
        # vacuously, silently muting the balance-guard signal.
        assert usdc_balance > 0, (
            f"Funding regression: funded_wallet {funded_wallet} has 0 USDC; "
            "balance-guard test would be vacuously true. Check "
            "tests/intents/base/conftest.py seeding."
        )

        balance_decimal = Decimal(usdc_balance) / Decimal(10**in_decimals)

        # Exceed balance by 2x so execution fails on-chain with insufficient
        # balance, but stay inside the compiler's price-impact guard (default
        # 30%) so this path exercises execution-level failure rather than
        # compile-time rejection. See bnb test_pancakeswap_v3_swap.py for the
        # sibling pattern (issue #2150).
        excessive_amount = balance_decimal * Decimal("2")

        print(f"\n{'=' * 80}")
        print("Test: SwapIntent with Insufficient Balance (PancakeSwap V3 / Base)")
        print(f"{'=' * 80}")
        print(f"Balance:   {balance_decimal} USDC")
        print(f"Trying:    {excessive_amount} USDC")

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=excessive_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="pancakeswap_v3",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=orchestrator.rpc_url,
        )
        compilation_result = compiler.compile(intent)

        # Accept BOTH outcomes as valid insufficient-balance signals, but
        # narrowly — the failure message in EITHER branch must point to the
        # insufficient-balance / price-impact family. Anything else (router
        # errors, slippage misconfig, liquidity gaps) would be masked by a
        # permissive "any failure wins" check, hiding unrelated regressions
        # in what is meant to be a balance-guard test.
        #
        #   1) Compilation SUCCESS -> execution must fail with an
        #      insufficient-balance error (balance check trips at execute time).
        #   2) Compilation FAILED -> compiler error must contain a price-impact
        #      or insufficient-balance phrase (the compiler guard trips first
        #      because an excessive amount also tips the price-impact guard).
        #
        # Narrow phrases: bare "balance" would also match unrelated errors like
        # "balance check failed for token X"; use explicit failure shapes from
        # the insufficient-balance / price-impact family.
        # Acceptable failure signals — must point to the insufficient-balance
        # / price-impact family. Anything else (router errors, slippage
        # misconfig, liquidity gaps) would be masked by a permissive "any
        # failure wins" check, hiding unrelated regressions in what is meant
        # to be a balance-guard test.
        #
        # Under Zodiac, ``ZodiacOrchestrator`` may either:
        #   (a) Successfully decode the inner ``Error(string)`` payload
        #       (e.g. ``transfer amount exceeds balance``) via the eth_call
        #       replay and embed it as ``reason='...'`` in the wrapper
        #       message — in which case one of the inner-reason substrings
        #       below matches directly; OR
        #   (b) Fail to decode the inner reason (eth_call replay returns
        #       only ``ModuleTransactionFailed()`` / selector
        #       ``0xd27b44a9``) — in which case the only signal available
        #       is the Safe wrapper's typed-error selector. We accept that
        #       selector ONLY (not the bare ``execTransactionWithRole``
        #       wrapper string, which would also match unrelated reverts —
        #       Codex P2 review). The bilateral conservation check below
        #       remains the load-bearing signal under Zodiac in either case.
        _expected_phrases = (
            "insufficient balance",
            "insufficient funds",
            "transfer amount exceeds balance",
            "price impact",
            # Safe ``ModuleTransactionFailed()`` 4-byte selector — the inner
            # revert was wrapped but not decoded. Specific enough to NOT
            # match unrelated reverts (each Safe error has its own selector).
            "0xd27b44a9",
        )
        if compilation_result.status.value == "SUCCESS":
            assert compilation_result.action_bundle is not None
            execution_result = await orchestrator.execute(compilation_result.action_bundle)
            assert not execution_result.success, "Execution should fail with insufficient balance"
            exec_err = (execution_result.error or "").lower()
            assert any(p in exec_err for p in _expected_phrases), (
                f"Execution failed but not with an expected insufficient-balance signal: "
                f"{execution_result.error!r}"
            )
            print(f"Execution failed as expected: {execution_result.error}")
        else:
            err = (compilation_result.error or "").lower()
            assert any(p in err for p in _expected_phrases), (
                f"Compilation failed but not with an expected insufficient-balance signal: "
                f"{compilation_result.error!r}"
            )
            print(f"Compilation failed as expected: {compilation_result.error}")

        # Verify balances unchanged (bilateral conservation check)
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)
        assert usdc_after == usdc_balance, "Input token balance must be unchanged after failed swap"
        assert weth_after == weth_before, "Output token balance must be unchanged after failed swap"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
