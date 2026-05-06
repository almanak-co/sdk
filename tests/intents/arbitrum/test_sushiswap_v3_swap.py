"""Production-grade SwapIntent tests for SushiSwap V3 on Arbitrum.

Restores manifest-coverage for ``(sushiswap_v3, SWAP)`` after PR #2123
(commit ``edf000e2``) deleted ``tests/intents/avalanche/test_sushiswap_v3_lp.py`` —
the only place the test suite constructed a ``SwapIntent(protocol="sushiswap_v3", ...)``.
Without that constructor the ``test_protocol_intent_has_onchain_case`` gate in
``tests/unit/permissions/test_onchain_case_coverage.py`` fails on ``main`` and
on every PR opened against it.

Arbitrum was chosen over the deleted Avalanche home because SushiSwap V3 is no
longer routed on Avalanche (VIB-2069: pool drained, ~54% price impact on $100,
on-chain reverts on $10) while Arbitrum still has a live deployment and an
existing ``test_sushiswap_v3_lp.py`` proving the WETH/USDC 0.3% pool has usable
liquidity at the fork block.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Layer 1: ``IntentCompiler.compile(intent)`` returns ``CompilationStatus.SUCCESS``
2. Layer 2: ``ExecutionOrchestrator.execute(bundle)`` succeeds on the Anvil fork
3. Layer 3: ``SushiSwapV3ReceiptParser.parse_receipt(...)`` extracts swap event
4. Layer 4: ``from_token`` balance decreases by exactly ``amount``;
   ``to_token`` balance increases (positive)

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

To run:
    uv run pytest tests/intents/arbitrum/test_sushiswap_v3_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.sushiswap_v3.receipt_parser import SushiSwapV3ReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
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

CHAIN_NAME = "arbitrum"

# WETH/USDC 0.3% fee tier — same pool the LP test exercises, so liquidity at
# the fork block is already proven.
SWAP_FEE_TIER = 3000


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.swap
class TestSushiSwapV3SwapIntent:
    """Test SushiSwap V3 swaps using SwapIntent.

    Verifies the full Intent flow:
    - SwapIntent creation with proper parameters and ``protocol="sushiswap_v3"``
    - IntentCompiler generates correct SushiSwap V3 SwapRouter transactions
    - Transactions execute successfully on-chain via the Anvil fork
    - SushiSwapV3ReceiptParser correctly interprets results
    - Balance changes match expected amounts (bilateral conservation)
    """

    @pytest.mark.asyncio
    async def test_swap_usdc_to_weth_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC -> WETH swap using SwapIntent on SushiSwap V3.

        Flow:
        1. Layer 1: Compile SwapIntent to ActionBundle
        2. Layer 2: Execute via ExecutionOrchestrator
        3. Layer 3: Parse receipts via SushiSwapV3ReceiptParser, assert
           ``parse_result.success`` and non-zero swap amounts
        4. Layer 4: USDC spent == swap amount exactly; WETH received > 0
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "sushiswap_v3", token_in, token_out, SWAP_FEE_TIER)

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("100")  # 100 USDC

        print(f"\n{'=' * 80}")
        print("Test: USDC -> WETH Swap via SwapIntent (SushiSwap V3)")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} USDC")

        # Layer 4 setup: record balances BEFORE
        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)

        print(f"USDC before: {format_token_amount(usdc_before, in_decimals)}")
        print(f"WETH before: {format_token_amount(weth_before, out_decimals)}")

        # Build the SwapIntent — this constructor satisfies the manifest-
        # coverage gate ``(sushiswap_v3, SWAP)`` and is the load-bearing
        # reason this file exists.
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="sushiswap_v3",
            chain=CHAIN_NAME,
        )

        print(f"\nCreated SwapIntent: {intent.from_token} -> {intent.to_token}, amount={intent.amount}")

        # Layer 1: compile.
        #
        # ``rpc_url=orchestrator.rpc_url`` is load-bearing: the V3 compiler
        # uses this RPC for auto fee-tier quoting and pool-state validation.
        # Without it, ``_get_chain_rpc_url()`` falls back to the default /
        # public Arbitrum RPC, which (a) may be unavailable or rate-limited
        # in CI and (b) is at a different chain state than the Anvil fork
        # the orchestrator executes against — both produce flaky failures
        # unrelated to SushiSwap coverage. Mirrors ``test_pancakeswap_v3_swap.py``.
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=orchestrator.rpc_url,
        )

        print("Compiling intent to ActionBundle...")
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        print(f"ActionBundle created with {len(compilation_result.action_bundle.transactions)} transactions")

        # Layer 2: execute
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: parse receipts
        parser = SushiSwapV3ReceiptParser(chain=CHAIN_NAME)
        saw_swap_event = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt is None:
                continue

            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parse_result.success, f"Parser must succeed on a confirmed receipt; error={parse_result.error}"

            if parse_result.swap_events:
                saw_swap_event = True
                for swap_data in parse_result.swap_events:
                    print(f"  Amount0: {swap_data.amount0}")
                    print(f"  Amount1: {swap_data.amount1}")
                    print(f"  Pool:    {swap_data.pool_address[:16]}...")
                    # The V3 pool emits one signed amount0 and one signed amount1;
                    # both must be non-zero on a real swap.
                    assert swap_data.amount0 != 0, "Amount0 must be non-zero in swap event"
                    assert swap_data.amount1 != 0, "Amount1 must be non-zero in swap event"

        assert saw_swap_event, (
            "Layer 3 contract: at least one transaction in the bundle must emit a "
            "SushiSwap V3 Swap event for the parser to extract."
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
        assert weth_received > 0, "Must receive positive WETH"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
