"""4-layer SwapIntent test for Camelot (Algebra V3) on Arbitrum Anvil fork.

Restores intent-coverage for ``(camelot, SWAP, arbitrum)`` after the
Phase-2 connector fold promoted ``protocol="camelot"`` from the monolithic
``_compile_default_router_swap_body`` into a dedicated
``CamelotCompiler`` (``almanak/framework/connectors/camelot/compiler.py``).

Camelot is the Arbitrum-native Algebra V1.9 fork: the SwapRouter exposes
``exactInputSingle`` without the Uniswap V3 ``fee`` parameter (fees are set
dynamically by the pool). ``CamelotCompiler`` subclasses ``UniswapV3Compiler``
and produces the Algebra-shaped calldata while otherwise reusing the V3
swap path. The connector is SWAP-only — LP / collect-fees stubs fail closed
(see ``docs/internal/plans/camelot-compiler-connector-folding-plan.md``).

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:

1. Layer 1: ``IntentCompiler.compile(intent)`` returns
   ``CompilationStatus.SUCCESS`` (routes through ``CamelotCompiler``).
2. Layer 2: ``ExecutionOrchestrator.execute(bundle)`` succeeds against the
   Camelot SwapRouter on the Anvil fork.
3. Layer 3: ``UniswapV3ReceiptParser`` extracts a ``Swap`` event from the
   Camelot pool log. Algebra V3 pools emit ``Swap(address indexed sender,
   address indexed recipient, int256 amount0, int256 amount1, uint160 price,
   uint128 liquidity, int24 tick)`` — identical parameter types to Uniswap V3,
   so the keccak topic hash is identical and the V3 parser is the canonical
   semantic-equivalent for Camelot pool events (consistent with
   ``CamelotCompiler`` subclassing ``UniswapV3Compiler``).
4. Layer 4: ``from_token`` balance decreases by exactly ``amount``;
   ``to_token`` balance increases (bilateral conservation).

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

To run::

    uv run pytest tests/intents/arbitrum/test_camelot_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser
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

CHAIN_NAME = "arbitrum"

# Camelot V3 (Algebra V1.9) has dynamic fees — no fixed fee-tier parameter at
# the router. The fee is determined per-pool by Algebra. WETH/USDC is the
# canonical Arbitrum pair on Camelot.


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.arbitrum
@pytest.mark.swap
class TestCamelotSwapIntent:
    """Test Camelot (Algebra V3) swaps using SwapIntent.

    Verifies the full Intent flow:

    - ``SwapIntent`` creation with ``protocol="camelot"``.
    - ``IntentCompiler`` dispatches to ``CamelotCompiler`` (via the
      ``compiler_registry`` lookup in ``_compile_swap``), which produces
      Algebra-shaped ``exactInputSingle`` calldata for the Camelot SwapRouter.
    - The transaction executes successfully on-chain via the Anvil fork
      under default-on Zodiac (Safe + Roles + execTransactionWithRole).
    - The Camelot pool emits a V3-shaped ``Swap`` event that
      ``UniswapV3ReceiptParser`` parses with non-zero amount0/amount1.
    - Wallet balances change exactly: ``from_token`` decreases by the swap
      amount, ``to_token`` increases.
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
        """Test USDC -> WETH swap using SwapIntent on Camelot (Algebra V3).

        Flow:

        1. Layer 1: Compile SwapIntent to ActionBundle via CamelotCompiler.
        2. Layer 2: Execute via ExecutionOrchestrator on the Anvil fork.
        3. Layer 3: Parse receipts via UniswapV3ReceiptParser (Algebra V3
           emits V3-shaped Swap events), assert ``parse_result.success``
           and non-zero swap amounts.
        4. Layer 4: USDC spent == swap amount exactly; WETH received > 0.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("100")  # 100 USDC

        print(f"\n{'=' * 80}")
        print("Test: USDC -> WETH Swap via SwapIntent (Camelot / Algebra V3)")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} USDC")

        # Layer 4 setup: record balances BEFORE
        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)

        # Fail fast if the chain fixture didn't fund the wallet with USDC —
        # otherwise compile/execute will fail much later with a confusing
        # "insufficient balance" / "approval failed" error that masks the
        # real fixture-setup problem.
        expected_usdc_in = int(swap_amount * Decimal(10**in_decimals))
        assert usdc_before >= expected_usdc_in, (
            f"funded_wallet must hold at least {swap_amount} USDC for this test; "
            f"got {format_token_amount(usdc_before, in_decimals)}. "
            f"Check the arbitrum conftest's wallet-funding fixture."
        )

        print(f"USDC before: {format_token_amount(usdc_before, in_decimals)}")
        print(f"WETH before: {format_token_amount(weth_before, out_decimals)}")

        # Build the SwapIntent — ``protocol="camelot"`` is the gate-coverage
        # attribution kwarg the intent-coverage gate scans for. Without it,
        # the gate cannot attribute this test to the
        # ``(camelot, SWAP, arbitrum)`` registry triple.
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="camelot",
            chain=CHAIN_NAME,
        )

        print(f"\nCreated SwapIntent: {intent.from_token} -> {intent.to_token}, amount={intent.amount}")

        # Layer 1: compile.
        #
        # ``rpc_url=orchestrator.rpc_url`` is load-bearing for Camelot: the
        # Algebra quoter (``compiler_constants.SWAP_QUOTER_ADDRESSES["arbitrum"]["camelot"]``)
        # is called to populate the expected output and validate the
        # min-output slippage guard. Without an RPC pointed at the same fork
        # the orchestrator executes against, the compiler's quote diverges
        # from on-chain state and the swap reverts on slippage.
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

        print(
            f"ActionBundle created with "
            f"{len(compilation_result.action_bundle.transactions)} transactions"
        )

        # Layer 2: execute
        print("\nExecuting via ExecutionOrchestrator...")
        execution_result = await orchestrator.execute(compilation_result.action_bundle)

        assert execution_result.success, f"Execution failed: {execution_result.error}"
        print(f"Execution successful! {len(execution_result.transaction_results)} transactions confirmed")

        # Layer 3: parse receipts.
        #
        # Camelot has no dedicated receipt parser — Algebra V3 pools emit a
        # ``Swap`` event with identical parameter types to Uniswap V3
        # (``address,address,int256,int256,uint160,uint128,int24``), so the
        # keccak topic hash is identical and ``UniswapV3ReceiptParser`` is
        # the semantic-equivalent parser. This mirrors the connector's own
        # ``CamelotCompiler(UniswapV3Compiler)`` subclass relationship.
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        saw_swap_event = False
        for i, tx_result in enumerate(execution_result.transaction_results):
            print(f"\nTransaction {i + 1}:")
            print(f"  Hash: {tx_result.tx_hash[:16]}...")
            print(f"  Gas used: {tx_result.gas_used}")

            if tx_result.receipt is None:
                continue

            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parse_result.success, (
                f"Parser must succeed on a confirmed receipt; error={parse_result.error}"
            )

            if parse_result.swap_events:
                saw_swap_event = True
                for swap_data in parse_result.swap_events:
                    print(f"  Amount0: {swap_data.amount0}")
                    print(f"  Amount1: {swap_data.amount1}")
                    print(f"  Pool:    {swap_data.pool_address[:16]}...")
                    # The Algebra V3 pool emits one signed amount0 and one
                    # signed amount1; both must be non-zero on a real swap.
                    assert swap_data.amount0 != 0, "Amount0 must be non-zero in swap event"
                    assert swap_data.amount1 != 0, "Amount1 must be non-zero in swap event"

        assert saw_swap_event, (
            "Layer 3 contract: at least one transaction in the bundle must emit "
            "an Algebra V3 Swap event (V3-topic-compatible) for the parser to extract."
        )

        # Layer 4: balance deltas (bilateral conservation)
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        weth_received = weth_after - weth_before

        print("\n--- Results ---")
        print(f"USDC spent:    {format_token_amount(usdc_spent, in_decimals)}")
        print(f"WETH received: {format_token_amount(weth_received, out_decimals)}")

        # Same scaled int as the fixture-quality assertion above.
        assert usdc_spent == expected_usdc_in, (
            f"USDC spent must EXACTLY equal swap amount. "
            f"Expected: {expected_usdc_in}, Got: {usdc_spent}"
        )
        assert weth_received > 0, "Must receive positive WETH"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
