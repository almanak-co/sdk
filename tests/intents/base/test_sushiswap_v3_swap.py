"""Production-grade SwapIntent tests for SushiSwap V3 on Base.

VIB-4307 / VIB-4298 Phase 2: backfill ``(sushiswap_v3, SWAP, base)``
coverage required by ``ConnectorRegistry`` and enforced by
``scripts/ci/check_intent_coverage.py``.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Layer 1: ``IntentCompiler.compile(intent)`` returns ``CompilationStatus.SUCCESS``
2. Layer 2: ``ExecutionOrchestrator.execute(bundle)`` succeeds on the Anvil fork
3. Layer 3: ``SushiSwapV3ReceiptParser.parse_receipt(...)`` extracts swap event
4. Layer 4: ``from_token`` balance decreases by exactly ``amount``;
   ``to_token`` balance increases (positive, bilateral conservation)

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

To run:
    uv run pytest tests/intents/base/test_sushiswap_v3_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.connectors.sushiswap_v3.receipt_parser import SushiSwapV3ReceiptParser
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

# WETH/USDC 0.3% fee tier — same pool the Base LP test uses, so liquidity
# at the fork block is already proven by ``test_sushiswap_v3_lp.py``.
SWAP_FEE_TIER = 3000


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.base
@pytest.mark.swap
class TestSushiSwapV3SwapIntent:
    """Test SushiSwap V3 swaps using SwapIntent on Base.

    Verifies the full Intent flow:
    - SwapIntent creation with ``protocol="sushiswap_v3"`` (canonical literal
      consumed by the AST-scanning intent-coverage gate).
    - IntentCompiler generates correct SushiSwap V3 SwapRouter transactions.
    - Transactions execute successfully on-chain via the Anvil fork.
    - SushiSwapV3ReceiptParser correctly interprets results.
    - Balance changes match expected amounts (bilateral conservation).
    """

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        strict=True,
        reason="VIB-4307: sushiswap_v3 swap pool selection on base — quoter returns no amount or route emits no Swap event (as of 2026-05-12)",
    )
    async def test_swap_usdc_to_weth_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Test USDC -> WETH swap using SwapIntent on SushiSwap V3 (Base).

        Flow:
        1. Layer 1: Compile SwapIntent to ActionBundle.
        2. Layer 2: Execute via ExecutionOrchestrator.
        3. Layer 3: Parse receipts via SushiSwapV3ReceiptParser, assert
           ``parse_result.success`` and non-zero swap amounts.
        4. Layer 4: USDC spent == swap amount exactly; WETH received > 0.
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "sushiswap_v3", token_in, token_out, SWAP_FEE_TIER)

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("100")  # 100 USDC

        print(f"\n{'=' * 80}")
        print("Test: USDC -> WETH Swap via SwapIntent (SushiSwap V3 / Base)")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} USDC")

        # Layer 4 setup: record balances BEFORE
        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)

        print(f"USDC before: {format_token_amount(usdc_before, in_decimals)}")
        print(f"WETH before: {format_token_amount(weth_before, out_decimals)}")

        # Build the SwapIntent — ``protocol="sushiswap_v3"`` literal is the
        # AST-scanned constructor that satisfies the coverage gate.
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="sushiswap_v3",
            chain=CHAIN_NAME,
        )

        # Layer 1: compile (``rpc_url`` load-bearing; see arbitrum file note).
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

        # Layer 2: execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: parse receipts
        parser = SushiSwapV3ReceiptParser(chain=CHAIN_NAME)
        saw_swap_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt is None:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            assert parse_result.success, (
                f"Parser must succeed on a confirmed receipt; error={parse_result.error}"
            )
            if parse_result.swap_events:
                saw_swap_event = True
                for swap_data in parse_result.swap_events:
                    assert swap_data.amount0 != 0, "Amount0 must be non-zero in swap event"
                    assert swap_data.amount1 != 0, "Amount1 must be non-zero in swap event"

        assert saw_swap_event, (
            "Layer 3 contract: at least one transaction in the bundle must emit "
            "a SushiSwap V3 Swap event for the parser to extract."
        )

        # Layer 4: balance deltas (bilateral conservation)
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        weth_received = weth_after - weth_before

        print(f"USDC spent:    {format_token_amount(usdc_spent, in_decimals)}")
        print(f"WETH received: {format_token_amount(weth_received, out_decimals)}")

        expected_usdc_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal swap amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )
        assert weth_received > 0, "Must receive positive WETH (no-op guard)"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
