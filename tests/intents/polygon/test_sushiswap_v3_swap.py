"""Production-grade SwapIntent tests for SushiSwap V3 on Polygon.

VIB-4307 / VIB-4298 Phase 2: backfill ``(sushiswap_v3, SWAP, polygon)``
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
    uv run pytest tests/intents/polygon/test_sushiswap_v3_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.sushiswap_v3.receipt_parser import SushiSwapV3ReceiptParser
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

CHAIN_NAME = "polygon"

# WETH/USDC.e 0.05% fee tier — Polygon SushiSwap V3 liquidity is concentrated
# in USDC.e (bridged) pairs; native USDC (circles 2024 deployment) has minimal
# pool depth and any 100 USDC->WETH attempt hits a 99%+ price-impact guard.
# Matches the LP test's fee choice (see ``test_sushiswap_v3_lp.py``).
SWAP_FEE_TIER = 500


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.polygon
@pytest.mark.swap
class TestSushiSwapV3SwapIntent:
    """Test SushiSwap V3 swaps using SwapIntent on Polygon."""

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_usdc_to_weth_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """USDC.e -> WETH swap via SwapIntent on SushiSwap V3 (Polygon).

        4-layer verification (compile / execute / parse / balance deltas).
        Uses USDC.e because native USDC pools on polygon sushiswap_v3 lack
        liquidity (Circle's native USDC was deployed in 2024; LPs have not
        migrated from USDC.e on polygon's sushi v3).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC.e"]
        token_out = tokens["WETH"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "sushiswap_v3", token_in, token_out, SWAP_FEE_TIER)

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("100")  # 100 USDC.e

        print(f"\n{'=' * 80}")
        print("Test: USDC.e -> WETH Swap via SwapIntent (SushiSwap V3 / Polygon)")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} USDC.e (fee tier {SWAP_FEE_TIER / 10_000}%)")

        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)

        # Fail fast if funding fixture did not seed USDC.e — otherwise an
        # infra/slot regression would surface later as a confusing compile or
        # revert error.
        expected_usdc_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdc_before >= expected_usdc_spent, (
            f"funded_wallet has insufficient USDC.e ({usdc_before} < "
            f"{expected_usdc_spent} base units). Funding fixture failed or "
            f"balance slot config is wrong."
        )

        intent = SwapIntent(
            from_token="USDC.e",
            to_token="WETH",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="sushiswap_v3",
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
        assert compilation_result.action_bundle is not None

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

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
            "Layer 3 contract: at least one transaction must emit a "
            "SushiSwap V3 Swap event."
        )

        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        weth_received = weth_after - weth_before

        print(f"USDC spent:    {format_token_amount(usdc_spent, in_decimals)}")
        print(f"WETH received: {format_token_amount(weth_received, out_decimals)}")

        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must EXACTLY equal swap amount. "
            f"Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )
        assert weth_received > 0, "Must receive positive WETH (no-op guard)"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
