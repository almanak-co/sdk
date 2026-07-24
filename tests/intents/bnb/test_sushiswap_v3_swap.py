"""Production-grade SwapIntent tests for SushiSwap V3 on BNB Chain.

VIB-4307 / VIB-4298 Phase 2: backfill ``(sushiswap_v3, SWAP, bnb)``
coverage required by ``ConnectorRegistry`` and enforced by
``scripts/ci/check_intent_coverage.py``.

Pair choice: USDT/WBNB at 0.3% — mirrors the LP test in this directory.
PancakeSwap V3 dominates BNB Chain DEX volume, so SushiSwap V3 pools
with USDC are sparse / illiquid; the USDT/WBNB 3000 pool is the same
liquidity venue the LP test already proves usable at the fork block.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Layer 1: ``IntentCompiler.compile(intent)`` returns ``CompilationStatus.SUCCESS``
2. Layer 2: ``ExecutionOrchestrator.execute(bundle)`` succeeds on the Anvil fork
3. Layer 3: ``SushiSwapV3ReceiptParser.parse_receipt(...)`` extracts swap event
4. Layer 4: ``from_token`` balance decreases by exactly ``amount``;
   ``to_token`` balance increases (positive, bilateral conservation)

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

To run:
    uv run pytest tests/intents/bnb/test_sushiswap_v3_swap.py -v -s
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

# ``CHAIN_NAME = "bsc"`` mirrors the rest of this directory: the SDK / contract
# registry uses ``bsc`` as the address-table key (see
# ``almanak/core/contracts.py:SUSHISWAP_V3``) while the directory itself is
# ``bnb`` (the canonical name used by ``ConnectorRegistry`` and the
# intent-coverage gate path attribution). Both names point at the same
# CHAIN_CONFIGS entry.
CHAIN_NAME = "bsc"

# USDT/WBNB 0.3% — the LP test proves liquidity at the fork block.
SWAP_FEE_TIER = 3000


# =============================================================================
# SwapIntent Tests
# =============================================================================


@pytest.mark.bnb
@pytest.mark.swap
class TestSushiSwapV3SwapIntent:
    """Test SushiSwap V3 swaps using SwapIntent on BNB Chain."""

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        strict=True,
        reason="VIB-5972: sushiswap_v3 swap pool selection on bnb — quoter returns no amount or route emits no Swap event (as of 2026-05-12; re-pointed to VIB-5972 2026-07-24)",
    )
    async def test_swap_usdt_to_wbnb_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """USDT -> WBNB swap via SwapIntent on SushiSwap V3 (BNB).

        4-layer verification (compile / execute / parse / balance deltas).
        """
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDT"]
        token_out = tokens["WBNB"]
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "sushiswap_v3", token_in, token_out, SWAP_FEE_TIER)

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("100")  # 100 USDT

        print(f"\n{'=' * 80}")
        print("Test: USDT -> WBNB Swap via SwapIntent (SushiSwap V3 / BNB)")
        print(f"{'=' * 80}")
        print(f"Swap amount: {swap_amount} USDT")

        usdt_before = get_token_balance(web3, token_in, funded_wallet)
        wbnb_before = get_token_balance(web3, token_out, funded_wallet)

        intent = SwapIntent(
            from_token="USDT",
            to_token="WBNB",
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

        usdt_after = get_token_balance(web3, token_in, funded_wallet)
        wbnb_after = get_token_balance(web3, token_out, funded_wallet)

        usdt_spent = usdt_before - usdt_after
        wbnb_received = wbnb_after - wbnb_before

        print(f"USDT spent:    {format_token_amount(usdt_spent, in_decimals)}")
        print(f"WBNB received: {format_token_amount(wbnb_received, out_decimals)}")

        expected_usdt_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdt_spent == expected_usdt_spent, (
            f"USDT spent must EXACTLY equal swap amount. "
            f"Expected: {expected_usdt_spent}, Got: {usdt_spent}"
        )
        assert wbnb_received > 0, "Must receive positive WBNB (no-op guard)"

        print("\nALL CHECKS PASSED")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
