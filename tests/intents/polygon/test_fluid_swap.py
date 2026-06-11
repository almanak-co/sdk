"""4-layer SwapIntent test for Fluid DEX on Polygon Anvil fork.

Replaces the VIB-2822 kill-switch pin test: Phase-0 validation (VIB-5028,
``docs/internal/qa/fluid-protocol-validation-2026-06-10.md``) proved the
protocol works at every size — the disable was a quote-shim artifact — and
Phase 1 (VIB-5029) re-enabled the connector with resolver-backed quoting.

Fluid is routerless: ``FluidCompiler`` resolves the per-pair pool on-chain
(USDC/USDT on polygon) and compiles approve + ``swapIn`` against the pool
contract directly. Quotes come from ``DexReservesResolver.estimateSwapIn``
and match execution to the wei, so layer 4 additionally asserts
quote-vs-execution parity, not just min-out.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:

1. Layer 1: ``IntentCompiler.compile(intent)`` returns SUCCESS (routes
   through ``FluidCompiler``; metadata carries pool + swap0to1).
2. Layer 2: ``ExecutionOrchestrator.execute(bundle)`` succeeds against the
   Fluid pool on the Anvil fork under default-on Zodiac.
3. Layer 3: ``FluidReceiptParser`` extracts the pool's
   ``Swap(bool,uint256,uint256,address)`` event with exact amounts.
4. Layer 4: bilateral conservation — USDC spent exactly equals the swap
   amount; USDT received >= compiled ``min_amount_out``.

NO MOCKING. All tests execute real on-chain swaps and verify state changes.

To run::

    uv run pytest tests/intents/polygon/test_fluid_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.fluid.receipt_parser import FluidReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents._fluid_quote_helpers import assert_min_out_quote_derived, fluid_resolver_quote
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    SWAP_MAX_SLIPPAGE,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

CHAIN_NAME = "polygon"


@pytest.mark.polygon
@pytest.mark.swap
class TestFluidSwapIntent:
    """Fluid DEX swaps via SwapIntent — full 4-layer verification."""

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_usdc_to_usdt_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """USDC -> USDT via the Fluid USDC/USDT pool (per-pair contract)."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["USDT"]

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("50")  # $50 — the size class Phase 0 verified

        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        usdt_before = get_token_balance(web3, token_out, funded_wallet)

        expected_usdc_in = int(swap_amount * Decimal(10**in_decimals))
        assert usdc_before >= expected_usdc_in, (
            f"funded_wallet must hold at least {swap_amount} USDC for this test; "
            f"got {format_token_amount(usdc_before, in_decimals)}. "
            f"Check the polygon conftest's wallet-funding fixture."
        )

        intent = SwapIntent(
            from_token="USDC",
            to_token="USDT",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="fluid",
            chain=CHAIN_NAME,
        )

        # Layer 1: compile. ``rpc_url`` is load-bearing for Fluid — the
        # per-pair pool address AND the resolver quote both come from the
        # same fork the orchestrator executes against.
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=orchestrator.rpc_url,
        )
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"

        metadata = compilation_result.action_bundle.metadata
        pool_address = metadata["pool"]
        min_amount_out = int(metadata["min_amount_out"])
        assert Web3.is_checksum_address(pool_address), "metadata.pool must be the resolved pool address"
        assert metadata["swap0to1"] in (True, False)
        assert min_amount_out > 0

        # The swap tx must target the pool itself (routerless protocol).
        swap_txs = [tx for tx in compilation_result.transactions if tx.tx_type == "swap"]
        assert len(swap_txs) == 1
        assert swap_txs[0].to.lower() == pool_address.lower()
        assert swap_txs[0].data.startswith("0x2668dfaa")  # swapIn selector

        # Money-safety invariant: min_amount_out must be the slippage-bounded
        # on-chain quote, verified against an INDEPENDENT resolver re-quote
        # (same fork state — compilation is read-only). A placeholder floor
        # (e.g. min_out=1) fails here even though execution would succeed.
        independent_quote = fluid_resolver_quote(web3, pool_address, metadata["swap0to1"], expected_usdc_in)
        assert_min_out_quote_derived(min_amount_out, independent_quote, SWAP_MAX_SLIPPAGE)

        # Layer 2: execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: parse receipts with the connector's own parser.
        parser = FluidReceiptParser(chain=CHAIN_NAME)
        saw_swap_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt is None:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            for swap_event in parse_result.swap_events:
                saw_swap_event = True
                assert swap_event.amount_in == expected_usdc_in, (
                    f"Swap event amount_in must equal the exact input: "
                    f"{swap_event.amount_in} != {expected_usdc_in}"
                )
                assert swap_event.amount_out >= min_amount_out
                # "Quotes match execution to the wei" (Phase-0 contract):
                # the executed output must EQUAL the independent resolver
                # quote taken on the same fork state — catches a compiler
                # quoting through the wrong source/rounding even when the
                # min-out floor still passes.
                assert swap_event.amount_out == independent_quote, (
                    f"Executed output drifted from the resolver quote: "
                    f"{swap_event.amount_out} != {independent_quote}"
                )

        assert saw_swap_event, (
            "Layer 3 contract: the Fluid pool must emit Swap(bool,uint256,uint256,address) "
            "and FluidReceiptParser must extract it."
        )

        # Layer 4: bilateral conservation + quote parity.
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        usdt_after = get_token_balance(web3, token_out, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        usdt_received = usdt_after - usdt_before

        print("\n--- Results ---")
        print(f"USDC spent:    {format_token_amount(usdc_spent, in_decimals)}")
        print(f"USDT received: {format_token_amount(usdt_received, out_decimals)}")

        assert usdc_spent == expected_usdc_in, (
            f"USDC spent must EXACTLY equal swap amount. Expected: {expected_usdc_in}, Got: {usdc_spent}"
        )
        assert usdt_received >= min_amount_out, (
            f"USDT received ({usdt_received}) below compiled min_amount_out ({min_amount_out})"
        )
        assert usdt_received == independent_quote, (
            f"Balance delta drifted from the resolver quote: {usdt_received} != {independent_quote}"
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
