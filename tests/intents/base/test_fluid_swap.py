"""4-layer SwapIntent test for Fluid DEX on Base Anvil fork (native leg).

Base has no Fluid USDC/USDT pool — the canonical liquid pair is
wstETH/ETH (pool verified in Phase-1 chain validation, VIB-5029). Fluid
pools pair the chain's **raw native token** (no WETH wrapping), so this
test also covers the native-output path: the pool pays raw ETH to the
wallet, the compiler maps ``to_token="ETH"`` to Fluid's ``0xEeee…``
sentinel, and the receipt parser reports the native leg without an ERC-20
Transfer log.

Layers: compile (pool + direction resolved on-chain) -> execute under
default-on Zodiac -> parse (``Swap`` event; native-out fallback) ->
bilateral conservation (wstETH spent exactly; native ETH received >=
compiled ``min_amount_out``).

NO MOCKING.

To run::

    uv run pytest tests/intents/base/test_fluid_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.fluid.receipt_parser import FluidReceiptParser
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents._fluid_quote_helpers import (
    assert_execution_matches_quote,
    assert_min_out_quote_derived,
    fluid_resolver_quote,
)
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    SWAP_MAX_SLIPPAGE,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

CHAIN_NAME = "base"


@pytest.mark.base
@pytest.mark.swap
class TestFluidSwapIntent:
    """Fluid DEX swap with a native-ETH output leg on Base."""

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_wsteth_to_native_eth_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """wstETH -> native ETH via the Fluid wstETH/ETH pool."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["wstETH"]
        in_decimals = get_token_decimals(web3, token_in)

        swap_amount = Decimal("0.02")  # ~$40 at current wstETH prices

        wsteth_before = get_token_balance(web3, token_in, funded_wallet)
        eth_before = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))

        expected_in = int(swap_amount * Decimal(10**in_decimals))
        assert wsteth_before >= expected_in, (
            f"funded_wallet must hold at least {swap_amount} wstETH for this test; "
            f"got {format_token_amount(wsteth_before, in_decimals)}. "
            f"Check the base conftest's wallet-funding fixture."
        )

        # Fluid pools pair raw native ETH; the price oracle fixture keys on
        # configured ERC-20 symbols, so seed the native symbol from WETH.
        oracle = dict(price_oracle)
        if "ETH" not in oracle and "WETH" in oracle:
            oracle["ETH"] = oracle["WETH"]

        intent = SwapIntent(
            from_token="wstETH",
            to_token="ETH",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="fluid",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=oracle,
            rpc_url=orchestrator.rpc_url,
        )
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", f"Compilation failed: {compilation_result.error}"
        assert compilation_result.action_bundle is not None, "ActionBundle must be created"
        metadata = compilation_result.action_bundle.metadata
        pool_address = metadata["pool"]
        min_amount_out = int(metadata["min_amount_out"])
        assert min_amount_out > 0

        swap_txs = [tx for tx in compilation_result.transactions if tx.tx_type == "swap"]
        assert len(swap_txs) == 1
        assert swap_txs[0].to.lower() == pool_address.lower()
        # ERC-20 input: no native value on the swap tx
        assert swap_txs[0].value == 0

        # Money-safety invariant: min_amount_out must be the slippage-bounded
        # on-chain quote, verified against an INDEPENDENT resolver re-quote
        # (same fork state — compilation is read-only). Covers the native-out
        # token shape as well as the ERC-20 pairs on the other chains.
        independent_quote = fluid_resolver_quote(web3, pool_address, metadata["swap0to1"], expected_in)
        assert_min_out_quote_derived(min_amount_out, independent_quote, SWAP_MAX_SLIPPAGE)

        # Layer 2: execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: parse — native-out swaps still emit the pool Swap event.
        parser = FluidReceiptParser(chain=CHAIN_NAME)
        saw_swap_event = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt is None:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            for swap_event in parse_result.swap_events:
                saw_swap_event = True
                assert swap_event.amount_in == expected_in
                assert swap_event.amount_out >= min_amount_out
                # "Quotes match execution to the wei" (Phase-0 contract).
                assert_execution_matches_quote(swap_event.amount_out, independent_quote, label="swap-event output")
        assert saw_swap_event, "Fluid pool must emit Swap event on native-out swaps"

        # Layer 4: bilateral conservation. Under default-on Zodiac the
        # wallet is the Safe and pays no gas, so the native delta is the
        # swap output alone.
        wsteth_after = get_token_balance(web3, token_in, funded_wallet)
        eth_after = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))

        wsteth_spent = wsteth_before - wsteth_after
        eth_received = eth_after - eth_before

        print("\n--- Results ---")
        print(f"wstETH spent: {format_token_amount(wsteth_spent, in_decimals)}")
        print(f"ETH received: {format_token_amount(eth_received, 18)}")

        assert wsteth_spent == expected_in, (
            f"wstETH spent must EXACTLY equal swap amount. Expected: {expected_in}, Got: {wsteth_spent}"
        )
        assert eth_received >= min_amount_out, (
            f"native ETH received ({eth_received}) below compiled min_amount_out ({min_amount_out})"
        )
        # Under default-on Zodiac the Safe pays no gas, so the native delta
        # is the swap output alone — it must equal the resolver quote.
        assert_execution_matches_quote(eth_received, independent_quote, label="native balance delta")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
