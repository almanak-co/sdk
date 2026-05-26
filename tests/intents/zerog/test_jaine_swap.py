"""Intent-level swap tests for Jaine DEX on 0G Chain.

Jaine is a Uniswap V3 fork on 0G. The framework routes 0G swaps via the
``uniswap_v3`` connector pointed at Jaine's deployed addresses (see
``almanak/core/contracts.py::UNISWAP_V3_ADDRESSES["zerog"]``).

This test exercises the native-in swap path that the Jaine UI uses: paying
in native A0GI and receiving USDC.e, with SwapRouter02 auto-wrapping
msg.value into W0G.

Full 4-layer verification per .claude/rules/intent-tests.md:
  1. Compilation of SwapIntent -> ActionBundle
  2. Execution via ExecutionOrchestrator on Anvil fork of 0G
  3. Receipt parsing via UniswapV3ReceiptParser
  4. Exact balance deltas (native side accounts for gas cost)
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.connectors.uniswap_v3.receipt_parser import UniswapV3ReceiptParser
from almanak.framework.data.tokens import get_token_resolver
from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    SWAP_MAX_SLIPPAGE,
    get_token_balance,
    get_token_decimals,
)
from tests.intents.pool_helpers import fail_if_v3_pool_missing

CHAIN_NAME = "zerog"


@pytest.mark.zerog
@pytest.mark.swap
class TestJaineSwapIntent:
    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_native_a0gi_to_usdce(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
        anvil_rpc_url: str,
    ) -> None:
        """Native A0GI -> USDC.e via Jaine SwapRouter02 (0.01% pool)."""
        # Fetch token addresses from the same resolver the compiler/adapter
        # uses; that way the pool-existence guard checks the identical pair
        # that resolve_for_swap(A0GI) -> W0G will route through at compile time.
        resolver = get_token_resolver()
        w0g = resolver.resolve("W0G", CHAIN_NAME).address
        token_out = resolver.resolve("USDC.E", CHAIN_NAME).address

        # The Jaine UI-verified pool W0G/USDC.e uses the 0.01% fee tier.
        fail_if_v3_pool_missing(web3, CHAIN_NAME, "uniswap_v3", w0g, token_out, 100)

        out_decimals = get_token_decimals(web3, token_out)
        swap_amount = Decimal("0.05")  # 0.05 A0GI
        amount_in_wei = int(swap_amount * Decimal(10**18))

        native_before = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdce_before = get_token_balance(web3, token_out, funded_wallet)

        intent = SwapIntent(
            from_token="A0GI",
            to_token="USDC.e",
            amount=swap_amount,
            max_slippage=SWAP_MAX_SLIPPAGE,
            protocol="uniswap_v3",
            chain=CHAIN_NAME,
        )

        # Layer 1: compile
        # Pass rpc_url so the compiler can query QuoterV2 to pick the best fee
        # tier. Without it, the heuristic defaults to 500 bps, but Jaine's
        # active W0G/USDC.e pool is at 100 bps.
        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        # Layer 2: execute
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # Layer 3: receipt parser (must find the Jaine/UniV3 Swap event)
        parser = UniswapV3ReceiptParser(chain=CHAIN_NAME)
        parsed_amount_in = 0
        parsed_amount_out = 0
        for tx_result in execution_result.transaction_results:
            if not tx_result.receipt:
                continue
            parse_result = parser.parse_receipt(tx_result.receipt.to_dict())
            if parse_result.success and parse_result.swap_result:
                parsed_amount_in = int(parse_result.swap_result.amount_in or 0)
                parsed_amount_out = int(parse_result.swap_result.amount_out or 0)
                assert parse_result.swap_result.effective_price > 0
        assert parsed_amount_in > 0, "Receipt parser must find swap input"
        assert parsed_amount_out > 0, "Receipt parser must find swap output"

        # Layer 4: bilateral balance deltas
        native_after = web3.eth.get_balance(Web3.to_checksum_address(funded_wallet))
        usdce_after = get_token_balance(web3, token_out, funded_wallet)

        total_gas_cost = sum(
            tx.gas_cost_wei or 0 for tx in execution_result.transaction_results
        )
        native_spent = native_before - native_after
        usdce_received = usdce_after - usdce_before

        # Native side: exact match after subtracting gas cost — catches any
        # silent double-spend or wrong msg.value.
        assert native_spent == amount_in_wei + total_gas_cost, (
            f"Native spend mismatch. Expected amount+gas={amount_in_wei + total_gas_cost}, "
            f"got {native_spent} (amount_in={amount_in_wei}, gas={total_gas_cost})"
        )
        # Also match what the Swap event reported — rules out contract-level
        # accounting drift (tax tokens, rebasing, etc.).
        assert parsed_amount_in == amount_in_wei, (
            f"Swap event amount_in ({parsed_amount_in}) must equal intent amount ({amount_in_wei})"
        )
        # Output side: no-op guard + parser agreement.
        assert usdce_received > 0, "Must receive positive USDC.e (no-op guard)"
        assert usdce_received == parsed_amount_out, (
            f"Wallet delta ({usdce_received}) must equal Swap event amount_out ({parsed_amount_out})"
        )
        assert out_decimals == 6, "Sanity: USDC.e on 0G is 6 decimals"
