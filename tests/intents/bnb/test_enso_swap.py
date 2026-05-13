"""Production-grade SwapIntent tests for Enso DEX aggregator on BNB/BSC.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow.
NO MOCKING. NOTE: Aggregator tests are flake-prone (see anti-pattern #12).

To run:
    uv run pytest tests/intents/bnb/test_enso_swap.py -v -s
"""

import os
from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    assert_swap_semantic_match,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

CHAIN_NAME = "bsc"

pytestmark = [
    pytest.mark.no_zodiac(
        reason="Aggregator routes non-deterministically; plan excludes from Zodiac coverage"
    ),
    pytest.mark.skipif(
        not os.environ.get("ENSO_API_KEY"),
        reason="ENSO_API_KEY not set -- Enso intent tests require API access",
    ),
]


@pytest.mark.bsc
@pytest.mark.swap
class TestEnsoSwapIntent:
    """Enso aggregator swaps using SwapIntent on BNB."""

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_usdc_to_wbnb_via_enso(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """BUY: USDC -> WBNB swap via Enso on BNB."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WBNB"]

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("100")

        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        wbnb_before = get_token_balance(web3, token_out, funded_wallet)

        expected_usdc_raw = int(swap_amount * Decimal(10**in_decimals))
        assert usdc_before >= expected_usdc_raw

        intent = SwapIntent(
            from_token="USDC",
            to_token="WBNB",
            amount=swap_amount,
            max_slippage=Decimal("0.02"),
            protocol="enso",
            chain=CHAIN_NAME,
        )

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

        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        l3_verified = False
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                from almanak.framework.connectors.enso.receipt_parser import EnsoReceiptParser

                parser = EnsoReceiptParser(chain=CHAIN_NAME)
                swap_amounts = parser.extract_swap_amounts(tx_result.receipt.to_dict())
                if swap_amounts:
                    assert_swap_semantic_match(
                        intent_amount=swap_amount,
                        intent_from_token="USDC",
                        intent_to_token="WBNB",
                        swap_result=swap_amounts,
                        chain=CHAIN_NAME,
                    )
                    l3_verified = True

        assert l3_verified

        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        wbnb_after = get_token_balance(web3, token_out, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        wbnb_received = wbnb_after - wbnb_before

        expected_usdc_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdc_spent == expected_usdc_spent
        assert wbnb_received > 0

        print(f"USDC spent: {format_token_amount(usdc_spent, in_decimals)}")
        print(f"WBNB received: {format_token_amount(wbnb_received, out_decimals)}")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_enso_swap_insufficient_balance_fails(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Insufficient balance must fail at compile OR execute layer."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WBNB"]

        usdc_balance = get_token_balance(web3, token_in, funded_wallet)
        wbnb_before = get_token_balance(web3, token_out, funded_wallet)
        in_decimals = get_token_decimals(web3, token_in)
        balance_decimal = Decimal(usdc_balance) / Decimal(10**in_decimals)

        excessive_amount = balance_decimal * Decimal("100")

        intent = SwapIntent(
            from_token="USDC",
            to_token="WBNB",
            amount=excessive_amount,
            max_slippage=Decimal("0.01"),
            protocol="enso",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
            rpc_url=anvil_rpc_url,
        )
        compilation_result = compiler.compile(intent)
        failed_at_compilation = (
            compilation_result.status.value != "SUCCESS"
            or compilation_result.action_bundle is None
        )

        if not failed_at_compilation:
            execution_result = await orchestrator.execute(compilation_result.action_bundle)
            assert not execution_result.success

        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        wbnb_after = get_token_balance(web3, token_out, funded_wallet)
        assert usdc_after == usdc_balance
        assert wbnb_after == wbnb_before


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
