"""Production-grade SwapIntent tests for LiFi on Base.

Tests the full Intent -> Compile -> Execute -> Parse -> Verify flow:
1. Create SwapIntent with token symbols and amounts
2. Compile to ActionBundle using IntentCompiler (protocol="lifi")
3. Execute via ExecutionOrchestrator (full production pipeline, including deferred refresh)
4. Parse receipts using LiFiReceiptParser
5. Verify balances changed correctly

NO MOCKING. All tests execute real on-chain swaps using the real LiFi API.

NOTE: Aggregator tests are flake-prone (see `.claude/rules/intent-tests.md`
anti-pattern #12). For a green CI gate, run `pytest ... -n 0 --count=10`.

To run:
    uv run pytest tests/intents/base/test_lifi_swap.py -v -s
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionOrchestrator
from almanak.framework.intents import SwapIntent
from almanak.framework.intents.compiler import IntentCompiler
from almanak.framework.intents.vocabulary import IntentType
from tests.intents.conftest import (
    CHAIN_CONFIGS,
    format_token_amount,
    get_token_balance,
    get_token_decimals,
)

pytestmark = pytest.mark.no_zodiac(
    reason="Aggregator routes non-deterministically; plan excludes from Zodiac coverage"
)

CHAIN_NAME = "base"


@pytest.mark.base
@pytest.mark.swap
class TestLiFiSwap:
    """LiFi same-chain swaps using SwapIntent on Base."""

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    @pytest.mark.xfail(
        strict=False,
        reason="VIB-4307: LiFi aggregator route flake on Anvil base fork — needs 10/10 run validation per intent-tests rule #12 (as of 2026-05-12)",
    )
    async def test_swap_usdc_to_weth_using_intent(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """USDC -> WETH swap via LiFi SwapIntent on Base."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]

        in_decimals = get_token_decimals(web3, token_in)
        out_decimals = get_token_decimals(web3, token_out)

        swap_amount = Decimal("100")  # 100 USDC

        usdc_before = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)
        assert usdc_before > 0, "funded_wallet must have USDC for this test"

        # L1: Build + compile intent
        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=swap_amount,
            max_slippage=Decimal("0.05"),
            protocol="lifi",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )
        compilation_result = compiler.compile(intent)

        assert compilation_result.status.value == "SUCCESS", (
            f"Compilation failed: {compilation_result.error}"
        )
        assert compilation_result.action_bundle is not None

        metadata = compilation_result.action_bundle.metadata
        assert metadata.get("deferred_swap") is True, "LiFi bundles must be deferred"
        assert metadata.get("protocol") == "lifi"

        # L2: Execute via orchestrator (deferred refresh fetches fresh route)
        execution_result = await orchestrator.execute(compilation_result.action_bundle)
        assert execution_result.success, f"Execution failed: {execution_result.error}"

        # L3: Parse receipts via LiFiReceiptParser
        for tx_result in execution_result.transaction_results:
            if tx_result.receipt:
                from almanak.connectors.lifi.receipt_parser import LiFiReceiptParser

                parser = LiFiReceiptParser()
                parse_result = parser.parse_swap_receipt(
                    receipt=tx_result.receipt.to_dict(),
                    wallet_address=funded_wallet,
                    token_out=token_out,
                    token_in=token_in,
                )
                if parse_result.success:
                    print(f"Parser: amount_in={parse_result.amount_in}, amount_out={parse_result.amount_out}")

        # L4: Bilateral balance deltas
        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)

        usdc_spent = usdc_before - usdc_after
        weth_received = weth_after - weth_before

        expected_usdc_spent = int(swap_amount * Decimal(10**in_decimals))
        assert usdc_spent == expected_usdc_spent, (
            f"USDC spent must equal swap amount. Expected: {expected_usdc_spent}, Got: {usdc_spent}"
        )
        assert weth_received > 0, "Must receive positive WETH (no-op guard)"

        print(f"USDC spent: {format_token_amount(usdc_spent, in_decimals)}")
        print(f"WETH received: {format_token_amount(weth_received, out_decimals)}")

    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_insufficient_balance_fails_safely(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        price_oracle: dict[str, Decimal],
    ):
        """Insufficient balance must fail safely with bilateral conservation."""
        tokens = CHAIN_CONFIGS[CHAIN_NAME]["tokens"]
        token_in = tokens["USDC"]
        token_out = tokens["WETH"]

        usdc_balance = get_token_balance(web3, token_in, funded_wallet)
        weth_before = get_token_balance(web3, token_out, funded_wallet)
        in_decimals = get_token_decimals(web3, token_in)
        balance_decimal = Decimal(usdc_balance) / Decimal(10**in_decimals)
        assert usdc_balance > 0, "funded_wallet must have USDC for this test"

        excessive_amount = balance_decimal * Decimal("100")

        intent = SwapIntent(
            from_token="USDC",
            to_token="WETH",
            amount=excessive_amount,
            max_slippage=Decimal("0.05"),
            protocol="lifi",
            chain=CHAIN_NAME,
        )

        compiler = IntentCompiler(
            chain=CHAIN_NAME,
            wallet_address=funded_wallet,
            price_oracle=price_oracle,
        )
        compilation_result = compiler.compile(intent)
        failed_at_compilation = (
            compilation_result.status.value != "SUCCESS"
            or compilation_result.action_bundle is None
        )

        if not failed_at_compilation:
            execution_result = await orchestrator.execute(compilation_result.action_bundle)
            assert not execution_result.success, "Execution should fail with insufficient balance"

        usdc_after = get_token_balance(web3, token_in, funded_wallet)
        weth_after = get_token_balance(web3, token_out, funded_wallet)
        assert usdc_after == usdc_balance, "Input token must be unchanged after failure"
        assert weth_after == weth_before, "Output token must be unchanged after failure"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
