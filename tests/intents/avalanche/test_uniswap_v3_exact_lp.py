"""Atomic, sealable Uniswap V3 concentrated-liquidity proofs on Avalanche.

The volatile leg is WAVAX, not WETH: Avalanche WETH.e carries no balance-slot
entry in the intent-test token table, so the fork cannot seed it and every
LP_OPEN fails its pre-flight balance check.

Sizes and the $5-$500 AVAX range come from the existing Avalanche LP test
(tests/intents/avalanche/test_uniswap_v3_lp.py). The shared defaults are
denominated for a ~$1000-$3000 WETH, which sits entirely above the WAVAX/USDC
price -- that makes the position single-sided and the LP min-out guard
correctly refuses to encode a zero minimum on one leg.
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents.vocabulary import IntentType
from tests.intents._uniswap_v3_lp_exact_proofs import (
    run_uniswap_v3_lp_close_exact_proof,
    run_uniswap_v3_lp_collect_fees_exact_proof,
    run_uniswap_v3_lp_open_exact_proof,
)

CHAIN = "avalanche"
VOLATILE_AMOUNT = Decimal("2.0")
STABLE_AMOUNT = Decimal("50")
RANGE_LOWER = Decimal("5")
RANGE_UPPER = Decimal("500")


@pytest.fixture
def execution_context(funded_wallet: str) -> ExecutionContext:
    return ExecutionContext(chain=CHAIN, wallet_address=funded_wallet, simulation_enabled=True)


@pytest.mark.avalanche
@pytest.mark.lp
class TestUniswapV3ExactLPProofs:
    async def _open(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        intent_evidence,
        anvil_eth_call_adapter,
    ) -> None:
        await run_uniswap_v3_lp_open_exact_proof(
            chain=CHAIN,
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            execution_context=execution_context,
            price_oracle=price_oracle,
            intent_evidence=intent_evidence,
            gateway_client=anvil_eth_call_adapter,
            rpc_url=str(web3.provider.endpoint_uri),
            volatile_symbol="WAVAX",
            volatile_amount=VOLATILE_AMOUNT,
            stable_amount=STABLE_AMOUNT,
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
        )

    async def _close(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        intent_evidence,
        anvil_eth_call_adapter,
    ) -> None:
        await run_uniswap_v3_lp_close_exact_proof(
            chain=CHAIN,
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            execution_context=execution_context,
            price_oracle=price_oracle,
            intent_evidence=intent_evidence,
            gateway_client=anvil_eth_call_adapter,
            rpc_url=str(web3.provider.endpoint_uri),
            volatile_symbol="WAVAX",
            volatile_amount=VOLATILE_AMOUNT,
            stable_amount=STABLE_AMOUNT,
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
        )

    async def _collect_fees(
        self,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        intent_evidence,
        anvil_eth_call_adapter,
    ) -> None:
        await run_uniswap_v3_lp_collect_fees_exact_proof(
            chain=CHAIN,
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            execution_context=execution_context,
            price_oracle=price_oracle,
            intent_evidence=intent_evidence,
            gateway_client=anvil_eth_call_adapter,
            rpc_url=str(web3.provider.endpoint_uri),
            volatile_symbol="WAVAX",
            volatile_amount=VOLATILE_AMOUNT,
            stable_amount=STABLE_AMOUNT,
            range_lower=RANGE_LOWER,
            range_upper=RANGE_UPPER,
        )

    @pytest.mark.qa_proof(protocol="uniswap_v3", contract="v3_lp.v2")
    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_exact_safe(
        self,
        web3,
        funded_wallet,
        orchestrator,
        execution_context,
        price_oracle,
        intent_evidence,
        anvil_eth_call_adapter,
    ):
        await self._open(
            web3,
            funded_wallet,
            orchestrator,
            execution_context,
            price_oracle,
            intent_evidence,
            anvil_eth_call_adapter,
        )

    @pytest.mark.qa_proof(protocol="uniswap_v3", contract="v3_lp.v2")
    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same LP_OPEN contract through EOA")
    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_exact_eoa(
        self,
        web3,
        funded_wallet,
        orchestrator,
        execution_context,
        price_oracle,
        intent_evidence,
        anvil_eth_call_adapter,
    ):
        await self._open(
            web3,
            funded_wallet,
            orchestrator,
            execution_context,
            price_oracle,
            intent_evidence,
            anvil_eth_call_adapter,
        )

    @pytest.mark.qa_proof(protocol="uniswap_v3", contract="v3_lp.v2")
    @pytest.mark.intent(IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_exact_safe(
        self,
        web3,
        funded_wallet,
        orchestrator,
        execution_context,
        price_oracle,
        intent_evidence,
        anvil_eth_call_adapter,
    ):
        await self._close(
            web3,
            funded_wallet,
            orchestrator,
            execution_context,
            price_oracle,
            intent_evidence,
            anvil_eth_call_adapter,
        )

    @pytest.mark.qa_proof(protocol="uniswap_v3", contract="v3_lp.v2")
    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same LP_CLOSE contract through EOA")
    @pytest.mark.intent(IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_exact_eoa(
        self,
        web3,
        funded_wallet,
        orchestrator,
        execution_context,
        price_oracle,
        intent_evidence,
        anvil_eth_call_adapter,
    ):
        await self._close(
            web3,
            funded_wallet,
            orchestrator,
            execution_context,
            price_oracle,
            intent_evidence,
            anvil_eth_call_adapter,
        )

    @pytest.mark.qa_proof(protocol="uniswap_v3", contract="v3_lp.v2")
    @pytest.mark.intent(IntentType.LP_COLLECT_FEES)
    @pytest.mark.asyncio
    async def test_lp_collect_fees_exact_safe(
        self,
        web3,
        funded_wallet,
        orchestrator,
        execution_context,
        price_oracle,
        intent_evidence,
        anvil_eth_call_adapter,
    ):
        await self._collect_fees(
            web3,
            funded_wallet,
            orchestrator,
            execution_context,
            price_oracle,
            intent_evidence,
            anvil_eth_call_adapter,
        )

    @pytest.mark.qa_proof(protocol="uniswap_v3", contract="v3_lp.v2")
    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise LP_COLLECT_FEES through EOA")
    @pytest.mark.intent(IntentType.LP_COLLECT_FEES)
    @pytest.mark.asyncio
    async def test_lp_collect_fees_exact_eoa(
        self,
        web3,
        funded_wallet,
        orchestrator,
        execution_context,
        price_oracle,
        intent_evidence,
        anvil_eth_call_adapter,
    ):
        await self._collect_fees(
            web3,
            funded_wallet,
            orchestrator,
            execution_context,
            price_oracle,
            intent_evidence,
            anvil_eth_call_adapter,
        )
