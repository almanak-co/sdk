"""Atomic, sealable Uniswap V3 concentrated-liquidity proofs on Robinhood (4663).

The pool is WETH/USDG fee-500, the chain's only V3 pool with real depth. WETH
sorts below USDG by address, so WETH is token0 and the price band is expressed
in USDG per WETH. At the pinned fork block the pool prices WETH near 2,500
USDG, inside the helper's default 1,000-3,000 band, so both tokens are
deposited and the two-sided minimums hold.
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents.vocabulary import IntentType
from tests.intents._uniswap_v3_lp_exact_proofs import (
    run_uniswap_v3_lp_close_exact_proof,
    run_uniswap_v3_lp_open_exact_proof,
)

CHAIN = "robinhood"
STABLE = "USDG"


@pytest.fixture
def execution_context(funded_wallet: str) -> ExecutionContext:
    return ExecutionContext(chain=CHAIN, wallet_address=funded_wallet, simulation_enabled=True)


@pytest.mark.robinhood
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
            stable_symbol=STABLE,
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
            stable_symbol=STABLE,
        )

    @pytest.mark.qa_proof(protocol="uniswap_v3", contract="v3_lp.v2")
    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_exact_safe(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence,
        anvil_eth_call_adapter,
    ):
        await self._open(
            web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence,
            anvil_eth_call_adapter,
        )

    @pytest.mark.qa_proof(protocol="uniswap_v3", contract="v3_lp.v2")
    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same LP_OPEN contract through EOA")
    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_exact_eoa(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence,
        anvil_eth_call_adapter,
    ):
        await self._open(
            web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence,
            anvil_eth_call_adapter,
        )

    @pytest.mark.qa_proof(protocol="uniswap_v3", contract="v3_lp.v2")
    @pytest.mark.intent(IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_exact_safe(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence,
        anvil_eth_call_adapter,
    ):
        await self._close(
            web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence,
            anvil_eth_call_adapter,
        )

    @pytest.mark.qa_proof(protocol="uniswap_v3", contract="v3_lp.v2")
    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same LP_CLOSE contract through EOA")
    @pytest.mark.intent(IntentType.LP_CLOSE)
    @pytest.mark.asyncio
    async def test_lp_close_exact_eoa(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence,
        anvil_eth_call_adapter,
    ):
        await self._close(
            web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence,
            anvil_eth_call_adapter,
        )
