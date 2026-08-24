"""Atomic, sealable Uniswap V3 SWAP proofs on Base."""

from decimal import Decimal

import pytest

from almanak.framework.intents.compiler_models import IntentCompilerConfig
from almanak.framework.intents.vocabulary import IntentType
from scripts.qa.operator_gateway import OperatorGatewayClient
from tests.intents._uniswap_v3_exact_proofs import (
    execute_uniswap_v3_exact_reverse_cleanup,
    run_uniswap_v3_swap_exact_proof,
)

CHAIN = "base"


@pytest.mark.base
@pytest.mark.swap
class TestUniswapV3ExactSwapProof:
    @pytest.mark.qa_proof(protocol="uniswap_v3", contract="swap.v1")
    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_exact_safe(
        self, web3, funded_wallet, orchestrator, price_oracle, intent_evidence, anvil_eth_call_adapter
    ):
        await run_uniswap_v3_swap_exact_proof(
            chain=CHAIN,
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            price_oracle=price_oracle,
            intent_evidence=intent_evidence,
            gateway_client=anvil_eth_call_adapter,
        )

    @pytest.mark.qa_proof(protocol="uniswap_v3", contract="swap.v1")
    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same receipt contract through EOA")
    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_exact_eoa(
        self, web3, anvil_rpc_url, funded_wallet, orchestrator, price_oracle, intent_evidence
    ):
        gateway = OperatorGatewayClient(web3, CHAIN)
        target = await run_uniswap_v3_swap_exact_proof(
            chain=CHAIN,
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            price_oracle=price_oracle,
            intent_evidence=intent_evidence,
            compiler_config=IntentCompilerConfig(managed_fork=False),
            rpc_url=anvil_rpc_url,
            gateway_client=gateway,
            max_price_impact=Decimal("0.02"),
        )
        await execute_uniswap_v3_exact_reverse_cleanup(
            chain=CHAIN,
            web3=web3,
            wallet=funded_wallet,
            orchestrator=orchestrator,
            price_oracle=price_oracle,
            execution_context=None,
            compiler_config=IntentCompilerConfig(managed_fork=False),
            rpc_url=anvil_rpc_url,
            gateway_client=gateway,
            amount_in_raw=target.amount_out_raw,
            max_price_impact=Decimal("0.02"),
        )
