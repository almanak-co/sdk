"""Atomic, sealable Trader Joe V2 SWAP proofs on Arbitrum.

Liquidity Book addresses a pair by (tokenX, tokenY, binStep), so the cell trades
the one WETH/USDC pair the connector's registry declares for this chain at bin
step 15; the venue and its size live in ``TRADERJOE_VENUES``.
"""

import pytest

from almanak.framework.intents.compiler_models import IntentCompilerConfig
from almanak.framework.intents.vocabulary import IntentType
from qa_lab.operator_gateway import OperatorGatewayClient
from tests.intents._traderjoe_v2_exact_proofs import (
    execute_traderjoe_v2_reverse_cleanup,
    run_traderjoe_v2_swap_exact_proof,
)

CHAIN = "arbitrum"


@pytest.mark.arbitrum
@pytest.mark.swap
class TestTraderJoeV2ExactSwapProof:
    @pytest.mark.qa_proof(protocol="traderjoe_v2", contract="liquidity_book_swap.v1")
    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_exact_safe(
        self, web3, funded_wallet, orchestrator, price_oracle, intent_evidence, anvil_eth_call_adapter
    ):
        await run_traderjoe_v2_swap_exact_proof(
            chain=CHAIN,
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            price_oracle=price_oracle,
            intent_evidence=intent_evidence,
            gateway_client=anvil_eth_call_adapter,
        )

    @pytest.mark.qa_proof(protocol="traderjoe_v2", contract="liquidity_book_swap.v1")
    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same receipt contract through EOA")
    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_exact_eoa(
        self, web3, anvil_rpc_url, funded_wallet, orchestrator, price_oracle, intent_evidence
    ):
        gateway = OperatorGatewayClient(web3, CHAIN)
        target = await run_traderjoe_v2_swap_exact_proof(
            chain=CHAIN,
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            price_oracle=price_oracle,
            intent_evidence=intent_evidence,
            compiler_config=IntentCompilerConfig(managed_fork=False),
            rpc_url=anvil_rpc_url,
            gateway_client=gateway,
        )
        await execute_traderjoe_v2_reverse_cleanup(
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
        )
