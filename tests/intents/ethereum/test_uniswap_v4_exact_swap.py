"""Atomic, sealable Uniswap V4 SWAP proofs on Ethereum.

V4 settles every pool through one PoolManager, so the proof binds the pool by
the keccak of its PoolKey rather than by an emitter address, and binds the
wallet through value flow -- the Swap event's ``sender`` is the router that
unlocked the manager, never the trader.

The connector settles USDC -> WETH here in the pool keyed on the two ERC-20
assets themselves rather than in the native-keyed pool, so these cells declare
``v4_swap.v1``. Fee 500 is pinned because it is the deepest initialised
USDC/WETH pool of that shape on ethereum (liquidity 2.98e15 against 3.17e10 at fee 3000); the pin travels into
the route, so the pool traded is the pool asserted.
"""

from decimal import Decimal

import pytest

from almanak.framework.intents.compiler_models import IntentCompilerConfig
from almanak.framework.intents.vocabulary import IntentType
from qa_lab.operator_gateway import OperatorGatewayClient
from tests.intents._uniswap_v4_exact_proofs import run_uniswap_v4_swap_exact_proof

CHAIN = "ethereum"
FEE_TIER = 500
FROM_SYMBOL = "USDC"
TO_SYMBOL = "WETH"
AMOUNT = Decimal("10")


@pytest.mark.ethereum
@pytest.mark.swap
class TestUniswapV4ExactSwapProof:
    @pytest.mark.qa_proof(protocol="uniswap_v4", contract="v4_swap.v1")
    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_exact_safe(
        self,
        web3,
        anvil_rpc_url,
        funded_wallet,
        orchestrator,
        price_oracle,
        intent_evidence,
        anvil_eth_call_adapter,
    ):
        await run_uniswap_v4_swap_exact_proof(
            chain=CHAIN,
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            price_oracle=price_oracle,
            intent_evidence=intent_evidence,
            rpc_url=anvil_rpc_url,
            gateway_client=anvil_eth_call_adapter,
            profile="v4_swap.v1",
            fee_tier=FEE_TIER,
            amount=AMOUNT,
            from_symbol=FROM_SYMBOL,
            to_symbol=TO_SYMBOL,
        )

    @pytest.mark.qa_proof(protocol="uniswap_v4", contract="v4_swap.v1")
    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same receipt contract through EOA")
    @pytest.mark.intent(IntentType.SWAP)
    @pytest.mark.asyncio
    async def test_swap_exact_eoa(
        self, web3, anvil_rpc_url, funded_wallet, orchestrator, price_oracle, intent_evidence
    ):
        await run_uniswap_v4_swap_exact_proof(
            chain=CHAIN,
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            price_oracle=price_oracle,
            intent_evidence=intent_evidence,
            rpc_url=anvil_rpc_url,
            compiler_config=IntentCompilerConfig(managed_fork=False),
            gateway_client=OperatorGatewayClient(web3, CHAIN),
            profile="v4_swap.v1",
            fee_tier=FEE_TIER,
            amount=AMOUNT,
            from_symbol=FROM_SYMBOL,
            to_symbol=TO_SYMBOL,
        )
