"""Atomic, sealable Aerodrome Slipstream (CL) LP proofs on Base.

These nodes are the Intent board's evidence for the ``aerodrome_slipstream`` lane, which
the support matrix advertises separately from classic (Solidly) Aerodrome. The open proof
binds the pool the SDK's own discovery returns for WETH/USDC, so it follows the venue a
strategy would be steered to rather than a hand-picked pool that happens to compile.
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents.compiler_models import IntentCompilerConfig
from almanak.framework.intents.vocabulary import IntentType
from tests.intents._aerodrome_slipstream_exact_proofs import run_aerodrome_slipstream_lp_open_exact_proof

CHAIN = "base"


@pytest.fixture
def execution_context(funded_wallet: str) -> ExecutionContext:
    return ExecutionContext(chain=CHAIN, wallet_address=funded_wallet, protocol="aerodrome_slipstream")


@pytest.mark.base
@pytest.mark.lp
class TestAerodromeSlipstreamLPExactProofs:
    async def _open(
        self,
        web3: Web3,
        anvil_rpc_url: str,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        intent_evidence,
        anvil_eth_call_adapter,
        compiler_config: IntentCompilerConfig | None = None,
    ) -> None:
        await run_aerodrome_slipstream_lp_open_exact_proof(
            chain=CHAIN,
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            execution_context=execution_context,
            price_oracle=price_oracle,
            intent_evidence=intent_evidence,
            venue_verification_gateway=anvil_eth_call_adapter,
            rpc_url=anvil_rpc_url,
            compiler_config=compiler_config,
        )

    @pytest.mark.qa_proof(protocol="aerodrome_slipstream", contract="v3_lp.v1")
    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_exact_safe(
        self,
        web3,
        anvil_rpc_url,
        funded_wallet,
        orchestrator,
        execution_context,
        price_oracle,
        intent_evidence,
        anvil_eth_call_adapter,
    ):
        await self._open(
            web3,
            anvil_rpc_url,
            funded_wallet,
            orchestrator,
            execution_context,
            price_oracle,
            intent_evidence,
            anvil_eth_call_adapter,
        )

    @pytest.mark.qa_proof(protocol="aerodrome_slipstream", contract="v3_lp.v1")
    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same LP_OPEN contract through EOA")
    @pytest.mark.intent(IntentType.LP_OPEN)
    @pytest.mark.asyncio
    async def test_lp_open_exact_eoa(
        self,
        web3,
        anvil_rpc_url,
        funded_wallet,
        orchestrator,
        execution_context,
        price_oracle,
        intent_evidence,
        anvil_eth_call_adapter,
    ):
        await self._open(
            web3,
            anvil_rpc_url,
            funded_wallet,
            orchestrator,
            execution_context,
            price_oracle,
            intent_evidence,
            anvil_eth_call_adapter,
            compiler_config=IntentCompilerConfig(managed_fork=False),
        )
