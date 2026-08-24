"""Atomic, sealable Aave V3 lending proofs on Ethereum."""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents.vocabulary import IntentType
from tests.intents._aave_v3_exact_proofs import run_aave_v3_exact_proof

CHAIN = "ethereum"


@pytest.fixture
def execution_context(funded_wallet: str) -> ExecutionContext:
    return ExecutionContext(chain=CHAIN, wallet_address=funded_wallet, simulation_enabled=True)


@pytest.mark.ethereum
@pytest.mark.lending
class TestAaveV3ExactProofs:
    async def _run(
        self,
        target: IntentType,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        intent_evidence,
    ) -> None:
        await run_aave_v3_exact_proof(
            target=target,
            chain=CHAIN,
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            execution_context=execution_context,
            price_oracle=price_oracle,
            intent_evidence=intent_evidence,
        )

    @pytest.mark.qa_proof(protocol="aave_v3", contract="lending.v1")
    @pytest.mark.intent(IntentType.SUPPLY)
    @pytest.mark.asyncio
    async def test_supply_exact_safe(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence
    ):
        await self._run(
            IntentType.SUPPLY, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence
        )

    @pytest.mark.qa_proof(protocol="aave_v3", contract="lending.v1")
    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same receipt contract through EOA")
    @pytest.mark.intent(IntentType.SUPPLY)
    @pytest.mark.asyncio
    async def test_supply_exact_eoa(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence
    ):
        await self._run(
            IntentType.SUPPLY, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence
        )

    @pytest.mark.qa_proof(protocol="aave_v3", contract="lending.v1")
    @pytest.mark.intent(IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_withdraw_exact_safe(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence
    ):
        await self._run(
            IntentType.WITHDRAW, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence
        )

    @pytest.mark.qa_proof(protocol="aave_v3", contract="lending.v1")
    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same receipt contract through EOA")
    @pytest.mark.intent(IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_withdraw_exact_eoa(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence
    ):
        await self._run(
            IntentType.WITHDRAW, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence
        )

    @pytest.mark.qa_proof(protocol="aave_v3", contract="lending.v1")
    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_exact_safe(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence
    ):
        await self._run(
            IntentType.BORROW, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence
        )

    @pytest.mark.qa_proof(protocol="aave_v3", contract="lending.v1")
    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same receipt contract through EOA")
    @pytest.mark.intent(IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_exact_eoa(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence
    ):
        await self._run(
            IntentType.BORROW, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence
        )

    @pytest.mark.qa_proof(protocol="aave_v3", contract="lending.v1")
    @pytest.mark.intent(IntentType.REPAY)
    @pytest.mark.asyncio
    async def test_repay_exact_safe(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence
    ):
        await self._run(
            IntentType.REPAY, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence
        )

    @pytest.mark.qa_proof(protocol="aave_v3", contract="lending.v1")
    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same receipt contract through EOA")
    @pytest.mark.intent(IntentType.REPAY)
    @pytest.mark.asyncio
    async def test_repay_exact_eoa(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence
    ):
        await self._run(
            IntentType.REPAY, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence
        )
