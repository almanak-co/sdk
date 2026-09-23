"""Atomic, sealable Morpho Blue lending proofs on Ethereum.

All four target Intents settle in USDC, the wstETH/USDC market's loan token;
the collateral asset is supplied in setup only. 1 wstETH of collateral against a
20 USDC borrow sits far below the market LLTV, so a price move cannot turn a
product failure into a health-factor revert.
"""

from decimal import Decimal

import pytest
from web3 import Web3

from almanak.framework.execution.orchestrator import ExecutionContext, ExecutionOrchestrator
from almanak.framework.intents.vocabulary import IntentType
from tests.intents._morpho_blue_exact_proofs import MorphoAmounts, run_morpho_blue_exact_proof

CHAIN = "ethereum"
MARKET = "wstETH/USDC"
AMOUNTS = MorphoAmounts(collateral=Decimal("1"))


@pytest.fixture
def execution_context(funded_wallet: str) -> ExecutionContext:
    return ExecutionContext(chain=CHAIN, wallet_address=funded_wallet, simulation_enabled=True)


@pytest.mark.ethereum
@pytest.mark.morpho
@pytest.mark.lending
class TestMorphoBlueExactProofs:
    async def _run(
        self,
        target: IntentType,
        web3: Web3,
        funded_wallet: str,
        orchestrator: ExecutionOrchestrator,
        execution_context: ExecutionContext,
        price_oracle: dict[str, Decimal],
        intent_evidence,
        anvil_rpc_url: str,
    ) -> None:
        await run_morpho_blue_exact_proof(
            target=target,
            chain=CHAIN,
            web3=web3,
            funded_wallet=funded_wallet,
            orchestrator=orchestrator,
            execution_context=execution_context,
            price_oracle=price_oracle,
            intent_evidence=intent_evidence,
            rpc_url=anvil_rpc_url,
            market_name=MARKET,
            amounts=AMOUNTS,
        )

    @pytest.mark.qa_proof(protocol="morpho_blue", contract="lending.v1")
    @pytest.mark.intent(IntentType.SUPPLY)
    @pytest.mark.asyncio
    async def test_supply_exact_safe(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence, anvil_rpc_url
    ):
        await self._run(
            IntentType.SUPPLY,
            web3,
            funded_wallet,
            orchestrator,
            execution_context,
            price_oracle,
            intent_evidence,
            anvil_rpc_url,
        )

    @pytest.mark.qa_proof(protocol="morpho_blue", contract="lending.v1")
    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same receipt contract through EOA")
    @pytest.mark.intent(IntentType.SUPPLY)
    @pytest.mark.asyncio
    async def test_supply_exact_eoa(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence, anvil_rpc_url
    ):
        await self._run(
            IntentType.SUPPLY,
            web3,
            funded_wallet,
            orchestrator,
            execution_context,
            price_oracle,
            intent_evidence,
            anvil_rpc_url,
        )

    @pytest.mark.qa_proof(protocol="morpho_blue", target="WITHDRAW", contract="lending.v1")
    @pytest.mark.intent(IntentType.SUPPLY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_withdraw_exact_safe(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence, anvil_rpc_url
    ):
        await self._run(
            IntentType.WITHDRAW,
            web3,
            funded_wallet,
            orchestrator,
            execution_context,
            price_oracle,
            intent_evidence,
            anvil_rpc_url,
        )

    @pytest.mark.qa_proof(protocol="morpho_blue", target="WITHDRAW", contract="lending.v1")
    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same receipt contract through EOA")
    @pytest.mark.intent(IntentType.SUPPLY, IntentType.WITHDRAW)
    @pytest.mark.asyncio
    async def test_withdraw_exact_eoa(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence, anvil_rpc_url
    ):
        await self._run(
            IntentType.WITHDRAW,
            web3,
            funded_wallet,
            orchestrator,
            execution_context,
            price_oracle,
            intent_evidence,
            anvil_rpc_url,
        )

    @pytest.mark.qa_proof(protocol="morpho_blue", target="BORROW", contract="lending.v1")
    @pytest.mark.intent(IntentType.SUPPLY, IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_exact_safe(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence, anvil_rpc_url
    ):
        await self._run(
            IntentType.BORROW,
            web3,
            funded_wallet,
            orchestrator,
            execution_context,
            price_oracle,
            intent_evidence,
            anvil_rpc_url,
        )

    @pytest.mark.qa_proof(protocol="morpho_blue", target="BORROW", contract="lending.v1")
    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same receipt contract through EOA")
    @pytest.mark.intent(IntentType.SUPPLY, IntentType.BORROW)
    @pytest.mark.asyncio
    async def test_borrow_exact_eoa(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence, anvil_rpc_url
    ):
        await self._run(
            IntentType.BORROW,
            web3,
            funded_wallet,
            orchestrator,
            execution_context,
            price_oracle,
            intent_evidence,
            anvil_rpc_url,
        )

    @pytest.mark.qa_proof(protocol="morpho_blue", target="REPAY", contract="lending.v1")
    @pytest.mark.intent(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    async def test_repay_exact_safe(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence, anvil_rpc_url
    ):
        await self._run(
            IntentType.REPAY,
            web3,
            funded_wallet,
            orchestrator,
            execution_context,
            price_oracle,
            intent_evidence,
            anvil_rpc_url,
        )

    @pytest.mark.qa_proof(protocol="morpho_blue", target="REPAY", contract="lending.v1")
    @pytest.mark.no_zodiac(reason="Exact-axis QA parity: exercise the same receipt contract through EOA")
    @pytest.mark.intent(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY)
    @pytest.mark.asyncio
    async def test_repay_exact_eoa(
        self, web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence, anvil_rpc_url
    ):
        await self._run(
            IntentType.REPAY,
            web3,
            funded_wallet,
            orchestrator,
            execution_context,
            price_oracle,
            intent_evidence,
            anvil_rpc_url,
        )
