"""Atomic, sealable Uniswap V3 LP proofs on BSC.

USDT/WBNB fee 3000 is the proven Uniswap V3 LP pool on this chain. USDT sorts
below WBNB by address, so USDT is token0 and the helper inverts the USDT-per-WBNB
band onto token1/token0.

The band is kept tight on purpose. The sealer checks tick_lower/tick_upper only
against the positions() witness, never against the declared price band, so the
band being narrow is the ONLY thing that would catch a band-to-tick conversion
error: a wrong tick puts the position out of range and the two-sided minimum
fails. A wide band mints in range regardless and seals green.

What that catches here is a SCALE error -- an off-by-orders-of-magnitude band,
an inverted band, or wrong tick-spacing rounding. It does NOT catch a decimals
transposition: both legs are 18-decimal on BSC (USDT is 18 here, unlike the
6-decimal USDT on most chains), so the 10**(d1 - d0) term is 1 and swapping the
two decimals arguments cancels exactly. A 6/18 pair is where that term has
teeth. ALM-10110 tracks deriving the expected ticks in the sealer instead of
relying on any of this.

The tradeoff a tight band buys: without ANVIL_FORK_BLOCK_BSC set, the fixture
prices at the fork head rather than a pin, so this band is asserted against the
live market with roughly 2.4x of room down and 2.1x up. A BNB move past either
edge fails compilation with UnprotectedTradeError, which will not say "the band
is stale" -- widen it here, do not disable the two-sided minimum.
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

CHAIN = "bsc"
_PAIR = {
    "volatile_symbol": "WBNB",
    "stable_symbol": "USDT",
    "volatile_amount": Decimal("0.01"),
    "stable_amount": Decimal("6"),
    "range_lower": Decimal("300"),
    "range_upper": Decimal("1500"),
    "fee_tier": 3000,
}


@pytest.fixture
def execution_context(funded_wallet: str) -> ExecutionContext:
    return ExecutionContext(chain=CHAIN, wallet_address=funded_wallet, simulation_enabled=True)


@pytest.mark.bsc
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
            **_PAIR,
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
            **_PAIR,
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
            **_PAIR,
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
            web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence, anvil_eth_call_adapter
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
            web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence, anvil_eth_call_adapter
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
            web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence, anvil_eth_call_adapter
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
            web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence, anvil_eth_call_adapter
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
            web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence, anvil_eth_call_adapter
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
            web3, funded_wallet, orchestrator, execution_context, price_oracle, intent_evidence, anvil_eth_call_adapter
        )
