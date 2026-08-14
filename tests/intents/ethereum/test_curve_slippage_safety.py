"""Curve malformed-slippage safety refusals on an Ethereum Anvil fork."""

import pytest
from web3 import Web3

from almanak.framework.intents.vocabulary import IntentType
from tests.intents._curve_slippage_safety_helpers import (
    assert_curve_invalid_slippage_refusals,
)


@pytest.mark.ethereum
@pytest.mark.asyncio
@pytest.mark.intent(IntentType.SWAP, IntentType.LP_OPEN, IntentType.LP_CLOSE)
# noqa: layers -- the shared helper covers compilation, orchestration, and balance conservation.
async def test_curve_invalid_slippage_refuses_without_balance_changes(
    web3: Web3,
    funded_wallet: str,
    anvil_rpc_url: str,
    orchestrator,
) -> None:
    await assert_curve_invalid_slippage_refusals(
        web3=web3,
        funded_wallet=funded_wallet,
        anvil_rpc_url=anvil_rpc_url,
        chain="ethereum",
        orchestrator=orchestrator,
    )
