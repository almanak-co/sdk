"""The Aave V3 exact-proof REPAY target repays a fixed slice of the setup debt.

``borrow_amount`` sizes the setup BorrowIntent while the REPAY target always
requests ``REPAY_AMOUNT``. A smaller debt is clamped on-chain, so the wallet
delta stops matching the request and the exact-proof predicates fail with an
opaque flags dict instead of naming the misconfigured parameter.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from almanak.framework.intents.vocabulary import IntentType
from tests.intents._aave_v3_exact_proofs import REPAY_AMOUNT, run_aave_v3_exact_proof


async def _run(target: IntentType, borrow_amount: Decimal) -> None:
    await run_aave_v3_exact_proof(
        target=target,
        chain="bsc",
        web3=None,
        funded_wallet="0x" + "11" * 20,
        orchestrator=None,
        execution_context=None,
        price_oracle={},
        intent_evidence=None,
        borrow_amount=borrow_amount,
    )


@pytest.mark.asyncio
async def test_repay_rejects_a_borrow_amount_below_the_repay_slice() -> None:
    with pytest.raises(ValueError, match="REPAY setup needs borrow_amount"):
        await _run(IntentType.REPAY, REPAY_AMOUNT - Decimal("1"))


@pytest.mark.asyncio
async def test_repay_accepts_a_borrow_amount_equal_to_the_repay_slice() -> None:
    # Passing the guard reaches chain/web3 wiring this unit test does not build,
    # so anything other than the guard's ValueError proves the guard let it through.
    with pytest.raises(Exception) as excinfo:
        await _run(IntentType.REPAY, REPAY_AMOUNT)
    assert "REPAY setup needs borrow_amount" not in str(excinfo.value)


@pytest.mark.asyncio
async def test_non_repay_targets_are_not_constrained_by_the_repay_slice() -> None:
    with pytest.raises(Exception) as excinfo:
        await _run(IntentType.BORROW, REPAY_AMOUNT - Decimal("1"))
    assert "REPAY setup needs borrow_amount" not in str(excinfo.value)
