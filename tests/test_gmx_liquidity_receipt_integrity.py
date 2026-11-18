"""Captured GM receipts reject altered share paths, amounts and currency accounting."""

from copy import deepcopy

import pytest

from almanak.connectors.gmx_v2 import liquidity_receipts
from almanak.connectors.gmx_v2.receipts import TRANSFER
from almanak.gateway.ethereum import DataError
from tests.gmx_native_runtime import gm_completed as gm_completed


@pytest.fixture(params=["deposit-mint", "withdrawal-escrow", "withdrawal-burn"])
def settlement(completed, request):
    kind, action = request.param.split("-")
    status = "accepted" if action == "escrow" else "executed"
    return next(
        o
        for o in completed.outcomes
        if o.request is not None and (o.request.kind, o.request.status) == (kind, status)
    )


def share_log(receipt, target):
    return next(
        log
        for log in receipt["logs"]
        if log["address"].lower() == target.market.share_asset.reference
        and log["topics"][0] == TRANSFER
    )


@pytest.mark.parametrize("change", ["amount", "sender", "receiver", "missing"])
def test_native_gm_share_evidence_requires_exact_mint_escrow_and_burn(settlement, change):
    operation = liquidity_receipts.economic_operation(settlement.decision, settlement.request)
    liquidity_receipts.share_effect(operation, settlement.request, settlement.receipt)
    receipt = deepcopy(settlement.receipt)
    log = share_log(receipt, operation.target)
    if change == "amount":
        log["data"] = "0x" + (int(log["data"], 16) + 1).to_bytes(32).hex()
    elif change == "missing":
        receipt["logs"].remove(log)
    else:
        log["topics"][1 if change == "sender" else 2] = "0x" + "00" * 12 + "12" * 20
    with pytest.raises(DataError, match="GM share transfer|exact request"):
        liquidity_receipts.share_effect(operation, settlement.request, receipt)


def test_native_gm_currency_escrow_cannot_underpay_or_overpay(completed):
    outcome = next(o for o in completed.outcomes if o.operation.kind == "gmx_create_deposit")
    liquidity_receipts.validate_cash(
        outcome.operation, outcome.request, outcome.receipt, outcome.assets
    )
    for change in (-1, 1):
        asset = outcome.assets[0].model_copy(update={"amount": outcome.assets[0].amount + change})
        with pytest.raises(DataError, match="currency effects differ"):
            liquidity_receipts.validate_cash(
                outcome.operation, outcome.request, outcome.receipt, (asset, *outcome.assets[1:])
            )


def test_native_gm_withdrawal_cannot_report_less_than_its_admitted_output(completed):
    outcome = next(
        o
        for o in completed.outcomes
        if o.request and (o.request.kind, o.request.status) == ("withdrawal", "executed")
    )
    operation = liquidity_receipts.economic_operation(outcome.decision, outcome.request)
    minimum = operation.minimum_outputs.root[0]
    changed = tuple(
        value.model_copy(update={"amount": minimum.amount - 1})
        if value.key.asset == minimum.asset
        else value
        for value in outcome.assets
    )
    with pytest.raises(DataError, match="below its admitted asset minimum"):
        liquidity_receipts.validate_cash(operation, outcome.request, outcome.receipt, changed)
