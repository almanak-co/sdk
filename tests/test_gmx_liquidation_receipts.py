"""Retained real-handler capture tests receipt and native residual attribution offline."""

import gzip
import json
from copy import deepcopy
from pathlib import Path

import pytest

from almanak.connectors.gmx_v2 import liquidation_receipts as receipts
from almanak.connectors.gmx_v2.state import Checkpoint
from almanak.gateway.ethereum import DataError
from tests.gmx_retired import current_json

EVIDENCE = Path(__file__).parents[1] / (
    "docs/internal/reports/evidence/2026-09-09-gmx-liquidation/native-arbitrum-eoa-calibration"
)
# The same fork calibration repeated on the current closure: the reference position's own
# lifecycle, the fixture-only MIN_COLLATERAL_USD intervention, then an existing liquidation
# keeper executing the current LiquidationHandler.
CURRENT = Path(__file__).parents[1] / (
    "docs/internal/reports/evidence/2026-09-09-gmx-liquidation-fork-2026-10-20"
    "/native-arbitrum-eoa-calibration"
)


def load_capture(evidence):
    state = Checkpoint.model_validate_json(
        current_json(gzip.decompress((evidence / "checkpoint.json.gz").read_bytes()))
    )
    captured = json.loads(gzip.decompress((evidence / "external-liquidation.json.gz").read_bytes()))
    before = next(o.after for o in reversed(state.outcomes) if o.after.position is not None)
    return before, captured


@pytest.fixture
def capture():
    return load_capture(EVIDENCE)


@pytest.fixture
def current_capture():
    return load_capture(CURRENT)


def test_authentic_liquidation_receipt_and_exact_native_residual(current_capture):
    before, captured = current_capture
    created, executed, decreased = receipts.validate_events(
        captured["receipt"], before.target, before
    )
    assert created.topics == executed.topics
    assert decreased.value("uint256", "orderType") == 7
    fees = receipts.fee_events(captured["receipt"], before.target, created.topics[0])
    assert fees[0].value("uint256", "liquidationFeeAmount") == 802459077424954
    assert (
        receipts.native_received(captured["trace"], captured["receipt"], before.target)
        == 0x15F7DC292202C8D
    )


def transfer(target, amount=17, **changes):
    return {
        "type": "CALL",
        "from": target.market.share_asset.reference,
        "to": target.fund.address,
        "value": hex(amount),
        **changes,
    }


def test_reverted_subtree_cannot_credit_native_inventory(capture):
    before, _ = capture
    assert (
        receipts.native_calls(
            {"error": "revert", "calls": [transfer(before.target)]}, before.target
        )
        == 0
    )
    assert (
        receipts.native_calls(
            {"calls": [transfer(before.target), transfer(before.target, error="revert")]},
            before.target,
        )
        == 17
    )


@pytest.mark.parametrize("kind", ["DELEGATECALL", "STATICCALL", "CALLCODE"])
def test_nontransferring_frame_value_is_not_a_fund_credit(capture, kind):
    before, _ = capture
    assert receipts.native_calls(transfer(before.target, type=kind), before.target) == 0


def test_unrelated_native_credit_cannot_be_adopted(capture):
    before, _ = capture
    with pytest.raises(DataError, match="counterparty"):
        receipts.native_calls(transfer(before.target, **{"from": "0x" + "12" * 20}), before.target)


def test_fund_debit_cannot_be_hidden_in_liquidation_trace(capture):
    before, _ = capture
    with pytest.raises(DataError, match="debited"):
        receipts.native_calls(
            transfer(
                before.target,
                **{
                    "from": before.target.fund.address,
                    "to": before.target.market.share_asset.reference,
                },
            ),
            before.target,
        )


@pytest.mark.parametrize(
    "change", [{"error": "revert"}, {"to": "0x" + "12" * 20}, {"type": "DELEGATECALL"}]
)
def test_trace_must_match_successful_handler_call(current_capture, change):
    before, captured = current_capture
    trace = {**captured["trace"], **change}
    with pytest.raises(DataError, match="successful handler"):
        receipts.native_received(trace, captured["receipt"], before.target)


def test_wrong_event_emitter_cannot_prove_liquidation(current_capture):
    before, captured = current_capture
    receipt = deepcopy(captured["receipt"])
    for log in receipt["logs"]:
        if log["address"].lower() == before.target.market.contracts.event_emitter:
            log["address"] = "0x" + "12" * 20
    with pytest.raises(DataError, match="requires one"):
        receipts.validate_events(receipt, before.target, before)


def test_missing_native_attribution_retains_financial_obligation(capture):
    from almanak.connectors.gmx_v2.liquidation_reconciliation import validate_native_measurement

    before, _ = capture
    wallets = tuple(
        value.model_copy(update={"amount": value.amount + 19})
        if value.asset.namespace == "slip44"
        else value
        for value in before.wallets
    )
    after = before.model_copy(update={"wallets": wallets})
    with pytest.raises(DataError, match="retain the position obligation"):
        validate_native_measurement(before, after, 0)
    validate_native_measurement(before, after, 19)
