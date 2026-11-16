"""Pinned gas quotes include accrual while preserving actual protocol observations."""

from types import SimpleNamespace

import pytest

from almanak.connectors.morpho_blue.gas import GasEstimation, estimate_gas
from almanak.gateway.ethereum import DataError
from tests.morpho_blue_helpers import candidate_morpho_support as candidate_morpho_support
from tests.morpho_blue_helpers import recorded_morpho as recorded_morpho


def test_quote_measures_accrual_without_mutating_market_or_reducing_pinned_estimate(
    recorded_morpho,
):
    outcome = next(o for o in recorded_morpho.outcomes if o.operation.kind == "borrow")
    before = outcome.after
    accounting = before.account.market.accounting
    assert accounting.last_update == before.block.timestamp
    original = before.model_dump_json()
    word = accounting.last_update + (accounting.fee << 128)
    calls = []

    def rpc(method, params):
        calls.append((method, params))
        if method == "eth_getStorageAt":
            return hex(word)
        return hex(90000 if len(params) == 2 else 130000)

    quote = estimate_gas(SimpleNamespace(_rpc=rpc), outcome.operation, before, {"data": "0x00"})
    evidence = quote["gas_estimation"]
    assert quote["gas_estimate"] == 130000
    assert evidence.pinned_estimate == 90000 and evidence.accrued_estimate == 130000
    assert int(evidence.estimated_word, 16) == word - 1
    assert len(calls) == 3
    assert all(
        p[1 if m == "eth_estimateGas" else 2]
        == {"blockHash": before.block.hash, "requireCanonical": True}
        for m, p in calls
    )
    assert calls[-1][1][2] == {
        before.target.reserve.market.core: {"stateDiff": {evidence.slot: evidence.estimated_word}}
    }
    assert before.model_dump_json() == original


def test_changed_timestamp_storage_cannot_be_overridden(recorded_morpho):
    outcome = next(o for o in recorded_morpho.outcomes if o.operation.kind == "borrow")
    reader = SimpleNamespace(_rpc=lambda method, params: "0x01")
    with pytest.raises(DataError, match="storage differs"):
        estimate_gas(reader, outcome.operation, outcome.after, {})


def test_estimation_evidence_cannot_override_market_fee():
    with pytest.raises(ValueError, match="lastUpdate"):
        GasEstimation(
            pinned_estimate=1,
            accrued_estimate=1,
            slot="0x" + "00" * 32,
            observed_word="0x" + (100).to_bytes(32, "big").hex(),
            estimated_word="0x" + ((1 << 128) + 99).to_bytes(32, "big").hex(),
        )
