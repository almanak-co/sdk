"""Base-specific retained pool and OP fee proofs supplement actual reference QA."""

import copy
import json

import httpx
import pytest

from almanak.backtest.engine import Journal, Simulator
from almanak.caip import ChainId
from almanak.connectors.uniswap_v4.calibration import Measurement, load_calibration
from almanak.connectors.uniswap_v4.deployments import ARTIFACTS
from almanak.connectors.uniswap_v4.economic_binding import UniswapEconomic
from almanak.connectors.uniswap_v4.economic_state import State
from almanak.dex_intents import RevokeExchange
from almanak.worker_session import preloaded_workers
from tests.uniswap_v4_base_support import historical, prepared


def refuse_http(*args, **kwargs):
    raise AssertionError("Base prepared execution must use only retained data")


@pytest.mark.parametrize("style", ["lp", "trading"])
def test_current_reference_reproduces_exact_prepared_state(tmp_path, monkeypatch, style):
    monkeypatch.setattr(httpx.Client, "send", refuse_http)
    manifest, expected, model = prepared(tmp_path, style)
    try:
        journal = Journal(tmp_path / "state.sqlite", State)
        journal.create(manifest.initial)
        with preloaded_workers():
            actual = Simulator(journal, model).run()
        assert actual == expected
        report = UniswapEconomic.report(actual, model)
        assert report["closed"] and report["positions"][0]["complete"]
        assert actual.evaluations == 25
        assert len(actual.completed) == (5 if style == "lp" else 2)
        assert all(
            item.pending.eligible_at >= item.pending.decision_time + 30 for item in actual.completed
        )
    finally:
        model.book.close()


@pytest.mark.parametrize("style", ["lp", "trading"])
def test_current_reference_reproduces_exact_detailed_state(tmp_path, monkeypatch, style):
    monkeypatch.setattr(httpx.Client, "send", refuse_http)
    manifest, expected, model, reader = historical(style)
    try:
        journal = Journal(tmp_path / "state.sqlite", type(manifest.initial))
        journal.create(manifest.initial)
        with preloaded_workers():
            assert Simulator(journal, model).run() == expected
        assert reader.used
    finally:
        reader.__exit__()


def test_prepared_revoke(tmp_path):
    manifest, expected, model = prepared(tmp_path)
    try:
        prior = expected.completed[0].pending
        intent = RevokeExchange[type(prior.intent.target)](target=prior.intent.target)
        pending = prior.model_copy(update={"intent": intent})
        change = model.execute(manifest.initial, pending)
        assert intent.target.fund.chain.chain_id == ChainId("eip155:8453")
        assert change.result.delta0 == change.result.delta1 == 0
        assert change.result.fill is None
        assert manifest.initial.portfolio.apply(change.effects) == manifest.initial.portfolio
    finally:
        model.book.close()


def measurements():
    return json.loads((ARTIFACTS / "base-safe-gas-calibration.json").read_text())["measurements"]


def test_base_calibration_requires_all_receipt_costs():
    _, maxima = load_calibration(ChainId("eip155:8453"))
    rows = measurements()
    assert len(rows) == 19 and len(maxima) == 8
    for raw in rows:
        item = Measurement.model_validate_json(json.dumps(raw))
        assert item.op_fee_evidence is not None
        execution = int(item.receipt["gasUsed"], 16) * int(item.receipt["effectiveGasPrice"], 16)
        assert item.cost() > execution


@pytest.mark.parametrize("change", ["missing", "block", "fee"])
def test_base_calibration_refuses_incomplete_or_changed_fees(change):
    raw = copy.deepcopy(measurements()[0])
    if change == "missing":
        raw.pop("op_fee_evidence")
    elif change == "block":
        raw["receipt"]["blockHash"] = "0x" + "12" * 32
    else:
        raw["op_fee_evidence"]["l1_fee_wei"] += 1
    with pytest.raises(ValueError):
        Measurement.model_validate_json(json.dumps(raw)).cost()
