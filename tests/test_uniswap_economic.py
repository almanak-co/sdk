"""Actual reference strategies and exact deployed Quoter comparisons, entirely offline."""

import json
from dataclasses import asdict
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest

from almanak.backtest import economic_cli
from almanak.backtest.cli import write_json
from almanak.backtest.engine import Journal, Simulator
from almanak.backtest.history import ReadEvidence
from almanak.backtest.types import UnixTime
from almanak.connectors.uniswap_v4.backtest_binding import contract_bindings
from almanak.connectors.uniswap_v4.economic_binding import UniswapEconomic
from almanak.connectors.uniswap_v4.economic_model import quote_at
from almanak.connectors.uniswap_v4.economic_state import State
from almanak.connectors.uniswap_v4.reader import EconomicQuoteSource, quote
from almanak.connectors.uniswap_v4.swap_math import exact_input
from almanak.models import AssetAmount
from almanak.worker_session import preloaded_workers
from tests.uniswap_economic_support import prepared, quoter_evidence
from tests.uniswap_support import CapturedHistory


def no_http(*args, **kwargs):
    raise AssertionError("prepared economic execution must not use HTTP")


@pytest.mark.parametrize("chain", ["ethereum", "avalanche"])
@pytest.mark.parametrize("kind", ["trading", "lp"])
def test_actual_strategy_closes_and_replays_without_network(tmp_path, monkeypatch, chain, kind):
    monkeypatch.setattr(httpx.Client, "send", no_http)
    manifest, expected, model = prepared(tmp_path, chain, kind)
    try:
        journal = Journal(tmp_path / "state.sqlite", State)
        journal.create(manifest.initial)
        with preloaded_workers():
            result = Simulator(journal, model).run()
        from tests.dx_replay import assert_economics

        assert_economics(result, expected)
        report = UniswapEconomic.report(result, model)
        assert report["closed"] and report["positions"][0]["complete"]
        assert result.evaluations == 25
        assert len(result.completed) == (5 if kind == "lp" else 2)
        if kind == "lp":
            assert [r.pending.intent.kind for r in result.completed] == [
                "swap_exact_input",
                "open_liquidity",
                "collect_liquidity_fees",
                "close_liquidity",
                "exit_exchange",
            ]
        assert not any(r.transition.result.gas_paid.amount for r in result.completed)
        assert all(r.pending.eligible_at >= r.pending.decision_time + 30 for r in result.completed)
        replay = Journal(tmp_path / "replay.sqlite", State)
        replay.create(manifest.initial)
        with preloaded_workers():
            assert Simulator(replay, model).run() == result
        for completed in result.completed:
            fill = completed.transition.result.fill
            if fill is not None:
                record = model.book.at(
                    completed.pending.intent.target, completed.pending.eligible_at
                )
                calculated = exact_input(
                    record.payload,
                    fill.amount.amount,
                    fill.amount.asset == completed.pending.intent.target.assets[0],
                )
                assert calculated.output == fill.output.amount
        for completed in result.completed:
            outcome = completed.transition.result
            assert int(outcome.effective_at.timestamp()) == completed.pending.eligible_at
            assert outcome.before.block.timestamp <= completed.pending.eligible_at
            if outcome.fill:
                assert isinstance(outcome.fill.source, EconomicQuoteSource)
                assert outcome.fill.quoter_gas is None
                assert outcome.fill.source.simulation_time == completed.pending.eligible_at
    finally:
        model.book.close()


@pytest.mark.parametrize("chain", ["ethereum", "avalanche"])
def test_exact_calculator_matches_historical_quoter_both_directions_sizes_and_fees(tmp_path, chain):
    manifest, _, model = prepared(tmp_path, chain)
    raw = quoter_evidence(chain)
    evidence = tuple(ReadEvidence.model_validate_json(json.dumps(r)) for r in raw["evidence"])
    reader = CapturedHistory(contract_bindings(manifest.settings, manifest.config)[0], evidence)
    try:
        target = manifest.config.target("trading")
        directions, sizes, crossings = set(), set(), []
        for case in raw["results"]:
            amount = AssetAmount.model_validate_json(json.dumps(case["input"]))
            record = model.book.at(target, case["sample_at"])
            reader.pin_block(record.reference.number)
            archived = quote(reader, target, amount, record.reference)
            calculated = exact_input(
                record.payload, amount.amount, amount.asset == target.assets[0]
            )
            assert calculated.output == archived.output.amount == int(case["quoter_output"])
            assert list(calculated.crossed_ticks) == case["crossed_ticks"]
            assert asdict(calculated)["steps"] == case["steps"]
            directions.add(amount.asset)
            sizes.add(amount.amount)
            crossings.extend(calculated.crossed_ticks)
        assert directions == set(target.assets) and len(sizes) > 8
        assert (bool(crossings), record.payload.pool.protocol_fee != 0) == (
            chain == "ethereum",
            chain == "ethereum",
        )
        for case in raw["refused"]:
            amount = AssetAmount.model_validate_json(json.dumps(case["input"]))
            record = model.book.at(target, case["sample_at"])
            with pytest.raises(ValueError, match="outside prepared tick coverage"):
                exact_input(record.payload, amount.amount, amount.asset == target.assets[0])
    finally:
        reader.__exit__()
        model.book.close()


@pytest.mark.parametrize("phase", ["pending", "effects", "outcome"])
def test_pending_effects_and_outcome_survive_runtime_replacement(tmp_path, monkeypatch, phase):
    monkeypatch.setattr(httpx.Client, "send", no_http)
    manifest, expected, model = prepared(tmp_path)
    directory = tmp_path / "run"
    directory.mkdir()
    write_json(directory / "manifest.json", manifest)
    journal = Journal(directory / "state.sqlite", State)
    journal.create(manifest.initial)
    simulator = Simulator(journal, model)
    try:
        with preloaded_workers():
            while True:
                simulator.tick()
                state = journal.load()
                if reached(state, phase):
                    break
    finally:
        model.book.close()
    economic_cli.resume(SimpleNamespace(run=directory))
    from tests.dx_replay import assert_economics

    assert_economics(Journal(directory / "state.sqlite", State).load(), expected)
    assert economic_cli.reproduce(SimpleNamespace(run=directory))["reproduced"]


def reached(state, phase):
    return {
        "pending": state.pending is not None,
        "effects": len(state.completed) > state.delivered,
        "outcome": state.delivered > 0,
    }[phase]


def test_quotes_bind_source_time_and_reject_future_samples(tmp_path):
    manifest, _, model = prepared(tmp_path)
    try:
        target = manifest.config.target("trading")
        at = int(manifest.settings.schedule.start)
        record = model.book.at(target, at + 3599)
        assert record.available_at == at
        value = quote_at(record, target, manifest.config.allocations[0].capital, at + 3599)
        assert value.block == record.reference and value.source.simulation_time == at + 3599
        assert model.snapshot(
            manifest.initial, manifest.initial.portfolio.positions[0].payload, at + 3599
        ).effective_at == datetime.fromtimestamp(at + 3599, UTC)
        with pytest.raises(ValueError, match="future"):
            quote_at(record, target, value.amount, at - 1)
        timeline = model.book.timelines[target]
        timeline.requirement = timeline.requirement.model_copy(update={"max_age_seconds": 30})
        with pytest.raises(ValueError, match="permitted source observation age"):
            model.book.at(target, UnixTime(at + 31))
    finally:
        model.book.close()
