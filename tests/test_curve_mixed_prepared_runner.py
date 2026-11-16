"""Captured Curve inputs replay through replacement readers and the actual references."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.backtest.engine import Journal, Simulator
from almanak.caip import ChainId
from almanak.configuration.backtest_inputs import read_backtest_inputs, resolve_backtest
from almanak.connectors.curve.economic_binding import CurveEconomic
from almanak.connectors.curve.economic_book import PoolBook
from almanak.connectors.curve.economic_data import CRYPTO_MODEL as MODEL
from almanak.connectors.curve.economic_model import EconomicModel
from almanak.connectors.curve.economic_prepare import acquire, authorize_ranges, provision
from almanak.connectors.identifiers import ConnectorId, ProtocolVariant
from almanak.worker_session import preloaded_workers
from tests.curve_retired import current_json
from tests.legacy_references import historical_strategy_current_imports
from tests.qa.catalog import Case
from tests.qa.worker import assert_measurements, assert_model_claim, completed_actions
from tests.rebuilt_datasets import assert_rebuilt_dataset
from tests.repeating_entries import first_entry_only

ROOT = Path(__file__).parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout/curve-mixed-prepared"


def raw(kind, name):
    return gzip.decompress((EVIDENCE / kind / (name + ".gz")).read_bytes())


def restore(directory, kind, names):
    directory.mkdir(exist_ok=True)
    for name in names:
        (directory / name).write_bytes(raw(kind, "dataset/" + name))


@pytest.fixture(params=("lp",))
def captured(request):
    kind = request.param
    return kind, CurveEconomic.manifest_type.model_validate_json(
        current_json(raw(kind, "run/manifest.json"))
    )


def test_captured_pool_book_rejects_changed_payload_and_future_sample(captured, tmp_path):
    kind, manifest = captured
    restore(tmp_path, kind, ("data.sqlite", "dataset.json"))
    book = PoolBook(tmp_path, manifest.config, manifest.settings)
    try:
        target, timeline = next(
            (target, timeline)
            for target, timeline in book.timelines.items()
            if hasattr(timeline.rows[0].payload.pool, "packed_fees")
        )
        row = timeline.rows[0]
        changed_pool = row.payload.pool.model_copy(
            update={"packed_fees": row.payload.pool.packed_fees + 1}
        )
        changed = row.model_copy(
            update={"payload": row.payload.model_copy(update={"pool": changed_pool})}
        )
        with pytest.raises(ValueError, match="differs from retained RPC"):
            book.validate_row(target, changed)
        with pytest.raises(ValueError, match="canonical time floor"):
            book.validate_row(target, row.model_copy(update={"available_at": row.source_time - 1}))
        path = book.paths[target]
        with pytest.raises(ValueError, match="frozen historical window"):
            book.validate_path(
                target,
                path.model_copy(
                    update={
                        "first": path.first.model_copy(
                            update={"timestamp": manifest.settings.schedule.start + 1}
                        )
                    }
                ),
            )
    finally:
        book.close()


def test_converted_economic_inputs_preserve_captured_mixed_settings(captured):
    _, manifest = captured
    authored = read_backtest_inputs(ROOT / "tests/qa/scenarios/curve_mixed/backtest-economic.yaml")
    resolved = resolve_backtest(authored, manifest.config, manifest.settings.history)
    assert resolved == manifest.settings


def test_replacement_history_gateway_reproduces_complete_dataset(captured, tmp_path):
    kind, manifest = captured
    provision(tmp_path, manifest.config, manifest.settings)
    restore(tmp_path, kind, ("observations.sqlite",))
    boundaries = authorize_ranges(tmp_path, manifest.config, manifest.settings, None)
    acquire(tmp_path, manifest.config, manifest.settings, None, boundaries)
    book = PoolBook(tmp_path, manifest.config, manifest.settings)
    try:
        assert_rebuilt_dataset(
            tmp_path,
            book.manifest.model_dump(mode="json"),
            json.loads(raw(kind, "dataset/dataset.json")),
            raw(kind, "dataset/data.sqlite"),
        )
    finally:
        book.close()


def test_actual_reference_reproduces_retained_state_with_no_network(
    captured, tmp_path, monkeypatch
):
    import socket

    import httpx

    def forbidden(*args, **kwargs):
        raise AssertionError("offline Curve backtest attempted network access")

    monkeypatch.setattr(socket.socket, "connect", forbidden)
    monkeypatch.setattr(socket, "getaddrinfo", forbidden)
    monkeypatch.setattr(httpx.Client, "send", forbidden)
    monkeypatch.setattr(httpx.AsyncClient, "send", forbidden)
    kind, manifest = captured
    restore(tmp_path, kind, ("data.sqlite", "dataset.json"))
    book = PoolBook(tmp_path, manifest.config, manifest.settings)
    try:
        journal = Journal(tmp_path / "run.sqlite", type(manifest.initial))
        journal.create(manifest.initial)
        model = EconomicModel(
            manifest.config,
            manifest.settings,
            historical_strategy_current_imports(manifest.strategy),
            book,
        )
        with preloaded_workers():
            final = Simulator(journal, model).run()
        expected_path = tmp_path / "expected.sqlite"
        expected_path.write_bytes(raw(kind, "run/state.sqlite"))
        assert first_entry_only(final) == Journal(expected_path, type(manifest.initial)).load()
        assert final.delivered == len(final.completed) == 19
        assert all(step.transition.result.success for step in final.completed)
        report = CurveEconomic.report(final, model)
        assert report["closed"] and not report["failures"]
        assert final.interval.effective.seconds == 5
        case = Case(
            id="mixed",
            lane="backtest",
            connector=ConnectorId("curve"),
            model=MODEL,
            model_mode="economic",
        )
        assert_model_claim(case, manifest.model_dump(mode="json"))
        assert_measurements(case, report)
        for family in ("stableswap_ng", "twocrypto_ng", "tricrypto_ng"):
            assert completed_actions(
                final,
                ChainId("eip155:1"),
                connector=ConnectorId("curve"),
                variant=ProtocolVariant(family),
            ) == {
                "open_liquidity",
                "increase_liquidity",
                "decrease_liquidity",
                "close_liquidity",
                "swap_exact_input",
                "exit_exchange",
            }
    finally:
        book.close()


def test_revoke_preserves_inventory_for_every_captured_family(captured, tmp_path):
    from almanak.dex_intents import RevokeExchange

    kind, manifest = captured
    restore(tmp_path, kind, ("data.sqlite", "dataset.json"))
    expected_path = tmp_path / "expected.sqlite"
    expected_path.write_bytes(raw(kind, "run/state.sqlite"))
    expected = Journal(expected_path, type(manifest.initial)).load()
    book = PoolBook(tmp_path, manifest.config, manifest.settings)
    try:
        model = EconomicModel(
            manifest.config,
            manifest.settings,
            historical_strategy_current_imports(manifest.strategy),
            book,
        )
        families = set()
        for allocation in manifest.config.allocations:
            prior = next(
                step.pending
                for step in expected.completed
                if step.pending.intent.target.allocation == allocation.name
            )
            target = prior.intent.target
            families.add(target.pool.family)
            pending = prior.model_copy(
                update={"intent": RevokeExchange[type(target)](target=target)}
            )
            change = model.execute(manifest.initial, pending)
            assert change.result.success and change.result.steps == ("revoke",)
            assert manifest.initial.portfolio.apply(change.effects) == manifest.initial.portfolio
        assert families == {"stableswap_ng", "twocrypto_ng", "tricrypto_ng"}
    finally:
        book.close()
