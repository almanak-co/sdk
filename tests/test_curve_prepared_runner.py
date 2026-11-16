"""Captured Curve inputs replay through replacement readers and the actual references."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.curve.economic_binding import CurveEconomic
from almanak.connectors.curve.economic_book import PoolBook
from almanak.connectors.curve.economic_model import EconomicModel
from almanak.connectors.curve.economic_prepare import acquire, authorize_ranges, provision
from almanak.strategy import StrategyDefinition
from almanak.worker_session import preloaded_workers
from tests.curve_retired import current_json
from tests.legacy_references import historical_strategy_current_imports
from tests.rebuilt_datasets import assert_rebuilt_dataset
from tests.repeating_entries import first_entry_only

ROOT = Path(__file__).parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout/curve-stable-prepared"


def raw(kind, name):
    if kind == "base_lp":
        directory = EVIDENCE.parent / "curve-base-stable-prepared" / "lp"
        return gzip.decompress((directory / (name + ".gz")).read_bytes())
    return gzip.decompress((EVIDENCE / kind / (name + ".gz")).read_bytes())


def restore(directory, kind, names):
    directory.mkdir(exist_ok=True)
    for name in names:
        (directory / name).write_bytes(raw(kind, "dataset/" + name))


@pytest.fixture(params=("trading", "lp", "base_lp"))
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
        target, timeline = next(iter(book.timelines.items()))
        row = timeline.rows[0]
        changed_pool = row.payload.pool.model_copy(update={"fee": row.payload.pool.fee + 1})
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
        assert final.delivered == len(final.completed) == (2 if kind == "trading" else 11)
        assert all(step.transition.result.success for step in final.completed)
        report = CurveEconomic.report(final, model)
        assert report["closed"] and not report["failures"]
        assert final.interval.effective.seconds == 5
    finally:
        book.close()


def test_revoke_preserves_captured_inventory(captured, tmp_path):
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
        prior = expected.completed[0].pending
        target = prior.intent.target
        assert target.pool.family == "stableswap_ng"
        change = model.execute(
            manifest.initial,
            prior.model_copy(update={"intent": RevokeExchange[type(target)](target=target)}),
        )
        assert change.result.success and change.result.steps == ("revoke",)
        assert manifest.initial.portfolio.apply(change.effects) == manifest.initial.portfolio
    finally:
        book.close()


def test_historical_strategy_import_conversion_is_narrow_and_in_memory(captured):
    _, manifest = captured
    original = manifest.strategy
    converted = historical_strategy_current_imports(original)
    assert original.source.count("from almanak_v3.") == 2
    assert converted.source == original.source.replace("almanak_v3.", "almanak.")
    assert converted.sha256 != original.sha256
    assert manifest.strategy == original
    changed = StrategyDefinition(
        filename=original.filename,
        source=original.source + '\nNAME = "almanak_v3.untrusted"\n',
    )
    with pytest.raises(ValueError, match="outside a from-import"):
        historical_strategy_current_imports(changed)
    same_line = StrategyDefinition(
        filename=original.filename,
        source=original.source.replace(
            "from almanak_v3.api import",
            'from almanak_v3.api import Memory; LABEL = "almanak_v3.untrusted" #',
            1,
        ),
    )
    with pytest.raises(ValueError, match="outside a from-import"):
        historical_strategy_current_imports(same_line)
    root_import = StrategyDefinition(
        filename=original.filename,
        source="from almanak_v3 import dashboard as ui\n",
    )
    assert historical_strategy_current_imports(root_import).source == (
        "from almanak import dashboard as ui\n"
    )
    same_root_line = StrategyDefinition(
        filename=original.filename,
        source='from almanak_v3 import dashboard; LABEL = "almanak_v3.untrusted"\n',
    )
    with pytest.raises(ValueError, match="outside a from-import"):
        historical_strategy_current_imports(same_root_line)
    with pytest.raises(ValueError, match="no legacy from-import"):
        historical_strategy_current_imports(converted)
