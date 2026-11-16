"""Current V4 LP explicit-close proofs retain both financial modes and source identity."""

import gzip
import hashlib
import json
from pathlib import Path

import httpx
import pytest

from almanak.backtest.cli import sdk_digest
from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.uniswap_v4.economic_binding import Manifest, UniswapEconomic
from almanak.connectors.uniswap_v4.state import Checkpoint
from almanak.strategy import StrategyDefinition
from almanak.worker_session import preloaded_workers
from tests.legacy_references import current_imports_if_legacy, reviewed_current_imports
from tests.qa.catalog import catalog
from tests.qa.intent_coverage import assert_economic_direct, assert_native_direct
from tests.qa.retained import current_json, current_value
from tests.strategy_fork.uniswap_assertions import assert_closed

ROOT = Path(__file__).parents[2]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-09-rollout"
PREFIX = "uniswap-v4-direct-close"
REFERENCES = ("uniswap_v4_lp", "uniswap_v4_avalanche_lp")


def directory(lane):
    return EVIDENCE / f"{PREFIX}-{lane}"


def manifest(lane):
    return json.loads((directory(lane) / "manifest.json").read_text())


def read(lane, relative):
    return gzip.decompress((directory(lane) / relative).read_bytes())


def source(reference):
    return StrategyDefinition.read(ROOT / "strategies" / reference / "strategy.py")


def case(case_id):
    return next(c for c in catalog() if c.id == case_id)


@pytest.mark.parametrize("lane", ["native", "prepared"])
def test_sealed_matrix_hashes_and_scope(lane):
    retained = manifest(lane)
    assert len(retained["cases"]) == 12
    assert len({row["case"] for row in retained["cases"]}) == 12
    for row in retained["files"]:
        assert_file(lane, row)


def assert_file(lane, row):
    binary = (directory(lane) / row["path"]).read_bytes()
    assert hashlib.sha256(binary).hexdigest() == row["sha256"]
    assert hashlib.sha256(gzip.decompress(binary)).hexdigest() == row["content_sha256"]


@pytest.mark.parametrize("row", manifest("native")["cases"], ids=lambda r: r["case"])
def test_actual_native_lp_close_and_fund_accounting(row):
    state = Checkpoint.model_validate_json(current_json(read("native", row["checkpoint_path"])))
    selected = case(row["case"])
    assert reviewed_current_imports(state.strategy) == source(selected.reference)
    assert_native_direct(selected, state)
    assert_closed(None, state)
    collect = one_operation(state, "collect")
    burn = one_operation(state, "burn")
    assert collect.fees0 + collect.fees1 > 0
    assert burn.fees0 + burn.fees1 > 0
    assert burn.decision.intent.kind == "close_liquidity"


def one_operation(state, kind):
    (outcome,) = (o for o in state.outcomes if o.operation.kind == kind)
    return outcome


@pytest.mark.parametrize("row", manifest("prepared")["cases"], ids=lambda r: r["case"])
def test_actual_historical_mode_and_direct_intents(tmp_path, row):
    from almanak.connectors.composition import connector_from_wire

    prefix = row["directory"] + "/run/"
    wire = json.loads(read("prepared", prefix + "manifest.json.gz"))
    binding = connector_from_wire(wire["config"]).backtest(wire["settings"]["market_model"])
    captured = binding.manifest_type.model_validate_json(json.dumps(current_value(wire)))
    path = tmp_path / "state.sqlite"
    path.write_bytes(read("prepared", prefix + "state.sqlite.gz"))
    state = Journal(path, type(captured.initial)).load()
    selected = case(row["case"])
    assert reviewed_current_imports(captured.strategy) == source(selected.reference)
    assert_economic_direct(selected, state, captured.config)
    assert state.mode == "finished" and state.delivered == len(state.completed)
    assert captured.settings.market_model == selected.model_mode
    assert captured.interval.effective.seconds == (5 if selected.input_policy == "native" else 3600)


@pytest.mark.parametrize("reference", REFERENCES)
def test_current_prepared_lp_whole_state_reproduction(tmp_path, monkeypatch, reference):
    monkeypatch.setattr(httpx.Client, "send", no_network)
    prefix = f"pinned-{reference}-economic/"
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    for name in ("data.sqlite", "dataset.json"):
        (dataset / name).write_bytes(read("prepared", prefix + "dataset/" + name + ".gz"))
    captured = Manifest.model_validate_json(
        current_json(read("prepared", prefix + "run/manifest.json.gz"))
    )
    assert reviewed_current_imports(captured.strategy) == source(reference)
    strategy = current_imports_if_legacy(captured.strategy)
    captured = captured.model_copy(update={"dataset": dataset, "sdk_digest": sdk_digest()})
    path = tmp_path / "expected.sqlite"
    path.write_bytes(read("prepared", prefix + "run/state.sqlite.gz"))
    expected = Journal(path, type(captured.initial)).load()
    book = UniswapEconomic.open_book(captured)
    try:
        driver = UniswapEconomic.driver(captured.config, captured.settings, strategy, book)
        journal = Journal(tmp_path / "actual.sqlite", type(captured.initial))
        journal.create(captured.initial)
        with preloaded_workers():
            assert Simulator(journal, driver).run() == expected
    finally:
        book.close()


def no_network(*args, **kwargs):
    raise AssertionError("captured-input replay attempted network access")


@pytest.mark.parametrize("name", ["lp", "avalanche_lp"])
def test_current_detailed_lp_whole_state_reproduction(tmp_path, monkeypatch, name):
    from tests.uniswap_support import historical_model

    monkeypatch.setattr(httpx.Client, "send", no_network)
    captured, expected, model = historical_model(name, current=True)
    assert reviewed_current_imports(captured.strategy) == source("uniswap_v4_" + name)
    try:
        journal = Journal(tmp_path / "actual.sqlite", type(captured.initial))
        journal.create(captured.initial)
        with preloaded_workers():
            assert Simulator(journal, model).run() == expected
    finally:
        for history in model.histories.values():
            history.reader.__exit__()


def test_all_current_pinned_modes_survive_real_process_death():
    rows = json.loads(read("prepared", "isolated-results.json.gz"))
    assert len(rows) == 4
    for row in rows:
        assert_isolation(row)
        assert_crashes(row)


def assert_isolation(row):
    assert row["network_probe_errno"] == 101
    assert row["resume"]["exact_state_equal"] and row["reproduce"]["exact_state_equal"]
    assert len(row["recovery"]) == 2


def assert_crashes(row):
    assert all(r["crash"]["returncode"] == -9 for r in row["recovery"])
    assert all(r["exact_state_equal"] for r in row["recovery"])


def test_each_economic_policy_has_exact_pinned_quoter_comparisons():
    paths = comparison_paths()
    assert len(paths) == 6
    rows = comparison_rows(paths)
    assert len(rows) == 84 and all(row["equal"] for row in rows)


def comparison_paths():
    return [
        row["path"]
        for row in manifest("prepared")["files"]
        if row["path"].endswith("/quoter-comparisons.json.gz")
    ]


def comparison_rows(paths):
    return [row for path in paths for row in json.loads(read("prepared", path))]
