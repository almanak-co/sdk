"""Current sandboxed lending references replay history as a new regression proof."""

import gzip
import hashlib
import json

import httpx
import pytest

from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.composition import connector_from_wire
from almanak.strategy import StrategyDefinition
from almanak.worker_session import preloaded_workers
from tests.composed_captures import with_dataset
from tests.lending_captures import current_record
from tests.qa.catalog import ROOT
from tests.qa.lending_refactor import PACKAGE, archived_reference
from tests.qa.test_arbitrum_lending import FILES, PREPARED, REFERENCES, artifact, prepared_prefix


def test_refactor_matrix_and_synthetic_evidence():
    manifest = json.loads((PACKAGE / "manifest.json").read_text())
    assert {row["path"] for row in manifest["files"]} == {
        f"strategies/{reference}/strategy.py" for reference in REFERENCES
    }
    for name, digest in manifest["regression_files"].items():
        assert hashlib.sha256((PACKAGE / name).read_bytes()).hexdigest() == digest
    assert json.loads((PACKAGE / "behavior-equivalence.json").read_text())["passed"]
    assert json.loads((PACKAGE / "validation-equivalence.json").read_text())["passed"]


@pytest.mark.parametrize("case", PREPARED, ids=lambda case: case.id)
def test_current_sandboxed_lending_reference_reproduces_whole_prepared_state(
    tmp_path, monkeypatch, case
):
    def forbidden(*args, **kwargs):
        raise AssertionError("refactor replay attempted external data access")

    monkeypatch.setattr(httpx.Client, "send", forbidden)
    prefix = prepared_prefix(case)
    raw = json.loads(artifact(prefix + "manifest.json.gz"))
    binding = connector_from_wire(raw["config"]).backtest(raw["settings"]["market_model"])
    manifest = binding.manifest_type.model_validate_json(json.dumps(current_record(raw)))
    assert manifest.strategy == archived_reference(case.reference)
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    restore_dataset(prefix.removesuffix("run/") + "dataset/", dataset)
    expected_path = tmp_path / "expected.sqlite"
    expected_path.write_bytes(artifact(prefix + "state.sqlite.gz"))
    expected = Journal(expected_path, type(manifest.initial)).load()
    current = StrategyDefinition.read(ROOT / "strategies" / case.reference / "strategy.py")
    book = binding.open_book(with_dataset(manifest, raw, dataset))
    try:
        driver = binding.driver(manifest.config, manifest.settings, current, book)
        journal = Journal(tmp_path / "current.sqlite", type(manifest.initial))
        journal.create(manifest.initial)
        with preloaded_workers():
            actual = Simulator(journal, driver).run()
        assert actual == expected
        assert binding.report(actual, driver)["closed"]
    finally:
        book.close()


def restore_dataset(prefix, dataset):
    for name in FILES:
        if name.startswith(prefix) and name.endswith(("/dataset.json.gz", "/data.sqlite.gz")):
            destination = dataset / name.removeprefix(prefix).removesuffix(".gz")
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(artifact(name))


def test_archived_regression_sources_preserve_original_bytes():
    manifest = json.loads((PACKAGE / "manifest.json").read_text())
    original = manifest["original_package_manifest"]
    raw = (PACKAGE / original["path"]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == original["sha256"]
    before = json.loads(raw)
    for name, row in manifest["archived_regression_sources"].items():
        decoded = gzip.decompress((PACKAGE / name).read_bytes())
        assert len(decoded) == row["decoded_size"]
        assert hashlib.sha256(decoded).hexdigest() == row["decoded_sha256"]
        assert row["decoded_sha256"] == before["regression_files"][row["original_path"]]
