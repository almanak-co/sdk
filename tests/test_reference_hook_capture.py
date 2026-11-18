"""Batch hook capture covers every unconverted author file and records refusals."""

import json
from pathlib import Path

import pytest

from scripts import capture_reference_hooks as batch
from scripts.capture_config_baseline import digest

ROOT = Path(__file__).resolve().parents[1]
LEGACY = ROOT / "tests/fixtures/author/aave_v4_weth_supply/legacy"
STRATEGY = ROOT / "strategies/aave_v4_weth_supply/strategy.py"
CONVERTED_NAME = "aave_v4_weth_supply"


def reference(root, name, production=False):
    directory = root / "strategies" / name
    directory.mkdir(parents=True)
    (directory / "strategy.py").write_bytes(STRATEGY.read_bytes())
    (directory / "config.yaml").write_bytes((LEGACY / "config.yaml").read_bytes())
    if production:
        source = (LEGACY / "config.production.yaml").read_bytes()
        (directory / "config.production.yaml").write_bytes(source)
    return directory


@pytest.fixture
def tree(tmp_path, monkeypatch):
    # Component tests control Git provenance; retained batches use the clean-source guard.
    monkeypatch.setattr(batch, "source_revision", lambda _: "component-test")
    monkeypatch.setattr(batch, "EVIDENCE", tmp_path / "evidence")
    reference(tmp_path / "root", "first_reference")
    reference(tmp_path / "root", "second_reference", production=True)
    reference(tmp_path / "root", CONVERTED_NAME)
    return tmp_path


@pytest.fixture
def recorded(monkeypatch):
    calls = []

    def fake(config_path, strategy_path, accounts_path):
        calls.append((config_path, strategy_path, accounts_path))
        return {"schema": "almanak.author-hook-baseline.v1", "content_sha256": "0" * 64}

    monkeypatch.setattr(batch, "capture", fake)
    return calls


def run(tree, *extra):
    output = tree / "evidence" / "batch"
    argv = ["--output-dir", str(output), "--root", str(tree / "root"), *extra]
    assert batch.main(argv) == 0
    return output, json.loads((output / "index.json").read_text())


def bindings(path):
    return [
        (
            entry["name"],
            entry["profile"]["fund"]["chain"],
            entry["profile"]["fund"]["address"].lower(),
        )
        for entry in json.loads(path.read_text())["bindings"]
    ]


def test_batch_captures_every_unconverted_author_file_and_skips_converted(tree, recorded):
    _, index = run(tree)
    assert set(index["captured"]) == {
        "first_reference.json",
        "second_reference.json",
        "second_reference.production.json",
    }
    assert CONVERTED_NAME not in json.dumps(index)
    assert [config.name for config, _, _ in recorded].count("config.production.yaml") == 1


def test_index_records_the_batch_schema_revision_counts_and_digests(tree, recorded):
    output, index = run(tree)
    assert index["schema"] == "almanak.reference-hook-batch.v1"
    assert index["source_revision"] == "component-test"
    assert index["failed"] == {}
    assert index["counts"] == {"captured": 3, "failed": 0}
    for name, entry in index["captured"].items():
        assert set(entry) == {"sha256", "content_sha256"}
        assert entry["content_sha256"] == "0" * 64
        assert entry["sha256"] == digest((output / name).read_bytes())


def test_each_capture_receives_the_reference_strategy_and_a_neighbouring_accounts_file(
    tree, recorded
):
    output, _ = run(tree)
    for config, strategy, accounts in recorded:
        assert strategy == config.parent / "strategy.py"
        assert accounts.parent == output
        assert accounts.is_file()
    assert (output / "second_reference.production.accounts.json").is_file()


def test_placeholder_accounts_are_positional_bindings_per_author_account(tree, recorded):
    output, _ = run(tree)
    chain = {"chain_id": "eip155:1", "environment": "mainnet"}
    assert bindings(output / "first_reference.accounts.json") == [
        ("lending_0", chain, "0x" + "01" * 20),
        ("lending_1", chain, "0x" + "02" * 20),
    ]
    assert bindings(output / "second_reference.production.accounts.json") == [
        ("treasury", chain, "0x" + "01" * 20)
    ]


def test_unreadable_author_file_is_recorded_as_a_failure(tree, recorded):
    (tree / "root/strategies/first_reference/config.yaml").write_text("strategy: {}\n")
    output, index = run(tree)
    assert set(index["captured"]) == {"second_reference.json", "second_reference.production.json"}
    assert index["counts"] == {"captured": 2, "failed": 1}
    assert index["failed"]["first_reference.json"]["error"] == "ConfigurationError"
    assert not (output / "first_reference.json").exists()


def test_a_refused_reference_is_recorded_without_ending_the_batch(tree, monkeypatch):
    def refusing(config_path, strategy_path, accounts_path):
        if config_path.parent.name == "first_reference":
            raise ValueError("connector does not support eip155:4663")
        return {"content_sha256": "0" * 64}

    monkeypatch.setattr(batch, "capture", refusing)
    _, index = run(tree)
    assert index["counts"] == {"captured": 2, "failed": 1}
    assert index["failed"] == {
        "first_reference.json": {
            "error": "ValueError",
            "message": "connector does not support eip155:4663",
        }
    }


def test_only_limits_the_batch_to_the_named_references(tree, recorded):
    _, index = run(tree, "--only", "second_reference")
    assert set(index["captured"]) == {"second_reference.json", "second_reference.production.json"}
    assert len(recorded) == 2


def test_output_directory_must_be_new_and_inside_the_evidence_directory(tree, recorded):
    outside = tree / "elsewhere"
    with pytest.raises(ValueError, match="must remain under"):
        batch.main(["--output-dir", str(outside), "--root", str(tree / "root")])
    assert not outside.exists()
    output, _ = run(tree)
    with pytest.raises(ValueError, match="already exists"):
        batch.main(["--output-dir", str(output), "--root", str(tree / "root")])
