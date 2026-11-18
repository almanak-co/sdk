import json
from pathlib import Path

import pytest

from scripts.capture_config_baseline import capture, capture_file, leaf_paths, write_capture


def test_inventory_keeps_disabled_and_zero_fields():
    assert leaf_paths({"a": None, "b": [], "c": {}, "inventory": [{"quantity": "0"}]}) == [
        "a",
        "b",
        "c",
        "inventory.0.quantity",
    ]


def test_capture_keeps_failed_configuration_and_its_identity(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("strategy:\n  connector: unknown\n")
    result = capture_file(path, tmp_path)
    assert result["path"] == "config.yaml"
    assert result["effective"]["status"] == "failed"
    assert result["authored"]["strategy"]["connector"] == "unknown"
    assert result["fields"] == ["strategy.connector"]
    assert len(result["sha256"]) == 64


def test_discovery_refuses_missing_reference_input(tmp_path):
    directory = tmp_path / "strategies" / "reference"
    directory.mkdir(parents=True)
    (directory / "strategy.py").write_text("pass\n")
    with pytest.raises(ValueError, match="no config.yaml"):
        capture(tmp_path, "revision", {})


def test_discovery_captures_all_variants_and_never_imports_strategy(tmp_path):
    directory = tmp_path / "strategies" / "reference"
    directory.mkdir(parents=True)
    (directory / "strategy.py").write_text("raise AssertionError('author code must not run')\n")
    reference = Path(__file__).parents[1] / "strategies" / "aave_v4_weth_supply"
    expected = {path.name for path in reference.glob("*.yaml")}
    for name in expected:
        (directory / name).write_bytes((reference / name).read_bytes())
    (directory / "backtest-invalid.yml").write_text("mode: unknown\n")
    result = capture(tmp_path, "frozen-revision", {"installed": []})
    assert result["source_revision"] == "frozen-revision"
    assert result["support"] == {"installed": []}
    assert len(result["references"]) == 1
    files = result["references"][0]["files"]
    assert {Path(row["path"]).name for row in files} == {*expected, "backtest-invalid.yml"}
    assert result["normalization_counts"] == {"parsed": len(expected), "failed": 1}
    assert {row["effective"].get("codec") for row in files} >= {
        "author-inputs-v1",
        "backtest-inputs-v1",
    }


def test_harness_reference_is_captured_as_normalized(tmp_path):
    from tests.test_author_resolution import LEGACY

    directory = tmp_path / "strategies" / "reference"
    directory.mkdir(parents=True)
    (directory / "strategy.py").write_text("raise AssertionError('author code must not run')\n")
    for name in ("config.yaml", "backtest.yaml"):
        (directory / name).write_bytes((LEGACY / name).read_bytes())
    result = capture(tmp_path, "frozen-revision", {"installed": []})
    assert result["normalization_counts"] == {"normalized": 2}


def test_public_root_is_captured_as_parsed_inputs(tmp_path):
    import yaml

    from scripts.capture_config_baseline import effective
    from tests.test_author_resolution import REFERENCE

    path = tmp_path / "config.yaml"
    path.write_bytes((REFERENCE / "config.yaml").read_bytes())
    result = effective(path)
    assert result["status"] == "parsed" and result["codec"] == "author-inputs-v1"
    assert result["value"]["markets"]["main_weth_market"]["chain"] == "eip155:1"
    assert "execution" not in result["value"]
    path.write_text(yaml.safe_dump({"strategy": {}, "markets": {"m": {"chain": "eip155:1"}}}))
    assert effective(path)["status"] == "failed"


def test_publication_preserves_existing_capture_and_cleans_partial_files(tmp_path):
    path = tmp_path / "capture.json"
    write_capture(path, {"amount": "0"})
    with pytest.raises(FileExistsError):
        write_capture(path, {"amount": "1"})
    assert json.loads(path.read_text()) == {"amount": "0"}
    assert list(tmp_path.iterdir()) == [path]
