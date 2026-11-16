"""Read original supported captures without scheduling or parameter substitutions."""

import gzip
import json
import lzma
from pathlib import Path

import pytest

from almanak.backtest.economic_state import EconomicState
from almanak.backtest.prepared import DatasetManifest
from almanak.connectors.aave_v4.economic_binding import Manifest
from almanak.connectors.composition import checkpoint_from_json
from tests.lending_captures import current_capture
from tests.uniswap_support import scope_only

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.mark.parametrize("name", ["ethereum-reference", "avalanche", "multichain"])
def test_original_aave_economic_inputs_state_and_database_remain_readable(tmp_path, name):
    label = f"aave-v4-{name}-economic"
    raw = json.loads(gzip.decompress((FIXTURES / f"{label}.json.gz").read_bytes()))
    # Scope-only authority (blueprint 27) retired the Aave V4 amount; the capture is read
    # through the documented translation, and everything else must round-trip unchanged.
    raw["manifest"] = current_capture(raw["manifest"])
    manifest = Manifest.model_validate_json(json.dumps(raw["manifest"]))
    state = EconomicState.model_validate_json(json.dumps(raw["state"]))
    assert Manifest.model_validate_json(manifest.model_dump_json()) == manifest
    assert EconomicState.model_validate_json(state.model_dump_json()) == state
    assert manifest.config.model_dump(mode="json") == raw["manifest"]["config"]
    assert state.portfolio.model_dump(mode="json") == raw["state"]["portfolio"]
    assert state.memory.model_dump(mode="json") == raw["state"]["memory"]
    assert state.pending is None and raw["state"]["pending"] is None
    dataset = DatasetManifest.model_validate_json(json.dumps(raw["dataset_manifest"]))
    (tmp_path / "data.sqlite").write_bytes(
        lzma.decompress((FIXTURES / f"{label}.sqlite.xz").read_bytes())
    )
    reader = dataset.open(tmp_path)
    try:
        assert reader.db.execute("SELECT COUNT(*) FROM inputs").fetchone()[0] == dataset.records
        assert dataset.records > 0 and dataset.evidence_records > 0
    finally:
        reader.close()


@pytest.mark.parametrize(
    "name",
    [
        "aave-v4-interval-lifecycle.json",
        "aave-v4-composed-ethereum-initialized.json.gz",
        "aave-v4-composed-avalanche-initialized.json.gz",
    ],
)
def test_original_supported_aave_checkpoints_preserve_parameters_and_ownership(name):
    data = (FIXTURES / name).read_bytes()
    data = gzip.decompress(data) if name.endswith(".gz") else data
    raw = scope_only(current_capture(json.loads(data)))
    state = checkpoint_from_json(json.dumps(raw))
    assert checkpoint_from_json(state.model_dump_json()) == state
    config = state.config.model_dump(mode="json")
    assert {key: config[key] for key in raw["config"]} == raw["config"]
    if name == "aave-v4-interval-lifecycle.json":
        assert set(config) - set(raw["config"]) == {"debts"} and config["debts"] == []
    else:
        assert config == raw["config"]
    for owned, original in zip(state.owned, raw["owned"], strict=True):
        value = owned.model_dump(mode="json")
        assert {key: value[key] for key in original} == original
        additions = {key: value[key] for key in value.keys() - original.keys()}
        assert additions == (
            {
                "drawn_shares": 0,
                "premium_shares": 0,
                "premium_offset_ray": 0,
                "collateral_enabled": False,
            }
            if name == "aave-v4-interval-lifecycle.json"
            else {}
        )
    assert state.pending is None and raw["pending"] is None
    assert "preparation_digest" not in state.config.model_dump(mode="json")
