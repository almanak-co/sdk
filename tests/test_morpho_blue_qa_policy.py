"""Native Morpho budgets count one shared market without changing sample resolution."""

from dataclasses import replace

import pytest

from almanak.caip import ChainId
from almanak.configuration.values import instant
from almanak.configuration.yaml import load
from almanak.connectors.identifiers import ConnectorId
from tests.qa.catalog import Case
from tests.qa.inputs import ROOT, resolved_history


@pytest.mark.parametrize("network,block_seconds", (("ethereum", 12), ("base", 2)))
def test_native_morpho_budget_preserves_sampling_and_author_settings(
    tmp_path, network, block_seconds
):
    reference = f"morpho_blue_{network}_lending"
    case = Case(
        id=f"native/{reference}",
        lane="backtest",
        reference=reference,
        connector=ConnectorId("morpho_blue"),
        chains=(ChainId("eip155:1" if network == "ethereum" else "eip155:8453"),),
        model_mode="economic",
        input_policy="native",
    )
    source = ROOT / f"strategies/{reference}/backtest-economic.yaml"
    original = source.read_bytes()
    data = load(resolved_history(case, tmp_path, 0)).value["data"]
    assert (data["sample_seconds"], data["max_age_seconds"]) == ("30s", "60s")
    # Both funds and collateral share the configured market observation.
    assert data["max_accounting_samples"] == 1200 // 30 + 1
    assert data["max_risk_blocks"] == 1200 // block_seconds + 1
    assert data["max_risk_samples"] == data["max_risk_blocks"]
    assert (tmp_path / "native-collection-policy.json").is_file()
    assert source.read_bytes() == original
    pinned = tmp_path / "pinned"
    pinned.mkdir()
    pinned_data = load(resolved_history(replace(case, input_policy="pinned"), pinned, 0)).value[
        "data"
    ]
    assert pinned_data["max_accounting_samples"] == 10
    assert not (pinned / "native-collection-policy.json").exists()
    fresh = tmp_path / "fresh"
    fresh.mkdir()
    now = 1788868800
    fresh_raw = load(resolved_history(replace(case, input_policy="fresh"), fresh, now)).value
    assert instant(fresh_raw["window"]["end"]) == now - 7200 - 60
    assert instant(fresh_raw["window"]["start"]) == now - 7200 - 60 - 30
    assert fresh_raw["data"]["max_accounting_samples"] == 10
