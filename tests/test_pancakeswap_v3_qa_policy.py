"""Native QA collection counts remain explicit without reducing pool fidelity."""

import json
from dataclasses import replace

import pytest

from almanak.configuration.yaml import load
from almanak.connectors.identifiers import ConnectorId
from tests.qa.catalog import Case
from tests.qa.inputs import ROOT, native_collection_limits, resolved_history


@pytest.mark.parametrize(
    "limits", [{"sample_seconds": 1}, {"max_risk_blocks": 0}, {"max_risk_samples": True}]
)
def test_native_limits_reject_resolution_changes_and_unbounded_values(limits):
    with pytest.raises(ValueError, match="positive collection limits"):
        native_collection_limits("reference", {"native_collection_limits": {"reference": limits}})


def test_limits_do_not_inherit_across_references():
    policy = {"native_collection_limits": {"reference": {"max_risk_blocks": 101}}}
    assert native_collection_limits("other", policy) == {}
    assert native_collection_limits("reference", policy) == {"max_risk_blocks": 101}


@pytest.mark.parametrize("style", ["lp", "trading"])
def test_bnb_native_inclusive_samples_preserve_resolution_age_and_tick_coverage(tmp_path, style):
    reference = "pancakeswap_v3_bnb_" + style
    case = Case(
        id="native/" + reference,
        lane="backtest",
        reference=reference,
        connector=ConnectorId("pancakeswap_v3"),
        model_mode="economic",
        input_policy="native",
    )
    source = ROOT / "strategies" / reference / "backtest-economic.yaml"
    original = source.read_bytes()
    before = load(source).value["data"]
    after = load(resolved_history(case, tmp_path, 0)).value["data"]
    assert after["max_samples"] == 1200 // 60 + 1 == 21
    assert {k: v for k, v in after.items() if k != "max_samples"} == {
        k: v for k, v in before.items() if k != "max_samples"
    }
    assert source.read_bytes() == original
    policy = json.loads((tmp_path / "native-collection-policy.json").read_text())
    assert policy == {"reference": reference, "limits": {"max_samples": 21}, "components": {}}
    pinned = tmp_path / "pinned"
    pinned.mkdir()
    assert (
        load(resolved_history(replace(case, input_policy="pinned"), pinned, 0)).value["data"][
            "max_samples"
        ]
        == 10
    )


def test_existing_spark_native_limits_and_sampling_remain_unchanged(tmp_path):
    reference = "sparklend_ethereum_lending"
    case = Case(
        id="native/spark",
        lane="backtest",
        reference=reference,
        connector=ConnectorId("sparklend"),
        model_mode="economic",
        input_policy="native",
    )
    source = ROOT / "strategies" / reference / "backtest-economic.yaml"
    original = source.read_bytes()
    # The converted reference validates against its shipped public author root.
    (tmp_path / "pinned").mkdir()
    data = load(resolved_history(case, tmp_path, 0)).value["data"]
    assert (data["sample_seconds"], data["max_age_seconds"]) == ("60s", "120s")
    assert data["max_accounting_samples"] == 42
    assert data["max_risk_blocks"] == 101
    assert data["max_risk_samples"] == 202
    pinned = load(resolved_history(replace(case, input_policy="pinned"), tmp_path / "pinned", 0))
    assert pinned.value["data"]["max_accounting_samples"] == 10
    assert source.read_bytes() == original
