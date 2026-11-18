"""Native QA budgets are explicit per reference and do not alter model resolution."""

from dataclasses import replace

import pytest

from almanak.configuration.yaml import load
from almanak.connectors.identifiers import ConnectorId
from tests.qa.catalog import Case
from tests.qa.inputs import ROOT, native_collection_limits, resolved_history


@pytest.mark.parametrize(
    "policy", ({"sample_seconds": 1}, {"max_risk_blocks": 0}, {"max_risk_samples": True})
)
def test_native_policy_cannot_override_resolution_or_use_unbounded_values(policy):
    with pytest.raises(ValueError, match="positive collection limits"):
        native_collection_limits("reference", {"native_collection_limits": {"reference": policy}})


def test_native_policy_does_not_inherit_another_references_limits():
    policy = {"native_collection_limits": {"reference": {"max_risk_blocks": 101}}}
    assert native_collection_limits("other", policy) == {}
    assert native_collection_limits("reference", policy) == {"max_risk_blocks": 101}


def test_spark_native_budget_preserves_sampling_and_frozen_author_settings(tmp_path):
    case = Case(
        id="native/spark",
        lane="backtest",
        reference="sparklend_ethereum_lending",
        connector=ConnectorId("sparklend"),
        model_mode="economic",
        input_policy="native",
    )
    source = ROOT / "strategies/sparklend_ethereum_lending/backtest-economic.yaml"
    original = source.read_bytes()
    raw = load(resolved_history(case, tmp_path, 0)).value
    data = raw["data"]
    assert (data["sample_seconds"], data["max_age_seconds"]) == ("60s", "120s")
    assert data["max_accounting_samples"] == (1200 // 60 + 1) * 2
    assert data["max_risk_blocks"] == 1200 // 12 + 1
    assert data["max_risk_samples"] == data["max_risk_blocks"] * 2
    assert (tmp_path / "native-collection-policy.json").is_file()
    assert source.read_bytes() == original
    pinned = tmp_path / "pinned"
    pinned.mkdir()
    raw = load(resolved_history(replace(case, input_policy="pinned"), pinned, 0)).value
    assert raw["data"]["max_accounting_samples"] == 10
    assert not (pinned / "native-collection-policy.json").exists()
