"""Native lending ceilings count each distinct configured reserve or market."""

import json
from copy import deepcopy

import pytest

from almanak.caip import ChainId
from almanak.configuration.values import seconds
from almanak.configuration.yaml import load
from tests.qa.catalog import catalog
from tests.qa.inputs import ROOT, resolved_history
from tests.test_native_risk_block_policy import required_blocks

POLICY = json.loads((ROOT / "tests/qa/policy.json").read_text())
REFERENCES = (
    ("aave_v3_arbitrum_lending", "aave_v3"),
    ("aave_v3_avalanche_lending", "aave_v3"),
    ("aave_v3_base_lending", "aave_v3"),
    ("aave_v3_bnb_lending", "aave_v3"),
    ("aave_v3_ethereum_lending", "aave_v3"),
    ("aave_v3_optimism_lending", "aave_v3"),
    ("aave_v3_polygon_lending", "aave_v3"),
    ("arbitrum_aave_borrow_and_trade", "aave_v3"),
    ("arbitrum_compound_borrow_and_trade", "compound_v3"),
    ("arbitrum_morpho_borrow_and_trade", "morpho_blue"),
    ("avalanche_aave_borrow_and_trade", "aave_v3"),
    ("bnb_aave_borrow_and_trade", "aave_v3"),
    ("bnb_morpho_curve_borrow_and_trade", "morpho_blue"),
    ("compound_v3_arbitrum_lending", "compound_v3"),
    ("compound_v3_polygon_lending", "compound_v3"),
    ("ethereum_compound_curve_borrow_and_trade", "compound_v3"),
    ("ethereum_spark_curve_borrow_and_trade", "sparklend"),
    ("morpho_blue_arbitrum_lending", "morpho_blue"),
    ("morpho_blue_bnb_lending", "morpho_blue"),
    ("morpho_blue_robinhood_lending", "morpho_blue"),
    ("optimism_aave_borrow_and_trade", "aave_v3"),
    ("polygon_aave_borrow_and_trade", "aave_v3"),
    ("robinhood_morpho_curve_borrow_and_trade", "morpho_blue"),
)
LIMIT_KEYS = {"max_accounting_samples", "max_risk_blocks", "max_risk_samples", "max_samples"}


def source_identity(connector, market):
    chain = market["chain"]
    if connector in ("aave_v3", "sparklend"):
        return chain, market.get("pool"), market["asset"]
    if connector == "compound_v3":
        return chain, market["comet"]
    if connector == "morpho_blue":
        return chain, market.get("core"), market["market_id"]
    raise AssertionError(connector)


def configured_sources(reference, connector):
    author = load(ROOT / "strategies" / reference / "config.yaml").value
    default = author["strategy"].get("connector")
    used = {
        allocation["market"]
        for group in ("allocations", "debts")
        for allocation in author["strategy"].get(group, {}).values()
    }
    sources = {
        source_identity(connector, market)
        for name, market in author["markets"].items()
        if name in used and market.get("connector", default) == connector
    }
    assert sources
    return sources


def without_limits(value):
    return {key: nested for key, nested in value.items() if key not in LIMIT_KEYS}


@pytest.mark.parametrize("reference,connector", REFERENCES)
def test_native_lending_policy_counts_sources_and_preserves_authored_inputs(
    tmp_path, reference, connector
):
    author_path = ROOT / "strategies" / reference / "config.yaml"
    backtest_path = author_path.with_name("backtest-economic.yaml")
    original_files = (author_path.read_bytes(), backtest_path.read_bytes())
    original = load(backtest_path).value
    cases = [c for c in catalog() if c.id == f"backtest/native/{reference}/economic"]
    assert len(cases) <= 1
    sources = configured_sources(reference, connector)
    directory = tmp_path / reference
    directory.mkdir()
    if cases:
        resolved = load(resolved_history(cases[0], directory, 0)).value
    else:
        # Candidate references remain truthfully refused by the public catalog. Their
        # future ceiling is still derived from the immutable authored inputs.
        resolved = deepcopy(original)
        resolved["data"].update(POLICY["native_collection_limits"][reference])

    sources_per_chain = {
        chain: sum(source[0] == chain for source in sources) for chain in {s[0] for s in sources}
    }
    blocks = sum(required_blocks(ChainId(chain)) for chain in sources_per_chain)
    risk_samples = sum(
        required_blocks(ChainId(chain)) * count for chain, count in sources_per_chain.items()
    )
    before_data = original["data"]
    after_data = resolved["data"]
    if "connectors" in before_data:
        before_lending = before_data["connectors"][connector]
        after_lending = after_data["connectors"][connector]
        limits = POLICY["native_component_collection_limits"][reference][connector]
    else:
        before_lending = before_data
        after_lending = after_data
        limits = POLICY["native_collection_limits"][reference]
    duration = 2 * POLICY["native_window_seconds"]
    accounting = (duration // seconds(before_lending["sample_seconds"]) + 1) * len(sources)
    expected = {
        "max_accounting_samples": accounting,
        "max_risk_blocks": blocks,
        "max_risk_samples": risk_samples,
    }
    assert limits == expected
    assert {key: after_lending[key] for key in expected} == expected
    assert without_limits(after_lending) == without_limits(before_lending)

    for name, component in before_data.get("connectors", {}).items():
        if name == connector:
            continue
        expected_samples = duration // seconds(component["sample_seconds"]) + 1
        assert POLICY["native_component_collection_limits"][reference][name] == {
            "max_samples": expected_samples
        }
        assert after_data["connectors"][name]["max_samples"] == expected_samples
        assert without_limits(after_data["connectors"][name]) == without_limits(component)
    assert (author_path.read_bytes(), backtest_path.read_bytes()) == original_files
