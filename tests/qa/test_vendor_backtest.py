"""The explicit vendor reference remains a first-class public QA selection."""

import pytest

from almanak.configuration.backtest_inputs import read_backtest_inputs
from tests.qa.catalog import catalog
from tests.qa.inputs import require_providers, resolved_author, resolved_history
from tests.qa.operators import example_operator

REFERENCE = "aave_v4_weth_supply"
VENDOR = "backtest-economic-vendor.yaml"


def vendor_cases():
    return [
        case for case in catalog() if case.reference == REFERENCE and case.backtest_file == VENDOR
    ]


def test_vendor_selection_has_explicit_lanes_and_preserves_model_identity():
    selected = vendor_cases()
    assert {case.input_policy for case in selected} == {"pinned", "fresh", "native"}
    assert {case.id for case in selected} == {
        f"backtest/{policy}/{REFERENCE}/economic/vendor" for policy in ("pinned", "fresh", "native")
    }
    ordinary = next(
        case for case in catalog() if case.id == f"backtest/pinned/{REFERENCE}/economic"
    )
    assert {case.model for case in selected} == {ordinary.model}
    assert all(not case.variants for case in selected)


def test_vendor_case_reads_selected_source_and_public_operator(tmp_path):
    case = next(case for case in vendor_cases() if case.input_policy == "pinned")
    resolved_author(case, tmp_path, {})
    path = resolved_history(case, tmp_path, 2_000_000_000)
    inputs = read_backtest_inputs(path).inputs
    assert inputs.data["risk_prices"] == "configured"
    assert inputs.data["vendors"] == [{"name": "market_prices", "kind": "coingecko"}]
    operator = example_operator(case, "backtest")
    assert [network.provider.api_key_env for network in operator.networks] == ["ALCHEMY_API_KEY"]
    assert [
        (vendor.name, vendor.provider.kind, vendor.provider.api_key_env)
        for vendor in operator.vendors
    ] == [("market_prices", "coingecko", "COINGECKO_API_KEY")]
    assert operator.digest != operator.model_copy(update={"vendors": ()}).digest


def test_vendor_case_requires_both_selected_credentials(tmp_path):
    case = next(case for case in vendor_cases() if case.input_policy == "pinned")
    env = tmp_path / "operator.env"
    env.write_text("ALCHEMY_API_KEY=archive-key\n")
    with pytest.raises(ValueError, match="COINGECKO_API_KEY"):
        require_providers(case, env)
    env.write_text("ALCHEMY_API_KEY=archive-key\nCOINGECKO_API_KEY=vendor-key\n")
    require_providers(case, env)
