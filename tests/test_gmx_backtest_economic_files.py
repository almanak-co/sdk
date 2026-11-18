"""Every GMX reference ships an explicit economic backtest configuration that resolves."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.backtest.config import HistoryBinding
from almanak.configuration.backtest_inputs import read_backtest_inputs, resolve_backtest
from almanak.connectors.gmx_v2.economic_config import EconomicConfig
from almanak.connectors.gmx_v2.economic_model import EconomicModel
from almanak.connectors.gmx_v2.economic_parameters import initial
from almanak.connectors.gmx_v2.economic_perpetual_state import EconomicPerpetualPosition
from almanak.connectors.gmx_v2.economic_prepare import binding
from almanak.connectors.identifiers import ConnectorId
from almanak.triggers import IntervalSelection
from scripts.legacy_configuration.author import read_author
from scripts.legacy_configuration.backtest import read_backtest
from tests.legacy_references import harness_directory
from tests.test_reference_conversion import effective, legacy_parameters, legacy_snapshot

ROOT = Path(__file__).parents[1] / "strategies"
CHAINS = {"arbitrum": "eip155:42161", "avalanche": "eip155:43114"}
# Measured from evidence/2026-09-09-gmx-prepared/<chain>-range-settings.json.
SUFFIXES = {"arbitrum": ("0xdeadbeef00010203", 15), "avalanche": ("0x", 0)}
KINDS = {
    "position": ("prepared_position_accounting", None),
    "position_path": ("prepared_position_accounting", None),
    "collateral_path": ("prepared_position_accounting", None),
    "swap": (None, None),
    "liquidity": (None, "fixed_external_gm_valuation"),
}


@pytest.fixture(params=[(chain, kind) for chain in CHAINS for kind in KINDS], ids="-".join)
def reference(request):
    chain, kind = request.param
    path = harness_directory(f"gmx_v2_{chain}_{kind}")
    author = read_author(path / "config.yaml")
    return chain, kind, author, read_backtest(path / "backtest-economic.yaml", author)


def test_reference_backtest_files_select_their_family_and_explicit_sources(reference):
    chain, kind, author, settings = reference
    position_model, liquidity_model = KINDS[kind]
    assert isinstance(settings, EconomicConfig)
    assert settings.market_model == "economic" and settings.costs.kind == "excluded"
    assert (settings.position_model, settings.liquidity_model) == (position_model, liquidity_model)
    assert settings.swap_model == "prepared_integer_market"
    assert str(settings.keeper.payments[0].chain) == CHAINS[chain]
    assert str(settings.history[0].chain.chain_id) == CHAINS[chain]
    source = settings.data.publication("Main")
    assert source.request_kind == "order"
    assert (source.calldata_suffix.prefix, source.calldata_suffix.length) == SUFFIXES[chain]
    assert settings.data.maximum_report_age_seconds <= settings.data.max_age_seconds
    assert settings.schedule.end_policy == "teardown"
    assert settings.funds[0].profile.submitter != settings.funds[0].fund
    assert EconomicConfig.model_validate_json(settings.model_dump_json()) == settings


def test_reference_backtest_files_resolve_parameters_and_initial_inventory(reference):
    _, kind, author, settings = reference
    config = binding.selected_parameters(author, settings)
    state = initial(settings, config, IntervalSelection(configured=author.decision_trigger))
    payload = state.portfolio.positions[0].payload
    assert payload.target.fund == settings.funds[0].fund and payload.escrow is None
    assert isinstance(payload, EconomicPerpetualPosition) == (KINDS[kind][0] is not None)


def test_reference_backtest_files_refuse_a_mismatched_model_family(reference):
    _, kind, author, settings = reference
    position_model = None if KINDS[kind][0] else "prepared_position_accounting"
    mismatched = settings.model_copy(update={"position_model": position_model})
    with pytest.raises(ValueError, match="position selection differs"):
        binding.selected_parameters(author, mismatched)
    with pytest.raises(ValueError, match="exact request family"):
        binding.validate_policy(binding.selected_parameters(author, settings), mismatched)


@pytest.mark.parametrize(
    "reference",
    [(chain, kind) for chain in CHAINS for kind in KINDS if KINDS[kind][0] is not None],
    indirect=True,
    ids="-".join,
)
def test_public_declaration_still_refuses_prepared_perpetual_preparation(reference):
    chain, _, author, settings = reference
    config = binding.selected_parameters(author, settings)
    # Arbitrum backtest is admitted for the proven GM model only, so its
    # unproven perpetual model refuses with the model-level message; Avalanche
    # remains fork-only and refuses the whole mode.
    message = (
        "integer-perpetual-v1' is unsupported on eip155:42161"
        if chain == "arbitrum"
        else "backtest is unsupported"
    )
    with pytest.raises(ValueError, match=message):
        binding.validate_sources(SimpleNamespace(strategy=None), author, settings)
    with pytest.raises(ValueError, match=message):
        EconomicModel(config, settings, None, None)


def test_composed_arbitrum_reference_binds_the_measured_suffix_from_the_retained_packet():
    name = "arbitrum_aave_borrow_and_trade_gmx"
    path = ROOT / name
    entry = legacy_snapshot(name)
    original = effective(entry, "backtest-economic.yaml")
    history = tuple(
        HistoryBinding.model_validate_json(json.dumps(binding)) for binding in original["history"]
    )
    settings = resolve_backtest(
        read_backtest_inputs(path / "backtest-economic.yaml"),
        legacy_parameters(entry),
        history,
    )
    source = settings.models[ConnectorId("gmx_v2")].data.publication("trade")
    assert (source.calldata_suffix.prefix, source.calldata_suffix.length) == SUFFIXES["arbitrum"]
    text = (path / "backtest-economic.yaml").read_text()
    assert "2026-09-09-gmx-prepared/arbitrum-range-settings.json" in text
