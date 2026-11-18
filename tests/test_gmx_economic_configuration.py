"""One YAML backtest input freezes explicit GMX publications, funds and keeper costs."""

from datetime import UTC, datetime

import pytest
import yaml

from almanak.connectors.gmx_v2.economic_config import EconomicConfig
from almanak.connectors.gmx_v2.economic_parameters import initial
from almanak.connectors.gmx_v2.economic_prepare import binding
from almanak.triggers import IntervalSelection
from scripts.legacy_configuration.author import read_author
from scripts.legacy_configuration.backtest import read_backtest
from tests.legacy_references import harness_directory
from tests.test_gmx_economic_book import prepared as prepared


def selected_author(config, kind="swap"):
    name = (
        "arbitrum" if str(config.allocations[0].chain.chain_id) == "eip155:42161" else "avalanche"
    )
    return read_author(harness_directory(f"gmx_v2_{name}_{kind}") / "config.yaml")


def test_yaml_settings_resolve_explicit_simulation_authority_and_native_units(prepared):
    directory, config, settings, _ = prepared
    author = selected_author(config)
    raw = {
        "mode": "economic",
        "window": {
            "start": datetime.fromtimestamp(settings.schedule.start, UTC).isoformat(),
            "end": datetime.fromtimestamp(settings.schedule.end, UTC).isoformat(),
            "end_policy": settings.schedule.end_policy,
            "cleanup": "0s",
        },
        "inventory": {
            "funds": {
                fund.name: {
                    "balances": [
                        {
                            "asset": str(value.asset),
                            "quantity": {"base_units": value.amount},
                        }
                        for value in fund.balances
                    ]
                }
                for fund in settings.funds
            }
        },
        "execution": {
            "latency": "1s",
            "models": {
                "swap_model": settings.swap_model,
                "liquidity_model": None,
                "position_model": None,
                "keeper": settings.keeper.model_dump(mode="json"),
            },
        },
        "data": {
            **settings.data.model_dump(mode="json"),
            "max_age_seconds": "60s",
            "history": [value.model_dump(mode="json") for value in settings.history],
        },
        "costs": {"kind": "excluded"},
    }
    path = directory / "backtest.yaml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    selected = read_backtest(path, author)
    assert isinstance(selected, EconomicConfig)
    assert selected.keeper == settings.keeper
    assert selected.data == settings.data
    assert selected.funds[0].profile.kind == "simulation"
    assert selected == EconomicConfig.model_validate_json(selected.model_dump_json())
    parameters = binding.selected_parameters(author, selected)
    state = initial(selected, parameters, IntervalSelection(configured=author.decision_trigger))
    assert state.portfolio.positions[0].payload.target.fund == selected.funds[0].fund


def test_missing_publication_binding_refuses_preparation_before_any_external_access(prepared):
    _, config, settings, _ = prepared
    source = settings.data.publications[0].model_copy(update={"allocation": "Unbound"})
    changed = settings.model_copy(
        update={"data": settings.data.model_copy(update={"publications": (source,)})}
    )
    with pytest.raises(ValueError, match="one explicit publication source"):
        binding.selected_parameters(selected_author(config), changed)


def test_keeper_payment_cannot_exceed_authorized_fund_fee_escrow(prepared):
    _, config, settings, _ = prepared
    payment = settings.keeper.payments[0]
    changed_payment = payment.model_copy(
        update={
            "payment": payment.payment.model_copy(
                update={"amount": config.allocations[0].execution_fee.amount + 1}
            )
        }
    )
    changed = settings.model_copy(
        update={"keeper": settings.keeper.model_copy(update={"payments": (changed_payment,)})}
    )
    with pytest.raises(ValueError, match="exceeds the fund"):
        binding.selected_parameters(selected_author(config), changed)


def test_swap_historical_binding_does_not_imply_prepared_perpetual_support(prepared):
    _, config, settings, _ = prepared
    with pytest.raises(ValueError, match="position selection differs"):
        binding.selected_parameters(selected_author(config, "position"), settings)


def test_unproven_history_refuses_shared_preparation_and_model_construction(prepared):
    from types import SimpleNamespace

    from almanak.connectors.gmx_v2.economic_model import EconomicModel

    _, config, settings, _ = prepared
    # Arbitrum backtest is admitted for the proven GM model only, so its
    # unproven swap model refuses with the model-level message; Avalanche
    # remains fork-only and refuses the whole mode.
    arbitrum = str(config.allocations[0].chain.chain_id) == "eip155:42161"
    message = (
        "integer-swap-v1' is unsupported on eip155:42161"
        if arbitrum
        else ("backtest is unsupported")
    )
    with pytest.raises(ValueError, match=message):
        binding.validate_sources(SimpleNamespace(strategy=None), selected_author(config), settings)
    with pytest.raises(ValueError, match=message):
        EconomicModel(config, settings, None, None)
