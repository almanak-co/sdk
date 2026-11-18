"""Report components preserve hypothetical units and explicit measurement gaps."""

from types import SimpleNamespace

import pytest

from almanak.backtest.config import ExcludedCosts
from almanak.backtest.dataset import InputUnavailable
from almanak.connectors.uniswap_v3 import authoring
from almanak.connectors.uniswap_v3.economic_binding import UniswapEconomic
from almanak.connectors.uniswap_v3.economic_config import Costs, DataPolicy
from almanak.connectors.uniswap_v3.economic_data import key
from almanak.connectors.uniswap_v3.economic_reporting import payer_reports, report
from tests.test_uniswap_v3_economic_model import component, execute
from tests.test_uniswap_v3_prepared_acquisition import ROWS


def report_component(index=0):
    model, state = component(index)
    model.settings.costs = Costs(**vars(model.settings.costs))
    model.settings.data = DataPolicy.model_validate(ROWS[index]["data"])
    model.strategy = SimpleNamespace(sha256="a" * 64)
    model.book.baselines = {}
    return model, state


@pytest.mark.parametrize("index", [0, 1])
def test_report_retains_simulated_position_principal_and_pool_evidence(index):
    model, state = report_component(index)
    allocation = model.config.allocations[0]
    observed = model.observe(state)[0]
    state, _ = execute(model, state, authoring.swap(allocation, observed, observed.entry_quote))
    observed = model.observe(state)[0]
    state, _ = execute(model, state, authoring.open_liquidity(allocation, observed))
    result = UniswapEconomic.report(state, model)
    position = result["positions"][0]
    assert position["measurement_status"] == "modeled"
    assert position["ownership"]["position"]["kind"] == "simulated_liquidity"
    assert any(int(p["amount"]) > 0 for p in position["principal"])
    assert {p["asset"] for p in position["principal"]} == {str(a) for a in observed.target.assets}
    assert position["observation"]["pool"]["source"] == observed.target.pool.model_dump(mode="json")
    assert not result["closed"] and not result["missing_inputs"]


def test_unavailable_report_retains_ownership_without_measured_zero():
    model, state = report_component()
    target = model.config.target(model.config.allocations[0].name)

    def unavailable(target, time):
        raise InputUnavailable(key(target), time, "sample exceeded explicit age")

    model.book.at = unavailable
    result = report(state, model)
    position = result["positions"][0]
    assert position["ownership"] == state.portfolio.positions[0].payload.owned.model_dump(
        mode="json"
    )
    assert position["measurement_status"] == "unavailable"
    assert position["observation"] is None
    assert all(p["amount"] is None for p in position["principal"] + position["uncollected_fees"])
    assert result["missing_inputs"][0]["key"] == key(target).model_dump(mode="json")
    assert not result["closed"]


def test_payer_report_counts_successful_costs_and_marks_excluded_costs():
    model, state = report_component()
    observed = model.observe(state)[0]
    intent = authoring.swap(model.config.allocations[0], observed, observed.entry_quote)
    state, success = execute(model, state, intent)
    _, failure = execute(model, state, intent, latency=301)
    state = state.model_copy(
        update={"completed": tuple(SimpleNamespace(transition=t) for t in (success, failure))}
    )
    result = payer_reports(state, model)[0]
    assert result["cost"]["amount"] == "3"
    assert result["measurement_status"] == "assumed"
    assert result["fund_accounts"] == ["fund"]
    model.settings.costs = ExcludedCosts(kind="excluded")
    result = payer_reports(state, model)[0]
    assert result["cost"]["amount"] is None
    assert result["measurement_status"] == "excluded"
