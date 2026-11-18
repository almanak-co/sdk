"""Aave model refusal, ownership and accounting checks against retained market inputs."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.backtest.config import load_settings
from almanak.caip import ChainId
from almanak.connectors.aave_v4.backtest import (
    AaveModel,
    AavePosition,
    capability,
    initial_state,
    parameters,
    position_key,
    supply_effects,
    validate_active,
    wallet,
    withdrawal_effects,
)
from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.connectors.aave_v4.history import Market
from almanak.connectors.aave_v4.history_config import FixedCosts
from almanak.effects import Effects, Position, PositionChange
from almanak.gateway.ethereum import DataError
from almanak.models import Chain
from almanak.triggers import IntervalSelection

ROOT = Path(__file__).parents[1]


@pytest.fixture
def aave(reference_strategy):
    author = ForkConfig.model_validate_json(
        (ROOT / "tests/fixtures/author/aave_v4_weth_supply/config.json").read_text()
    )
    settings = load_settings(ROOT / "tests/fixtures/author/aave_v4_weth_supply/backtest.json")
    interval = IntervalSelection(configured=author.decision_trigger)
    config = parameters(settings, author.allocations, interval.effective)
    config = config.model_copy(update={"parameters": author.parameters})
    state = initial_state(settings, config, interval)
    fixture = json.loads((ROOT / "tests/fixtures/aave-v4-historical-market.json").read_text())
    market = Market.model_validate_json(json.dumps(fixture["markets"][0]))
    model = AaveModel(
        config,
        settings,
        reference_strategy,
        {market.reserve.source.chain: SimpleNamespace()},
    )
    return model, state, market


def apply_position(state, before, after, deltas):
    change = PositionChange[AavePosition](
        before=before, after=Position[AavePosition](key=before.key, payload=after)
    )
    portfolio = state.portfolio.apply(Effects[AavePosition](assets=deltas, positions=(change,)))
    return state.model_copy(update={"portfolio": portfolio})


def test_supply_and_withdraw_conserve_weth_and_keep_other_position(aave):
    _, state, market = aave
    before = state.portfolio.positions[0]
    after, deltas, assets, shares = supply_effects(state, before.payload, 10**16, market)
    supplied = apply_position(state, before, after, deltas)
    assert supplied.portfolio.balance(wallet(before.payload.target)) == 3 * 10**16
    assert supplied.portfolio.positions[1] == state.portfolio.positions[1]
    closed, deltas, assets_out, burned = withdrawal_effects(supplied, after, market)
    result = apply_position(supplied, supplied.portfolio.positions[0], closed, deltas)
    assert (
        result.portfolio.balance(wallet(before.payload.target)) == 4 * 10**16 - assets + assets_out
    )
    assert shares == burned and closed.shares == 0
    assert assets_out <= assets


@pytest.mark.parametrize("flag", ["paused", "inactive", "halted", "frozen", "cap"])
def test_historical_execution_conditions_refuse_supply(aave, flag):
    _, state, market = aave
    if flag in ("paused", "frozen"):
        market = market.model_copy(
            update={"reserve": market.reserve.model_copy(update={flag: True})}
        )
    elif flag == "inactive":
        market = market.model_copy(update={"active": False})
    elif flag == "halted":
        market = market.model_copy(update={"halted": True})
    else:
        market = market.model_copy(update={"add_cap": 0})
    with pytest.raises(ValueError):
        validate_active(market)
        supply_effects(state, state.portfolio.positions[0].payload, 10**16, market)


def test_liquidity_rounding_and_storage_limits(aave):
    _, state, market = aave
    before = state.portfolio.positions[0]
    after, deltas, _, _ = supply_effects(state, before.payload, 10**16, market)
    supplied = apply_position(state, before, after, deltas)
    illiquid = market.model_copy(update={"liquidity": 0, "added_assets": market.added_assets * 2})
    with pytest.raises(ValueError, match="liquidity"):
        withdrawal_effects(supplied, after, illiquid)
    with pytest.raises(ValueError, match="bounds"):
        supply_effects(
            state, before.payload, 10**16, market.model_copy(update={"added_shares": 2**120})
        )
    dust = after.model_copy(update={"shares": 0})
    with pytest.raises(ValueError, match="dust"):
        withdrawal_effects(supplied, dust, market)


def test_revision_scope_budget_and_fixed_cost_refusals(aave):
    model, state, market = aave
    model.validate_revision(market)
    revision = market.revisions[0].model_copy(update={"code_hash": "0x" + "ff" * 32})
    with pytest.raises(DataError, match="revision"):
        model.validate_revision(market.model_copy(update={"revisions": (revision,)}))
    model.validate_state(state)
    bad = state.portfolio.positions[0].model_copy(update={"key": state.portfolio.positions[1].key})
    with pytest.raises(ValueError, match="ownership"):
        model.validate_state(
            state.model_copy(
                update={
                    "portfolio": state.portfolio.model_copy(
                        update={"positions": (bad, *state.portfolio.positions[1:])}
                    )
                }
            )
        )
    before = state.portfolio.positions[0].payload
    # No cumulative supply budget: an entry beyond the allocation's capital is refused
    # only by the inventory it needs.
    held = state.portfolio.balance(wallet(before.target))
    model.validate_supply_entry(state, before, held)
    with pytest.raises(ValueError, match="inventory"):
        model.validate_supply_entry(state, before, held + 1)
    policy = FixedCosts(kind="fixed_native", approve=1, supply=2, withdraw=3, revoke=4)
    model.settings = model.settings.model_copy(update={"costs": policy})
    assert model.cost_effects(state, before.target, "supply")[0].amount == -2
    model.settings = model.settings.model_copy(
        update={"costs": policy.model_copy(update={"supply": 10**30})}
    )
    with pytest.raises(ValueError, match="funding"):
        model.cost_effects(state, before.target, "supply")
    assert position_key(before.target).chain == before.target.fund.chain


def test_other_aave_chains_require_their_own_model_proof():
    with pytest.raises(ValueError, match="historical support"):
        capability(Chain(chain_id=ChainId("eip155:42161"), environment="mainnet"))
