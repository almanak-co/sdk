"""V4 retains concrete records while all components consume one fund inventory."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.composed.authority import declared, validate
from almanak.connectors.composed.economic_config import EconomicConfig, V4Policy
from almanak.connectors.composed.economic_inventory import initial, refresh_position
from almanak.connectors.composed.execution import validate_inventory
from almanak.connectors.composed.inventory import initial_owned, refresh
from almanak.connectors.composed.models import OwnedParameters, wallet_key
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.uniswap_v4.backtest import HistoricalPosition
from almanak.connectors.uniswap_v4.state import Ownership, Swap
from almanak.effects import AssetDelta, apply_assets
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount
from tests.composed_public import economic_inputs
from tests.lending_captures import read_capture
from tests.uniswap_support import scope_only_json

ROOT = Path(__file__).parents[1]
REFERENCE = ROOT / "strategies/base_borrow_and_trade_v4"


@pytest.fixture
def inputs():
    _, config, settings, interval = economic_inputs(REFERENCE.name)
    return config, settings, interval


def test_concrete_policy_and_parameters_survive_serialization(inputs):
    config, settings, _ = inputs
    assert type(settings.models[ConnectorId("uniswap_v4")]) is V4Policy
    assert EconomicConfig.model_validate_json(settings.model_dump_json()) == settings
    assert OwnedParameters.model_validate_json(config.model_dump_json()) == config
    capabilities = declared(config)
    validate(config, capabilities)
    assert {
        str(c.target.pool.kind) for c in capabilities.requirements if hasattr(c.target, "pool")
    } == {"uniswap_v4"}


def test_borrowed_cash_projects_into_v4_without_duplicate_ownership(inputs):
    config, _, _ = inputs
    owned = initial_owned(config)
    assert isinstance(owned[-1], Ownership)
    target = config.target("trade")
    key = wallet_key(target, config.target("loan").asset)
    before = next(b.amount for b in config.inventory if b.key == key)
    changed = apply_assets(config.inventory, (AssetDelta(key=key, amount=5_000_000),))
    views = refresh(owned, changed)
    assert views[:2] == owned[:2]
    assert views[-1].inventory1.amount == before + 5_000_000
    assert len(changed) == len(config.inventory) == 2


def test_v4_swap_cannot_spend_unowned_measured_cash(inputs):
    config, _, _ = inputs
    target = config.target("trade")
    available = next(
        b.amount for b in config.inventory if b.key == wallet_key(target, target.funding_currency)
    )
    operation = Swap(
        target=target,
        amount=AssetAmount(asset=target.funding_currency, amount=available + 1),
        minimum=AssetAmount(asset=target.counter_currency, amount=1),
        deadline=2**32,
    )
    with pytest.raises(DataError, match="retained fund inventory"):
        validate_inventory(SimpleNamespace(inventory=config.inventory), operation)


def test_prepared_position_preserves_v4_allowance_and_cash_views(inputs):
    config, settings, interval = inputs
    state = initial(settings, config, interval)
    v4 = next(p for p in state.portfolio.positions if p.key.connector == ConnectorId("uniswap_v4"))
    assert isinstance(v4.payload, HistoricalPosition)
    assert v4.payload.allowances and not any(a.amount for a in v4.payload.allowances)
    target = v4.payload.target
    key = wallet_key(target, target.funding_currency)
    changed = apply_assets(state.portfolio.balances, (AssetDelta(key=key, amount=5_000_000),))
    projected = refresh_position(v4, changed)
    assert projected.key == v4.key
    assert projected.payload.allowances == v4.payload.allowances
    assert (
        projected.payload.owned.inventory1.amount == v4.payload.owned.inventory1.amount + 5_000_000
    )


def captured_decision():
    import gzip
    import json

    path = ROOT / "tests/fixtures/composed-v4-mixed-decision.json.gz"
    return json.loads(gzip.decompress(path.read_bytes()))["record"]


def test_actual_v4_decision_roundtrips_mixed_fork_observations():
    import json

    from almanak.connectors.aave_v3.observations import Snapshot as LendingSnapshot
    from almanak.connectors.composed.codec import codec
    from almanak.connectors.uniswap_v4.state import DecisionRecord, Snapshot

    raw = json.dumps(captured_decision())
    record = codec().decision_record.model_validate_json(raw)
    assert type(record) is DecisionRecord
    assert tuple(type(o) for o in record.observations) == (
        LendingSnapshot,
        LendingSnapshot,
        Snapshot,
    )
    assert DecisionRecord.model_validate_json(record.model_dump_json()) == record


@pytest.mark.parametrize("mutation", ["allocation", "chain", "environment", "missing", "duplicate"])
def test_v4_decision_rejects_mismatched_observation_scope(mutation):
    import json

    from almanak.connectors.uniswap_v4.state import DecisionRecord

    raw = captured_decision()
    if mutation == "allocation":
        raw["intent"]["target"]["allocation"] = "another-allocation"
    elif mutation == "chain":
        raw["intent"]["target"]["fund"]["chain"]["chain_id"] = "eip155:1"
    elif mutation == "environment":
        raw["intent"]["target"]["fund"]["chain"]["environment"] = "anvil:another-fork"
    elif mutation == "missing":
        raw["observations"].pop()
    else:
        raw["observations"].append(raw["observations"][-1])
    with pytest.raises(ValueError):
        DecisionRecord.model_validate_json(json.dumps(raw))


def test_returned_collateral_does_not_become_v4_profit():

    from almanak.connectors.composed.reporting import report
    from almanak.connectors.composed.state import Checkpoint

    path = ROOT / "tests/fixtures/composed-v4-closed-fork.json.gz"
    state = Checkpoint.model_validate_json(scope_only_json(read_capture(path)))
    assert state.mode == "closed"
    collateral = state.config.target("collateral").asset
    cash = next(b for b in state.inventory if b.key.asset == collateral)
    assert cash.amount >= state.config.allocation("collateral").capital
    projected = report(state)
    exchange = projected["positions"][-1]
    assert exchange["owned"]["inventory0"]["amount"] == cash.amount
    assert exchange["principal"] and exchange["uncollected_fees"]
    assert "pnl_excluding_gas" not in exchange
    assert "fund_value_excluding_gas" not in exchange
    assert "premint_hold_benchmark" not in exchange and "lp_minus_hold" not in exchange
    assert len(projected["inventory"]) == len(state.inventory) == 2
