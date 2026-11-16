"""Shared cash accounting component checks, separate from fork lifecycle evidence."""

import json
from types import SimpleNamespace

import pytest

from almanak.connectors.composed.authority import declared
from almanak.connectors.composed.inventory import initial_owned, ownership
from almanak.connectors.composed.models import Config, inventory_keys, wallet_key
from almanak.connectors.composed.state import Checkpoint
from almanak.effects import Balance
from almanak.gateway.evm import native_metadata
from almanak.gateway.production_config import ForkGasExecution, GasPolicy
from almanak.models import AssetAmount, Block
from almanak.strategy import StrategyDefinition
from almanak.triggers import DecisionSchedule
from tests.composed_public import resolved_parameters
from tests.test_composed_configuration import parameters


@pytest.fixture
def config():
    return forked(parameters())


def forked(source):
    """Fork-scoped composed configuration with an explicit zero inventory."""
    instance = "0x" + "34" * 32
    wire = json.loads(source.model_dump_json())

    def fork(value):
        if isinstance(value, dict):
            if "chain_id" in value:
                value["environment"] = "anvil:" + instance
            for child in value.values():
                fork(child)
        elif isinstance(value, list):
            for child in value:
                fork(child)

    fork(wire)
    scoped = type(source).model_validate_json(json.dumps(wire))
    inventory = tuple(Balance(key=k, amount=0) for k in sorted(inventory_keys(scoped), key=str))
    origin = Block(
        chain=source.accounts.bindings[0].profile.fund.chain,
        number=50972164,
        hash="0x" + "56" * 32,
        timestamp=1788710000,
    )
    return Config(
        **{name: getattr(scoped, name) for name in type(scoped).model_fields},
        inventory=inventory,
        fork_bindings=(
            {
                "chain": scoped.accounts.bindings[0].profile.fund.chain,
                "origin": origin,
                "instance": instance,
            },
        ),
    )


def checkpoint(config):
    return Checkpoint(
        config=config,
        strategy=StrategyDefinition(filename="strategy.py", source=""),
        capabilities=declared(config),
        permissions=None,
        schedule=DecisionSchedule(),
        deployment_id="inventory-component-test",
        mode="running",
        initialized=False,
        inventory=config.inventory,
        owned=initial_owned(config),
    )


def test_component_gateway_views_retain_top_level_fork_gas_policy():
    _, parameters = resolved_parameters("ethereum_aave_v4_borrow_and_trade")
    instance = "0x" + "34" * 32
    wire = json.loads(parameters.model_dump_json())

    def identify_fork(value):
        if isinstance(value, dict):
            if value.get("chain_id") == "eip155:1":
                value["environment"] = "anvil:" + instance
            for child in value.values():
                identify_fork(child)
        elif isinstance(value, list):
            for child in value:
                identify_fork(child)

    identify_fork(wire)
    scoped = type(parameters).model_validate_json(json.dumps(wire))
    binding = scoped.accounts.bindings[0]
    asset, _ = native_metadata(binding.profile.fund.chain.chain_id)
    policy = GasPolicy(
        accounts=(binding.name,),
        asset=asset,
        source="network_rpc_fee_history",
        reward_percentile=50,
        base_fee_multiplier_bps=20000,
        max_gas_price_wei=100,
        max_gas_limit=1000,
        max_transaction_fee=AssetAmount(asset=asset, amount=100_000),
        total_budget=AssetAmount(asset=asset, amount=1_000_000),
        exit_reserve=AssetAmount(asset=asset, amount=100_000),
        gas_limit_buffer_bps=2500,
    )
    admitted = ForkGasExecution(gas=(policy,))
    origin = Block(
        chain=parameters.accounts.bindings[0].profile.fund.chain,
        number=1,
        hash="0x" + "56" * 32,
        timestamp=1,
    )
    config = Config(
        **{name: getattr(scoped, name) for name in type(scoped).model_fields if name != "gas"},
        gas=(policy,),
        fork_gas=admitted,
        preparation_digest="0" * 64,
        fork_bindings=(
            {"chain": binding.profile.fund.chain, "origin": origin, "instance": instance},
        ),
    )
    assert all(
        config.component(identity).fork_gas == admitted for identity in config.parameter_components
    )


def cash_outcome(target, kind, amount, owned):
    return SimpleNamespace(
        success=True,
        operation=SimpleNamespace(target=target, kind=kind),
        assets=amount,
        after=SimpleNamespace(
            position=SimpleNamespace(
                supplied_scaled=owned.supply_scaled,
                debt_scaled=owned.debt_scaled + amount,
                collateral_enabled=owned.collateral_enabled,
            )
        ),
    )


def test_borrowed_cash_is_one_balance_and_restores_into_exchange_view(config):
    state = checkpoint(config)
    target = config.target("loan")
    trading = state.owned[-1]
    assert trading.inventory0.amount == trading.inventory1.amount == 0
    assert config.allocation("position").capital.amount > 0
    outcome = cash_outcome(target, "borrow", 100, state.owned[1])
    changed = state.model_copy(update=ownership(state, outcome))
    restored = Checkpoint.model_validate_json(changed.model_dump_json())
    assert restored == changed
    key = wallet_key(target, target.asset)
    assert [b.amount for b in restored.inventory if b.key == key] == [100]
    view = restored.owned[-1]
    assert (
        next(a.amount for a in (view.inventory0, view.inventory1) if a.asset == target.asset) == 100
    )


def test_partial_repayment_cannot_spend_unowned_cash(config):
    state = checkpoint(config)
    outcome = cash_outcome(config.target("loan"), "repay", 1, state.owned[1])
    with pytest.raises(ValueError, match="insufficient"):
        ownership(state, outcome)
    assert all(b.amount == 0 for b in state.inventory)


def repayment(target, amount, *, full=True):
    from almanak.connectors.aave_v3.operations import RepayOperation

    return RepayOperation(
        target=target, amount=AssetAmount(asset=target.asset, amount=amount), full=full
    )


def borrowed(config, amount=100):
    state = checkpoint(config)
    loan = config.target("loan")
    state = state.model_copy(
        update=ownership(state, cash_outcome(loan, "borrow", amount, state.owned[1]))
    )
    return state, loan, wallet_key(loan, loan.asset)


def debt_observation(loan, debt, wallet):
    position = SimpleNamespace(
        debt_assets=debt, reserve=SimpleNamespace(variable_borrow_rate_ray=0)
    )
    return SimpleNamespace(
        allocation="loan", target=loan, block=None, position=position, wallet_balance=wallet
    )


def repaid(loan, assets, *, success=True):
    return SimpleNamespace(
        success=success,
        operation=repayment(loan, assets),
        assets=assets,
        after=SimpleNamespace(
            position=SimpleNamespace(supplied_scaled=0, debt_scaled=0, collateral_enabled=False)
        ),
    )


def retained(state, key):
    return next(b.amount for b in state.inventory if b.key == key)


def test_full_repayment_spends_a_top_up_and_absorbs_it_into_the_inventory(config):
    from almanak.connectors.composed.execution import validate_inventory
    from almanak.connectors.composed.lifecycle import repayment_coverage
    from almanak.connectors.composed.reporting import report
    from almanak.gateway.ethereum import DataError

    state, loan, key = borrowed(config)
    # Interest took the debt to 105 against 100 retained; the operator then sends 5.
    with pytest.raises(DataError, match="needs 5 more than the fund's 100"):
        validate_inventory(state, repayment(loan, 100), debt_observation(loan, 105, 100))
    [short] = repayment_coverage(state, (debt_observation(loan, 105, 100),))
    assert (short.available, short.shortfall) == (100, 5)
    validate_inventory(state, repayment(loan, 105), debt_observation(loan, 105, 105))
    [covered] = repayment_coverage(state, (debt_observation(loan, 105, 105),))
    assert (covered.available, covered.shortfall) == (105, 0)
    # Only a full repay may spend the top-up; other spending stays within the inventory.
    with pytest.raises(DataError, match="retained fund inventory"):
        validate_inventory(state, repayment(loan, 105, full=False))
    partial = SimpleNamespace(success=True, operation=repayment(loan, 105, full=False), assets=105)
    with pytest.raises(ValueError, match="insufficient"):
        ownership(state, partial)

    changed = state.model_copy(update=ownership(state, repaid(loan, 105)))
    restored = Checkpoint.model_validate_json(changed.model_dump_json())
    assert restored == changed
    assert retained(restored, key) == 0
    assert restored.owned[1].debt_scaled == 0
    # The absorbed amount is retained evidence: configured inventory, plus the borrow,
    # plus the top-up, minus the receipt reconstructs the ledger.
    assert [(b.key, b.amount) for b in restored.top_ups] == [(key, 5)]
    [row] = report(restored)["repayment_top_ups"]
    assert (row["amount"], row["evidence"]) == ("5", "receipt")
    assert "repayment_top_ups" not in report(state)
    assert ownership(state, repaid(loan, 105, success=False))["inventory"] == state.inventory


def test_full_repayment_within_the_inventory_absorbs_nothing(config):
    state, loan, key = borrowed(config)
    changed = state.model_copy(update=ownership(state, repaid(loan, 60)))
    assert retained(changed, key) == 40
    assert changed.top_ups == ()
    assert "top_ups" not in Checkpoint.model_validate_json(changed.model_dump_json()).model_dump()


def test_absorbed_top_up_refreshes_the_exchange_view_that_shares_the_key(config, monkeypatch):
    from almanak.connectors.composed import inventory as shared

    state, loan, key = borrowed(config)
    trading = config.target("position")
    assert key == wallet_key(trading, loan.asset)
    changed = state.model_copy(update=ownership(state, repaid(loan, 130)))
    view = changed.owned[-1]
    assert next(a.amount for a in (view.inventory0, view.inventory1) if a.asset == loan.asset) == 0
    assert [b.amount for b in changed.top_ups] == [30]
    # The receipt left an empty wallet; the ledger stays within it, and cash credited
    # without a matching wallet balance is still refused.
    empty = {balance.key: (None, 0) for balance in changed.inventory}
    monkeypatch.setattr(shared, "cash_evidence", lambda observations: empty)
    shared.validate_observed(changed, ())
    credited = tuple(
        b.model_copy(update={"amount": 1}) if b.key == key else b for b in changed.inventory
    )
    with pytest.raises(ValueError, match="exceeds its measured wallet"):
        shared.validate_observed(changed.model_copy(update={"inventory": credited}), ())


def test_runtime_cannot_rewrite_absorbed_top_ups_without_execution_evidence(config):
    from almanak.gateway.admission import validate_execution_update

    state, loan, key = borrowed(config)
    state = state.model_copy(update=ownership(state, repaid(loan, 105)))
    assert ownership(state)["top_ups"] == state.top_ups
    for top_ups in ((), (Balance(key=key, amount=6),)):
        with pytest.raises(ValueError, match="verified execution evidence"):
            validate_execution_update(None, state, state.model_copy(update={"top_ups": top_ups}))


@pytest.mark.parametrize("mutation", ["duplicate", "unknown"])
def test_checkpoint_rejects_top_ups_outside_the_shared_inventory(config, mutation):
    from almanak.caip import AssetType

    state, loan, key = borrowed(config)
    other = key.model_copy(update={"asset": AssetType(f"{key.asset.chain}/erc20:0x" + "77" * 20)})
    keys = (key, key) if mutation == "duplicate" else (other,)
    wire = state.model_copy(
        update={"top_ups": tuple(Balance(key=k, amount=1) for k in keys)}
    ).model_dump_json()
    with pytest.raises(ValueError, match="absorbed top-ups"):
        Checkpoint.model_validate_json(wire)


def test_checkpoint_rejects_currency_view_that_invents_capital(config):
    state = checkpoint(config)
    view = state.owned[-1]
    changed = view.model_copy(
        update={"inventory0": view.inventory0.model_copy(update={"amount": 1})}
    )
    wire = state.model_copy(update={"owned": (*state.owned[:-1], changed)}).model_dump_json()
    with pytest.raises(ValueError, match="shared fund inventory"):
        Checkpoint.model_validate_json(wire)


@pytest.mark.parametrize("mutation", ["duplicate", "missing"])
def test_inventory_requires_exact_unique_wallet_keys(config, mutation):
    values = config.inventory
    inventory = (*values, values[0]) if mutation == "duplicate" else values[:-1]
    wire = config.model_copy(update={"inventory": inventory}).model_dump_json()
    with pytest.raises(ValueError, match="inventory"):
        Config.model_validate_json(wire)


def test_swap_consumes_borrowed_cash_once_and_updates_both_currency_views(config):
    from almanak.connectors.composed.execution import validate_inventory
    from almanak.connectors.uniswap_v3.operations import Swap
    from almanak.models import AssetAmount

    state = checkpoint(config)
    loan = config.target("loan")
    state = state.model_copy(
        update=ownership(state, cash_outcome(loan, "borrow", 100, state.owned[1]))
    )
    target = config.target("position")
    operation = Swap(
        target=target,
        amount=AssetAmount(asset=loan.asset, amount=100),
        minimum=AssetAmount(asset=target.counter_currency, amount=1),
        deadline=1788710300,
    )
    validate_inventory(state, operation)
    deltas = tuple(-100 if asset == loan.asset else 2 for asset in target.assets)
    outcome = SimpleNamespace(
        success=True,
        operation=operation,
        delta0=deltas[0],
        delta1=deltas[1],
        after=SimpleNamespace(owned=state.owned[-1].model_copy(update={"entry_complete": True})),
    )
    changed = state.model_copy(update=ownership(state, outcome))
    restored = Checkpoint.model_validate_json(changed.model_dump_json())
    assert restored.owned[-1].entry_complete
    assert {
        a.asset: a.amount for a in (restored.owned[-1].inventory0, restored.owned[-1].inventory1)
    } == {
        loan.asset: 0,
        target.counter_currency: 2,
    }
    with pytest.raises(ValueError, match="insufficient"):
        ownership(restored, outcome)
    with pytest.raises(Exception, match="retained fund inventory"):
        validate_inventory(restored, operation)


def test_failed_operation_does_not_invent_cash_or_mutate_positions(config):
    state = checkpoint(config)
    outcome = SimpleNamespace(
        success=False, operation=SimpleNamespace(target=config.target("loan"))
    )
    assert ownership(state, outcome) == ownership(state)


def test_copied_configuration_rebuilds_views_without_stale_targets(config):
    from almanak.connectors.identifiers import ConnectorId

    identity = ConnectorId("aave_v3")
    old = config.component(identity)
    changed = config.model_copy(
        update={
            "allocations": (
                config.allocations[0].model_copy(update={"capital": 1}),
                *config.allocations[1:],
            )
        }
    )
    assert changed.component(identity) is not old
    assert changed.component(identity).allocations[0].capital == 1
    assert old.allocations[0].capital != 1
