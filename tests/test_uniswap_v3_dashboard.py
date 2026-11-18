"""Read-only dashboard components retain exchange targets and explicit evidence gaps."""

from types import SimpleNamespace

import pytest

from almanak.caip import AssetType
from almanak.connectors.dex_dashboard import transactions_dataset
from almanak.connectors.uniswap_v3.dashboard import (
    inventory_dataset,
    payer_values,
    position_values,
)
from almanak.connectors.uniswap_v3.planning import operation as plan
from almanak.dashboard.models import Context, Ref, Value
from almanak.gateway.evm import native_asset
from almanak.strategy_api import Memory
from tests.test_uniswap_v3_validation import state_for


@pytest.mark.parametrize("index", [0, 1])
def test_inventory_dashboard_roundtrip_preserves_exact_v3_target(index):
    state, before, _ = state_for(chain=index)
    state.baseline, state.closure, state.initialized = (before,), (), True
    values = {}
    position = position_values(state, before.owned, values)
    dataset = payer_values(state, values)
    context = Context(
        config=state.config,
        memory=Memory(),
        positions=(position,),
        values=values,
        datasets={"payer_costs": dataset},
    )
    assert Context.model_validate_json(context.model_dump_json()) == context
    assert position.target == before.target
    assert position.inventory[0].block == before.block
    assert position.inventory[0].amount == str(before.owned.inventory0.amount)
    assert values["payer0-gas"].scope.payer == before.payer
    with pytest.raises(ValueError, match="different fund chain"):
        Value.model_validate(
            {**position.inventory[0].model_dump(), "asset": AssetType("eip155:10/slip44:60")}
        )


def test_unobserved_dashboard_labels_configured_inventory_and_missing_gas():
    state, before, _ = state_for()
    state.baseline, state.closure, state.initialized = (), (), False
    values = {}
    position = position_values(state, before.owned, values)
    payer_values(state, values)
    assert all(v.evidence == "configured" and v.block is None for v in position.inventory)
    assert values["payer0-gas"].amount is None
    assert values["payer0-gas"].reason == "No initialized receipt ledger."


def test_shared_payer_costs_are_aggregated_at_submitter_scope():
    state, before, _ = state_for()
    binding = state.config.accounts.bindings[0]
    state.config = SimpleNamespace(
        accounts=SimpleNamespace(bindings=(binding, binding.model_copy(update={"name": "other"}))),
        asset_metadata={},
    )
    state.initialized = True
    state.outcomes = tuple(
        SimpleNamespace(
            execution=SimpleNamespace(submitter=before.payer),
            gas_paid=SimpleNamespace(amount=amount),
            after=before,
        )
        for amount in (3, 5)
    )
    values = {}
    dataset = payer_values(state, values)
    assert len(dataset.rows) == 1
    assert dataset.rows[0]["accounts"] == f"{binding.name}, other"
    assert values["payer0-gas"].amount == "8"
    assert values["payer0-gas"].scope.target is None


def test_inventory_lists_each_token_by_symbol_and_names_accounts_only_when_several():
    state, before, _ = state_for()
    state.baseline, state.closure, state.initialized = (before,), (), True
    position = position_values(state, before.owned, {})
    single = inventory_dataset((position,))
    assert [c.label for c in single.columns] == ["Position", "Token", "Amount"]
    assert [r["token"] for r in single.rows] == [
        v.symbol or str(v.asset) for v in position.inventory
    ]
    assert [r["amount"] for r in single.rows] == [Ref(ref=v.id) for v in position.inventory]
    other = position.model_copy(
        update={"target": position.target.model_copy(update={"account": "other"})}
    )
    several = inventory_dataset((position, other))
    assert [c.label for c in several.columns] == ["Position", "Fund account", "Token", "Amount"]
    assert {r["account"] for r in several.rows} == {position.target.account, "other"}


def test_single_fund_gas_total_drops_the_accounts_column():
    state, before, _ = state_for()
    state.initialized, state.outcomes = True, ()
    dataset = payer_values(state, {})
    assert [c.key for c in dataset.columns] == ["payer", "gas"]
    assert set(dataset.rows[0]) == {"payer", "gas"}


def test_transactions_list_each_receipt_with_its_action_gas_and_time():
    state, before, intent = state_for()
    state.initialized = True
    empty = before.model_copy(
        update={"allowances": tuple(a.model_copy(update={"amount": 0}) for a in before.allowances)}
    )
    approval, swap = (
        plan(state.config, state, intent, empty),
        plan(state.config, state, intent, before),
    )
    gas = native_asset(before.payer.chain.chain_id)
    named = dict(zip(swap.target.assets, ("AAA", "BBB"), strict=True))
    state.config = SimpleNamespace(
        asset_metadata={
            asset: SimpleNamespace(symbol=symbol, decimals=6) for asset, symbol in named.items()
        }
    )
    state.outcomes = tuple(
        SimpleNamespace(
            operation=operation,
            tx_hash=f"0x{index:064x}",
            success=success,
            gas_paid=SimpleNamespace(asset=gas, amount=1000 * (index + 1)),
            after=before,
            execution=SimpleNamespace(submitter=before.payer),
        )
        for index, (operation, success) in enumerate(((approval, True), (swap, False)))
    )
    values = {}
    dataset = transactions_dataset(state, values)
    assert [c.label for c in dataset.columns] == ["Transaction", "Receipt gas", "Time"]
    approve, swapped = dataset.rows
    assert approve["transaction"] == f"Approve {named[approval.asset]}"
    spent, received = named[swap.amount.asset], named[swap.minimum.asset]
    assert swapped["transaction"] == f"Swap {spent} → {received} (reverted)"
    assert values[swapped["gas"].ref].amount == "2000"
    assert values[swapped["gas"].ref].symbol == "ETH"
    # Undeclared native gas still reads in whole ETH, not as base units beside "ETH".
    assert values[swapped["gas"].ref].decimals == 18
    assert approve["time"].endswith(" UTC")
    assert not dataset.truncated


def test_a_zero_approval_reads_as_a_revoke():
    from almanak.connectors.dex_dashboard import operation_label

    state, before, intent = state_for()
    empty = before.model_copy(
        update={"allowances": tuple(a.model_copy(update={"amount": 0}) for a in before.allowances)}
    )
    approval = plan(state.config, state, intent, empty)
    state.config = SimpleNamespace(
        asset_metadata={approval.asset: SimpleNamespace(symbol="AAA", decimals=6)}
    )
    assert operation_label(state, approval) == "Approve AAA"
    revoke = approval.model_copy(update={"amount": 0})
    assert operation_label(state, revoke) == "Revoke approval AAA"


def test_transactions_keep_the_newest_rows_and_say_so(monkeypatch):
    monkeypatch.setattr("almanak.dashboard.tables.MAX_TABLE_ROWS", 2)
    state, before, intent = state_for()
    swap = plan(state.config, state, intent, before)
    gas = native_asset(before.payer.chain.chain_id)
    state.outcomes = tuple(
        SimpleNamespace(
            operation=swap,
            tx_hash=f"0x{index:064x}",
            success=True,
            gas_paid=SimpleNamespace(asset=gas, amount=index),
            after=before,
            execution=SimpleNamespace(submitter=before.payer),
        )
        for index in range(3)
    )
    values = {}
    dataset = transactions_dataset(state, values)
    assert dataset.truncated and len(dataset.rows) == 2
    assert [values[r["gas"].ref].amount for r in dataset.rows] == ["1", "2"]


def test_transactions_name_the_position_when_several_trade():
    state, before, intent = state_for()
    swap = plan(state.config, state, intent, before)
    other = swap.model_copy(
        update={"target": swap.target.model_copy(update={"allocation": "other"})}
    )
    gas = native_asset(before.payer.chain.chain_id)
    state.outcomes = tuple(
        SimpleNamespace(
            operation=operation,
            tx_hash=f"0x{index:064x}",
            success=True,
            gas_paid=SimpleNamespace(asset=gas, amount=1),
            after=before,
            execution=SimpleNamespace(submitter=before.payer),
        )
        for index, operation in enumerate((swap, other))
    )
    dataset = transactions_dataset(state, {})
    assert [c.label for c in dataset.columns] == ["Position", "Transaction", "Receipt gas", "Time"]
    assert [r["allocation"] for r in dataset.rows] == [swap.target.allocation, "other"]
    assert {r["transaction"] for r in dataset.rows} == {"Swap"}


def test_transactions_name_the_position_when_several_are_held_but_one_traded():
    state, before, intent = state_for()
    swap = plan(state.config, state, intent, before)
    held = before.owned
    state.owned = (
        held,
        held.model_copy(update={"target": held.target.model_copy(update={"allocation": "other"})}),
    )
    state.outcomes = (
        SimpleNamespace(
            operation=swap,
            tx_hash="0x" + "1" * 64,
            success=True,
            gas_paid=SimpleNamespace(asset=native_asset(before.payer.chain.chain_id), amount=1),
            after=before,
            execution=SimpleNamespace(submitter=before.payer),
        ),
    )
    dataset = transactions_dataset(state, {})
    assert dataset.columns[0].label == "Position"
    assert [r["allocation"] for r in dataset.rows] == [swap.target.allocation]
