"""Every connector's dashboard tables share their conventions: tokens by symbol, a fund
account named only when there are several, one row per committed transaction."""

from types import SimpleNamespace

from almanak.caip import AssetType
from almanak.connectors.aave_v3.dashboard import project as aave_v3_project
from almanak.connectors.aave_v4.dashboard import project as aave_v4_project
from almanak.connectors.gmx_v2.dashboard import gmx_label
from almanak.dashboard.models import Column, Ref
from almanak.dashboard.tables import named_accounts, token_dataset, transactions_dataset
from almanak.gateway.evm import native_asset
from almanak.models import AssetMetadata
from tests.aave_v3_helpers import recorded_v3 as recorded_v3
from tests.test_dashboard import dashboard_state as dashboard_state
from tests.test_uniswap_v3_validation import state_for

WETH = AssetType("eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1")
USDC = AssetType("eip155:42161/erc20:0xaf88d065e77c8cc2239327c5edb3a432268e5831")


def test_account_column_only_when_rows_name_several():
    columns = (Column(key="account", label="Fund account"), Column(key="x", label="X"))
    single, rows = named_accounts(columns, ({"account": "fund", "x": "1"},))
    assert [c.key for c in single] == ["x"] and rows == ({"x": "1"},)
    several, rows = named_accounts(
        columns, ({"account": "a", "x": "1"}, {"account": "b", "x": "2"})
    )
    assert [c.key for c in several] == ["account", "x"] and rows[1]["account"] == "b"


def test_fund_balances_list_tokens_without_a_position_column():
    dataset = token_dataset(
        ({"account": "fund", "token": "USDC", "amount": Ref(ref="cash")},),
        "Fund cash.",
        position=False,
    )
    assert [c.label for c in dataset.columns] == ["Token", "Amount"]


def outcome(before, operation, index, *, gas=True, success=True):
    return SimpleNamespace(
        operation=operation,
        tx_hash=f"0x{index:064x}",
        success=success,
        gas_paid=SimpleNamespace(asset=native_asset(before.payer.chain.chain_id), amount=index + 1)
        if gas
        else None,
        after=before,
        execution=SimpleNamespace(submitter=before.payer),
    )


def test_transactions_name_each_row_skip_keeper_receipts_and_add_extra_columns():
    state, before, intent = state_for()
    from almanak.connectors.uniswap_v3.planning import operation as plan

    swap = plan(state.config, state, intent, before)
    state.outcomes = (
        outcome(before, swap, 0),
        outcome(before, swap, 1, gas=False),
        outcome(before, swap, 2, success=False),
    )
    values = {}
    dataset = transactions_dataset(
        state,
        values,
        lambda _, o: "Did " + o.tx_hash[-1],
        extra=((Column(key="hash", label="Hash"), lambda o, _: o.tx_hash),),
    )
    assert [c.label for c in dataset.columns] == ["Transaction", "Hash", "Receipt gas", "Time"]
    assert [r["transaction"] for r in dataset.rows] == ["Did 0", "Did 2 (reverted)"]
    assert [values[r["gas"].ref].amount for r in dataset.rows] == ["1", "3"]
    assert dataset.rows[0]["time"].endswith(" UTC") and not dataset.truncated


def test_lending_transactions_read_in_lending_terms(recorded_v3):
    context, _ = aave_v3_project(recorded_v3)
    dataset = context.datasets["transactions"]
    labels = [r["transaction"] for r in dataset.rows]
    assert {"Approve WETH", "Supply WETH", "Borrow USDC", "Repay USDC"} <= set(labels)
    assert {"Use WETH as collateral", "Stop using WETH as collateral"} <= set(labels)
    assert dataset.columns[0].label == "Position"
    assert "account" not in {c.key for c in context.datasets["fund_costs"].columns}
    assert context.datasets["lending_positions"].columns[0].label == "Position"


def test_aave_v4_keeps_its_asset_effect_beside_the_shared_columns(dashboard_state):
    context, _ = aave_v4_project(dashboard_state)
    dataset = context.datasets["transactions"]
    assert [c.label for c in dataset.columns] == [
        "Transaction",
        "Asset effect",
        "Receipt gas",
        "Time",
    ]
    supply = next(r for r in dataset.rows if r["transaction"].startswith("Supply"))
    assert context.values[supply["assets"].ref].amount == "10000000000000000"


def gmx_state(metadata):
    return SimpleNamespace(
        config=SimpleNamespace(
            asset_metadata={
                asset: AssetMetadata(decimals=decimals, symbol=symbol)
                for asset, (symbol, decimals) in metadata.items()
            }
        )
    )


def gmx_order(order_type, size, held):
    target = SimpleNamespace(side="long", market=SimpleNamespace(index_asset=WETH))
    operation = SimpleNamespace(
        kind="gmx_create_order", order_type=order_type, size_delta_usd_e30=size, target=target
    )
    position = SimpleNamespace(size_usd_e30=held) if held is not None else None
    return SimpleNamespace(operation=operation, before=SimpleNamespace(position=position))


def test_gmx_orders_read_as_trades():
    state = gmx_state({WETH: ("ETH", 18), USDC: ("USDC", 6)})
    assert gmx_label(state, gmx_order(2, 10, None)) == "Open long ETH"
    assert gmx_label(state, gmx_order(2, 10, 5)) == "Increase long ETH"
    assert gmx_label(state, gmx_order(4, 10, 10)) == "Close long ETH"
    assert gmx_label(state, gmx_order(4, 4, 10)) == "Reduce long ETH"
    swap = SimpleNamespace(
        kind="gmx_create_swap",
        amount=SimpleNamespace(asset=USDC),
        minimum_output=SimpleNamespace(asset=WETH),
    )
    assert gmx_label(state, SimpleNamespace(operation=swap)) == "Swap USDC → ETH"
    claim = SimpleNamespace(operation=SimpleNamespace(kind="gmx_claim_funding"))
    assert gmx_label(state, claim) == "Claim funding"


def test_lending_amounts_carry_their_token_symbol(recorded_v3):
    context, _ = aave_v3_project(recorded_v3)
    amounts = [
        v
        for k, v in context.values.items()
        if k.endswith(("-supply", "-debt")) and v.unit == "base_units"
    ]
    assert amounts
    assert {v.symbol for v in amounts} == {"WETH", "USDC"}


def test_gmx_requests_read_fees_in_tokens_and_deadlines_in_utc():
    from almanak.connectors.gmx_v2.dashboard import request_dataset

    state = gmx_state({WETH: ("WETH", 18)})
    fund = SimpleNamespace(address="0xfund")
    state.requests = (
        SimpleNamespace(
            allocation="Main",
            key="0xkey",
            fund=fund,
            status="pending",
            execution_fee=SimpleNamespace(asset=WETH, amount=20_000_000_000_000_000),
            admission_deadline=1793782380,
        ),
    )
    (row,) = request_dataset(state).rows
    assert row["fee"] == "0.02 WETH"
    assert row["deadline"] == "2026-11-04 08:53 UTC"
    assert row["key"] == "0xkey"


def test_wrapped_native_reads_by_symbol_without_declared_metadata():
    from almanak.caip import AssetType
    from almanak.dashboard.tables import wrapped_native_symbol

    weth = AssetType("eip155:42161/erc20:0x82af49447d8a07e3bd95bd0d56f35241523fbab1")
    usdc = AssetType("eip155:42161/erc20:0xaf88d065e77c8cc2239327c5edb3a432268e5831")
    assert wrapped_native_symbol(weth) == "WETH"
    assert wrapped_native_symbol(usdc) is None


def test_several_configured_accounts_keep_the_account_column_when_one_holds_everything():
    from almanak.dashboard.models import Column
    from almanak.dashboard.tables import named_accounts

    columns = (Column(key="account", label="Fund account"), Column(key="token", label="Token"))
    rows = ({"account": "main", "token": "WETH"}, {"account": "main", "token": "USDC"})
    kept, _ = named_accounts(columns, rows, keep=True)
    dropped, trimmed = named_accounts(columns, rows)
    assert [c.key for c in kept] == ["account", "token"]
    assert [c.key for c in dropped] == ["token"] and "account" not in trimmed[0]
