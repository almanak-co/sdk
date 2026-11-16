"""Entry funding requirements are pure parameter facts; preflight only compares them."""

from collections import namedtuple
from types import SimpleNamespace
from typing import Any

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak.caip import AssetType, ChainId
from almanak.connectors.composed.connector import connector as composed
from almanak.connectors.composition import installed_connectors
from almanak.connectors.compound_v3.execution import CompoundExecution
from almanak.connectors.funding import (
    TokenRequirement,
    capital_requirements,
    funding_rows,
    lending_requirements,
    summed,
)
from almanak.connectors.morpho_blue.author_inputs import AllocationInput as MorphoAllocationInput
from almanak.connectors.morpho_blue.domain import PARAMS_ABI, Market, MarketId, Reserve
from almanak.connectors.morpho_blue.execution import MorphoExecution
from almanak.models import AssetAmount, Chain

USDC = AssetType("eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
WETH = AssetType("eip155:1/erc20:0xc02aaa39a223e2b44c2edd18a0b6ec3fe9b69cfd")


def lending(account, asset, capital, borrow=False):
    return SimpleNamespace(
        account=account,
        reserve=SimpleNamespace(asset=asset),
        capital=capital,
        borrow=borrow,
    )


def test_every_connector_reports_requirements_without_observations():
    connectors = (*installed_connectors(), ("composed", composed()))
    assert all(callable(c.execution.requirements) for _, c in connectors)


def test_summed_totals_each_asset_once_in_first_seen_order():
    amounts = (
        AssetAmount(asset=USDC, amount=5),
        AssetAmount(asset=WETH, amount=2),
        AssetAmount(asset=USDC, amount=7),
    )
    assert summed(amounts, "basis") == (
        TokenRequirement(asset=USDC, amount=12, basis="basis"),
        TokenRequirement(asset=WETH, amount=2, basis="basis"),
    )


def test_rows_keep_a_missing_balance_distinct_from_zero():
    required = (
        TokenRequirement(asset=USDC, amount=10, basis="basis"),
        TokenRequirement(asset=WETH, amount=0, basis="basis"),
    )
    missing, zero = funding_rows(required, {WETH: 0})
    assert missing["observed_token_base_units"] is None and not missing["funded"]
    assert zero["observed_token_base_units"] == "0" and zero["funded"]
    assert missing["requirement"] == "basis"


def test_lending_requirement_is_the_capital_without_an_interest_buffer():
    # Interest is not prefunded: a borrowing leg requires nothing before entry.
    config = SimpleNamespace(
        positions=(
            lending("fund", WETH, capital=100),
            lending("fund", USDC, capital=0, borrow=True),
            lending("other", USDC, capital=50),
        )
    )
    assert [(r.asset, r.amount) for r in lending_requirements(config, "fund")] == [
        (WETH, 100),
        (USDC, 0),
    ]
    assert lending_requirements(config, "missing") == ()


def test_capital_requirement_sums_the_account_allocations():
    config = SimpleNamespace(
        allocations=(
            SimpleNamespace(account="fund", capital=AssetAmount(asset=USDC, amount=3)),
            SimpleNamespace(account="fund", capital=AssetAmount(asset=USDC, amount=4)),
            SimpleNamespace(account="other", capital=AssetAmount(asset=WETH, amount=9)),
        )
    )
    assert [(r.asset, r.amount) for r in capital_requirements(config, "fund")] == [(USDC, 7)]


def aave_v4_observation(account, balance, supplied=0):
    return SimpleNamespace(
        target=SimpleNamespace(account=account, asset=WETH),
        wallet_balance=balance,
        allowance=0,
        position=SimpleNamespace(supplied_shares=supplied, total_debt=0),
        drawn_shares=0,
        premium_shares=0,
        premium_offset_ray=0,
        collateral_enabled=False,
    )


def test_aave_v4_preflight_reads_only_the_requested_account():
    from almanak.connectors.aave_v4.preflight import preflight

    config = SimpleNamespace(
        allocations=(
            SimpleNamespace(account="fund", reserve=SimpleNamespace(asset=WETH), capital=10),
        ),
        debts=(),
    )
    foreign = aave_v4_observation("other", balance=100, supplied=5)
    tokens, pristine = preflight(config, "fund", (foreign, aave_v4_observation("fund", 4)))
    assert tokens[0]["observed_token_base_units"] == "4" and not tokens[0]["funded"]
    assert pristine
    tokens, pristine = preflight(config, "fund", (foreign,))
    assert tokens[0]["observed_token_base_units"] is None and not pristine


def uniswap_v4_observation(account, balance, liquidity=0):
    return SimpleNamespace(
        target=SimpleNamespace(account=account),
        wallet0=AssetAmount(asset=USDC, amount=balance),
        wallet1=AssetAmount(asset=WETH, amount=0),
        needs_cleanup=False,
        position_liquidity=liquidity,
    )


def test_uniswap_v4_preflight_reads_only_the_requested_account():
    from almanak.connectors.uniswap_v4.execution import UniswapExecution

    config = SimpleNamespace(
        allocations=(SimpleNamespace(account="fund", capital=AssetAmount(asset=USDC, amount=10)),)
    )
    foreign = uniswap_v4_observation("other", balance=100, liquidity=1)
    tokens, pristine = UniswapExecution.preflight(
        config, "fund", (foreign, uniswap_v4_observation("fund", 4))
    )
    assert tokens[0]["observed_token_base_units"] == "4" and not tokens[0]["funded"]
    assert pristine
    tokens, pristine = UniswapExecution.preflight(config, "fund", (foreign,))
    assert tokens[0]["observed_token_base_units"] is None and not pristine


LendingTarget = namedtuple("LendingTarget", "allocation account asset")


def lending_config(*positions):
    by_name = {p.name: p for p in positions}
    return SimpleNamespace(
        positions=positions,
        target=lambda name: LendingTarget(name, by_name[name].account, by_name[name].reserve.asset),
    )


def compound_observation(target, balance, principal=0):
    return SimpleNamespace(
        target=target,
        wallet_balance=balance,
        allowance=0,
        account=SimpleNamespace(principal=principal, collaterals=(SimpleNamespace(balance=0),)),
    )


def morpho_observation(target, balance, collateral=0):
    return SimpleNamespace(
        target=target,
        wallet_balance=balance,
        allowance=0,
        account=SimpleNamespace(
            position=SimpleNamespace(supply_shares=0, borrow_shares=0, collateral=collateral)
        ),
    )


@pytest.mark.parametrize(
    ("execution", "observe", "dirty"),
    [
        (CompoundExecution, compound_observation, {"principal": 1}),
        (MorphoExecution, morpho_observation, {"collateral": 1}),
    ],
)
def test_lending_preflight_compares_account_requirements_and_pristine_targets(
    execution, observe, dirty
):
    supply = lending("fund", USDC, capital=20, borrow=True)
    supply.name = "borrow"
    foreign = lending("other", USDC, capital=500)
    foreign.name = "foreign"
    config = lending_config(supply, foreign)
    own, other = config.target("borrow"), config.target("foreign")
    observations = (observe(own, 20), observe(other, 0))
    tokens, pristine = execution.preflight(config, "fund", observations)
    assert [(t["required_token_base_units"], t["funded"]) for t in tokens] == [("20", True)]
    assert pristine
    tokens, pristine = execution.preflight(config, "fund", (observe(own, 19, **dirty),))
    assert not tokens[0]["funded"] and not pristine
    assert not execution.preflight(config, "fund", (observe(other, 20),))[1]


def test_capital_is_the_whole_requirement_and_its_basis_names_it():
    earn = lending("fund", USDC, capital=500)
    earn.name = "earn"
    config = lending_config(earn)
    (requirement,) = MorphoExecution.requirements(config, "fund")
    assert (requirement.asset, requirement.amount) == (USDC, 500)
    assert requirement.basis == "configured allocation capital"
    tokens, pristine = MorphoExecution.preflight(
        config, "fund", (morpho_observation(config.target("earn"), 500),)
    )
    assert pristine and [(t["required_token_base_units"], t["funded"]) for t in tokens] == [
        ("500", True)
    ]
    tokens, _ = MorphoExecution.preflight(
        config, "fund", (morpho_observation(config.target("earn"), 499),)
    )
    assert [(t["required_token_base_units"], t["funded"]) for t in tokens] == [("500", False)]


BASE = ChainId("eip155:8453")
BASE_USDC = AssetType("eip155:8453/erc20:0x833589fcd6edb6e08f4c7c32d4f71b54bda02913")
BASE_WETH = AssetType("eip155:8453/erc20:0x4200000000000000000000000000000000000006")


def morpho_reserve():
    fields: dict[str, Any] = {
        "chain": Chain(chain_id=BASE, environment="mainnet"),
        "core": "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb",
        "loan_asset": BASE_USDC,
        "collateral_asset": BASE_WETH,
        "oracle": "0x0000000000000000000000000000000000000001",
        "irm": "0x0000000000000000000000000000000000000002",
        "lltv": 86 * 10**16,
    }
    draft = Market.model_construct(**fields)
    market_id = "0x" + keccak(encode([PARAMS_ABI], [draft.parameters])).hex()
    return Reserve(market=Market(**fields, market_id=MarketId(market_id)), asset=BASE_USDC)


def morpho_input(**amounts):
    return MorphoAllocationInput(
        market="earn",
        account="fund",
        minimum_health_factor="2",
        minimum_native_balance="0",
        **amounts,
    )


def resolve_morpho(**amounts):
    market = SimpleNamespace(
        selection=SimpleNamespace(chain=BASE),
        reserve=morpho_reserve(),
        asset=SimpleNamespace(decimals=6),
    )
    return morpho_input(**amounts).resolve("earn", market)


def test_morpho_capital_converts_at_the_market_precision_and_defaults_to_zero():
    explicit = resolve_morpho(capital="500")
    assert (explicit.capital, explicit.borrow) == (500 * 10**6, False)
    default = resolve_morpho()
    assert default.capital == 0
    assert "capital" not in morpho_input().model_dump()


@pytest.mark.parametrize("field", ["supply_limit", "borrow_limit", "entry_amount"])
def test_morpho_input_refuses_the_retired_budget_fields(field):
    with pytest.raises(ValueError, match=field):
        morpho_input(**{field: "1"})


def test_gmx_requirements_sum_position_capital_for_the_account():
    from almanak.connectors.gmx_v2.execution import requirements

    config = SimpleNamespace(
        allocations=(
            SimpleNamespace(account="fund", capital=AssetAmount(asset=WETH, amount=6)),
            SimpleNamespace(account="other", capital=AssetAmount(asset=WETH, amount=9)),
        )
    )
    assert [(r.asset, r.amount, r.basis) for r in requirements(config, "fund")] == [
        (WETH, 6, "configured position capital including the request fee")
    ]


def gas_policy(fee, reserve):
    return SimpleNamespace(
        max_transaction_fee=SimpleNamespace(amount=fee),
        exit_reserve=SimpleNamespace(amount=reserve),
    )


def test_native_entry_minimum_is_the_larger_of_entry_fees_and_allocation_threshold():
    from almanak.connectors.funding import (
        admission_reserve,
        entry_reserve,
        native_entry_minimum,
        payer_accounts,
    )

    shared, alone = SimpleNamespace(address="0xshared"), SimpleNamespace(address="0xalone")
    config = SimpleNamespace(
        accounts=SimpleNamespace(
            bindings=(
                SimpleNamespace(name="a", profile=SimpleNamespace(submitter=shared)),
                SimpleNamespace(name="b", profile=SimpleNamespace(submitter=shared)),
                SimpleNamespace(name="c", profile=SimpleNamespace(submitter=alone)),
            )
        ),
        positions=(
            SimpleNamespace(account="a", minimum_native_balance=3),
            SimpleNamespace(account="b", minimum_native_balance=40),
            SimpleNamespace(account="c", minimum_native_balance=900),
        ),
    )
    names = payer_accounts(config, shared)
    assert names == ("a", "b")
    assert admission_reserve(None) == 0 and admission_reserve(gas_policy(5, 10)) == 15
    # Each entry transaction pays its fee ceiling and must still leave exit_reserve.
    assert entry_reserve(gas_policy(5, 10), 4) == 30
    assert entry_reserve(gas_policy(5, 10), 0) == entry_reserve(gas_policy(5, 10), 1) == 15
    assert native_entry_minimum(gas_policy(5, 10), config, names, 1) == 40
    assert native_entry_minimum(gas_policy(5, 10), config, names, 7) == 45
    assert native_entry_minimum(gas_policy(50, 10), config, names, 4) == 210
    assert native_entry_minimum(None, config, names, 4) == 40
    assert native_entry_minimum(None, config, (), 0) == 0


def lending_position(account, collateral=None, borrow=False):
    position = SimpleNamespace(account=account, borrow=borrow)
    if collateral is not None:
        position.collateral = collateral
    return position


def test_lending_entry_counts_every_granted_entry_action():
    from almanak.connectors.funding import lending_entry_transactions

    config = SimpleNamespace(
        positions=(
            # Aave V3 collateral: approve, supply, enable collateral.
            lending_position("fund", collateral=True),
            # Aave V3 debt: supply is always granted, then borrow.
            lending_position("fund", collateral=False, borrow=True),
            # Compound V3 / Morpho collateral has no separate enablement.
            lending_position("other"),
        )
    )
    assert lending_entry_transactions(config, "fund") == 3 + 3
    assert lending_entry_transactions(config, "other") == 2
    assert lending_entry_transactions(config, "none") == 0


def test_every_connector_states_its_entry_transactions():
    from almanak.connectors.composed.execution import ComposedExecution

    for _, bound in installed_connectors():
        assert callable(bound.execution.entry_transactions)
    assert callable(ComposedExecution.entry_transactions)


def exchange_position(liquidity, capital=100, entry_swap_bps=10000, **values):
    return SimpleNamespace(
        account="fund",
        liquidity_enabled=liquidity,
        capital=AssetAmount(asset=WETH, amount=capital),
        entry_swap_bps=entry_swap_bps,
        **values,
    )


def test_exchange_entry_counts_every_transaction_through_the_mint():
    from almanak.connectors.aerodrome.execution import AerodromeExecution
    from almanak.connectors.uniswap_v3.execution import UniswapExecution as V3Execution
    from almanak.connectors.uniswap_v4.execution import UniswapExecution as V4Execution

    config = SimpleNamespace(positions=(exchange_position(False), exchange_position(True)))
    assert V3Execution.entry_transactions(config, "fund") == 2 + 5
    # The other currency's approvals add no exposure but pay their fees before the mint.
    assert V4Execution.entry_transactions(config, "fund") == 3 + 8
    assert AerodromeExecution.entry_transactions(config, "fund") == 2 + 5


def banded_executions():
    from almanak.connectors.aerodrome_slipstream.execution import SlipstreamExecution
    from almanak.connectors.pancakeswap_v3.execution import PancakeExecution
    from almanak.connectors.uniswap_v3.execution import UniswapExecution as V3Execution
    from almanak.connectors.uniswap_v4.execution import UniswapExecution as V4Execution

    return [
        (V3Execution, 2, 5),
        (PancakeExecution, 2, 5),
        (SlipstreamExecution, 2, 5),
        (V4Execution, 3, 8),
    ]


@pytest.mark.parametrize("execution,trade,mint", banded_executions())
def test_a_banded_zero_entry_allocation_counts_its_rebalance_swap(execution, trade, mint):
    # A band enters through one rebalance_quote swap even when entry_swap_bps offers none.
    banded = exchange_position(True, entry_swap_bps=0, band=object())
    assert execution.entry_transactions(SimpleNamespace(positions=(banded,)), "fund") == mint


@pytest.mark.parametrize("execution,trade,mint", banded_executions())
def test_a_fixed_range_zero_entry_allocation_counts_no_swap(execution, trade, mint):
    fixed = exchange_position(True, entry_swap_bps=0, band=None)
    assert execution.entry_transactions(SimpleNamespace(positions=(fixed,)), "fund") == mint - trade


@pytest.mark.parametrize("execution,trade,mint", banded_executions())
def test_a_banded_sliced_entry_counts_every_slice(execution, trade, mint):
    banded = exchange_position(True, entry_swap_bps=2500, band=object())
    assert execution.entry_transactions(SimpleNamespace(positions=(banded,)), "fund") == (
        mint + 3 * trade
    )


def test_curve_liquidity_entry_approves_every_pool_coin():
    from almanak.connectors.curve.execution import CurveExecution

    def curve(liquidity, coins):
        return SimpleNamespace(
            account="fund",
            liquidity_enabled=liquidity,
            funding_asset=WETH,
            capital=SimpleNamespace(root=(AssetAmount(asset=WETH, amount=100),)),
            entry_swap_bps=10000,
            pool=SimpleNamespace(coins=coins),
        )

    two = SimpleNamespace(positions=(curve(True, ("a", "b")),))
    three = SimpleNamespace(positions=(curve(True, ("a", "b", "c")), curve(False, ("a", "b"))))
    assert CurveExecution.entry_transactions(two, "fund") == 5
    assert CurveExecution.entry_transactions(three, "fund") == 6 + 2


def test_entry_repeats_for_each_capital_slice():
    from almanak.connectors.funding import entry_slices
    from almanak.connectors.uniswap_v3.execution import UniswapExecution as V3Execution
    from almanak.connectors.uniswap_v4.execution import UniswapExecution as V4Execution

    assert entry_slices(100, 10000) == 1
    assert entry_slices(100, 2500) == 4
    assert entry_slices(100, 3000) == 4
    # A floored slice takes one more entry than 10000 / bps suggests.
    assert entry_slices(3, 5000) == 3
    # A zero slice is never offered, so it swaps nothing.
    assert entry_slices(0, 5000) == 0
    assert entry_slices(9999, 1) == 0
    trade = SimpleNamespace(positions=(exchange_position(False, entry_swap_bps=2500),))
    assert V3Execution.entry_transactions(trade, "fund") == 4 * 2
    assert V4Execution.entry_transactions(trade, "fund") == 4 * 3
    # Liquidity is offered the same slices before it mints.
    half = SimpleNamespace(positions=(exchange_position(True, entry_swap_bps=5000),))
    assert V3Execution.entry_transactions(half, "fund") == 7
    quarter = SimpleNamespace(positions=(exchange_position(True, entry_swap_bps=2500),))
    assert V3Execution.entry_transactions(quarter, "fund") == 4 * 2 + 3
    assert V4Execution.entry_transactions(quarter, "fund") == 4 * 3 + 5
    # Without a slice to swap, liquidity still approves and mints.
    unsliced = SimpleNamespace(positions=(exchange_position(True, entry_swap_bps=1, capital=9999),))
    assert V3Execution.entry_transactions(unsliced, "fund") == 3
    assert V4Execution.entry_transactions(unsliced, "fund") == 5
    assert (
        V3Execution.entry_transactions(
            SimpleNamespace(positions=(exchange_position(False, entry_swap_bps=1, capital=9999),)),
            "fund",
        )
        == 0
    )


def test_aave_v4_debt_entry_enables_each_collateral_then_borrows():
    from almanak.connectors.aave_v4.models import Allocation, DebtAllocation
    from almanak.connectors.aave_v4.preflight import entry_transactions

    supply = Allocation.model_construct(account="fund")
    debt = DebtAllocation.model_construct(account="fund", collateral=("a", "b"))
    config = SimpleNamespace(positions=(supply, debt))
    assert entry_transactions(config, "fund") == 2 + 3


def test_gmx_entry_approves_each_escrowed_input_then_requests():
    from almanak.connectors.gmx_v2.exchange_models import SwapAllocation
    from almanak.connectors.gmx_v2.execution import entry_transactions
    from almanak.connectors.gmx_v2.liquidity_models import LiquidityAllocation
    from almanak.connectors.gmx_v2.models import Allocation

    market = SimpleNamespace(long_asset="long", short_asset="short", wrapped_native="long")
    config = SimpleNamespace(
        positions=(
            Allocation.model_construct(account="perp"),
            SwapAllocation.model_construct(account="swap"),
            LiquidityAllocation.model_construct(account="gm", market=market),
        )
    )
    assert entry_transactions(config, "perp") == 2
    assert entry_transactions(config, "swap") == 3
    # Wrapped native is the long currency here, so two distinct inputs plus the deposit.
    assert entry_transactions(config, "gm") == 3


def test_composed_entry_sums_its_components_for_the_account(monkeypatch):
    from almanak.connectors.composed import execution

    components = {
        "fund": (("aave_v3", "lending"), ("uniswap_v3", "pool")),
    }
    counts = {"lending": 4, "pool": 2}
    monkeypatch.setattr(
        execution, "components_for_account", lambda config, account: components.get(account, ())
    )
    monkeypatch.setattr(
        execution,
        "connector_for",
        lambda view: SimpleNamespace(
            execution=SimpleNamespace(entry_transactions=lambda _, __: counts[view])
        ),
    )
    assert execution.entry_transactions(None, "fund") == 6
    assert execution.entry_transactions(None, "other") == 0


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        # Aave V3 collateral (approve, supply, enable) and loan (approve, supply, borrow),
        # then a Uniswap V3 trade (approve, swap).
        ("base_borrow_and_trade", {"fund": 3 + 3 + 2}),
        # The GMX swap approves its input and the wrapped-native fee, then requests.
        ("arbitrum_aave_borrow_and_trade_gmx", {"fund": 3 + 3 + 3}),
        ("ethereum_base_borrow_and_trade", {"ethereum": 8, "base": 8}),
    ],
)
def test_composed_payer_entry_sums_each_component_of_the_account(name, expected):
    from almanak.configuration.funding import payer_entries
    from tests.composed_public import resolved_parameters

    _, parameters = resolved_parameters(name)
    payers = {tuple(p["accounts"]): p["entry_transactions"] for p in payer_entries(parameters)}
    assert payers == {(account,): count for account, count in expected.items()}
