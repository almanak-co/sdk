"""`almanak support` gas lifecycle counts are the ones funding and check compute."""

from types import SimpleNamespace

import pytest

from almanak import reports
from almanak.caip import AssetType
from almanak.check_warnings import lifecycle_transactions
from almanak.connectors import composition
from almanak.connectors.composition import installed_connectors
from almanak.connectors.funding import payer_entry_transactions
from almanak.connectors.gmx_v2.exchange_models import SwapAllocation
from almanak.connectors.gmx_v2.liquidity_models import LiquidityAllocation as GmLiquidity
from almanak.connectors.gmx_v2.models import Allocation as GmxPosition
from almanak.models import AssetAmount

WETH = AssetType("eip155:1/erc20:0xc02aaa39a223e2b44c2edd18a0b6ec3fe9b69cfd")
SLICED_BPS = {1: 10000, 4: 2500}
EXCHANGES = ("uniswap_v3", "pancakeswap_v3", "aerodrome_slipstream", "uniswap_v4", "aerodrome")


def predicted(connector, kind, slices=1, items=None, granted=()):
    """A count from the printed table alone, by the rule its fields document."""
    [row, *_] = reports.support(connector)[connector]
    counts = row["gas_lifecycle"][kind]
    return (
        counts["entry"]
        + counts["per_entry_slice"] * (slices - 1)
        + sum(counts["per_item"][name] * n for name, n in (items or {}).items())
        + sum(counts["when_granted"][name] for name in granted)
    )


def exchange(liquidity, slices, **values):
    return SimpleNamespace(
        account="fund",
        liquidity_enabled=liquidity,
        capital=AssetAmount(asset=WETH, amount=100),
        entry_swap_bps=SLICED_BPS[slices],
        **values,
    )


def curve(liquidity, slices, coins):
    return SimpleNamespace(
        account="fund",
        liquidity_enabled=liquidity,
        funding_asset=WETH,
        capital=SimpleNamespace(root=(AssetAmount(asset=WETH, amount=100),)),
        entry_swap_bps=SLICED_BPS[slices],
        pool=SimpleNamespace(coins=coins),
    )


def lending(**granted):
    return SimpleNamespace(**{"account": "fund", "borrow": False, **granted})


def gm_market(*inputs):
    long, short, native = inputs
    return SimpleNamespace(long_asset=long, short_asset=short, wrapped_native=native)


def cases():
    for connector in EXCHANGES:
        for liquidity, kind in ((False, "trade"), (True, "liquidity")):
            for slices in SLICED_BPS:
                yield connector, (exchange(liquidity, slices),), [(kind, slices, {}, ())]
    for liquidity, kind in ((False, "trade"), (True, "liquidity")):
        for slices in SLICED_BPS:
            for coins in (("a", "b"), ("a", "b", "c")):
                items = {"coins": len(coins)} if liquidity else {}
                yield "curve", (curve(liquidity, slices, coins),), [(kind, slices, items, ())]
    for connector in ("aave_v3", "sparklend"):
        positions = (lending(collateral=True), lending(collateral=False, borrow=True))
        expected = [("lending", 1, {}, ("collateral",)), ("lending", 1, {}, ("borrow",))]
        yield connector, positions, expected
    for connector in ("compound_v3", "morpho_blue"):
        yield (
            connector,
            (lending(), lending(borrow=True)),
            [
                ("lending", 1, {}, ()),
                ("lending", 1, {}, ("borrow",)),
            ],
        )
    from almanak.connectors.aave_v4.models import Allocation, DebtAllocation

    yield (
        "aave_v4",
        (
            Allocation.model_construct(account="fund"),
            DebtAllocation.model_construct(account="fund", collateral=("a",)),
            DebtAllocation.model_construct(account="fund", collateral=("a", "b")),
        ),
        [
            ("lending", 1, {}, ()),
            ("debt", 1, {"collateral": 1}, ()),
            ("debt", 1, {"collateral": 2}, ()),
        ],
    )
    yield (
        "gmx_v2",
        (
            GmxPosition.model_construct(account="fund"),
            SwapAllocation.model_construct(account="fund"),
            GmLiquidity.model_construct(account="fund", market=gm_market("l", "s", "l")),
            GmLiquidity.model_construct(account="fund", market=gm_market("l", "s", "w")),
        ),
        [
            ("position", 1, {}, ()),
            ("swap", 1, {}, ()),
            ("liquidity", 1, {"input_assets": 2}, ()),
            ("liquidity", 1, {"input_assets": 3}, ()),
        ],
    )
    from almanak.connectors.hyperliquid.models import Allocation as PerpAllocation

    yield "hyperliquid", (PerpAllocation.model_construct(account="fund"),), [("perp", 1, {}, ())]


CASES = list(cases())


def test_every_installed_connector_has_a_case():
    assert {connector for connector, _, _ in CASES} == {
        str(name) for name, _ in installed_connectors()
    }


@pytest.mark.parametrize(("connector", "positions", "expected"), CASES)
def test_support_counts_equal_the_funding_entry_count(monkeypatch, connector, positions, expected):
    binding = {str(name): bound for name, bound in installed_connectors()}[connector]
    monkeypatch.setattr(composition, "connector_for", lambda config: binding)
    config = SimpleNamespace(positions=positions)
    assert payer_entry_transactions(config, ("fund",)) == sum(
        predicted(connector, *case) for case in expected
    )
    for position, case in zip(positions, expected, strict=True):
        single = SimpleNamespace(positions=(position,))
        assert payer_entry_transactions(single, ("fund",)) == predicted(connector, *case)


def check_inputs(connector, allocations=(), debts=()):
    return SimpleNamespace(
        strategy=SimpleNamespace(
            allocations={f"a{i}": s for i, s in enumerate(allocations)},
            debts={f"d{i}": s for i, s in enumerate(debts)},
        ),
        markets={"m": SimpleNamespace(connector=connector)},
    )


def check_shape(connector, kind):
    selection = SimpleNamespace(account="fund", market="m", kind=kind)
    if kind == "liquidity":
        selection.range = object()
    if kind == "debt":
        return lifecycle_transactions(check_inputs(connector, debts=(selection,)), ("fund",))
    return lifecycle_transactions(check_inputs(connector, (selection,)), ("fund",))


@pytest.mark.parametrize("connector", sorted(str(name) for name, _ in installed_connectors()))
def test_support_exits_are_check_counts_and_entries_never_below_them(connector):
    for row in reports.support(connector)[connector]:
        for kind, counts in row["gas_lifecycle"].items():
            entries, exits = check_shape(connector, kind)
            assert counts["exit"] == exits
            assert counts["entry"] >= entries


def test_support_json_carries_the_lifecycle_on_every_chain_row():
    rows = reports.support("uniswap_v3")["uniswap_v3"]
    assert rows
    for row in rows:
        assert row["gas_lifecycle"]["trade"] == {
            "entry": 2,
            "exit": 2,
            "per_entry_slice": 2,
            "per_item": {},
            "when_granted": {},
            "exit_per_item": {},
            "exit_when_granted": {},
            "exit_per_unwind_cycle": 0,
        }
        assert row["gas_lifecycle"]["liquidity"]["entry"] == 5


# Exits as the rig's fork verdicts record the teardown transactions:
# V3 pools: close, approve, swap, then a revoke per position-manager allowance the mint
# left: two in the B3 borrow-LP forks (l_borrow_lp_arb, abB3-tpl), one in a banded
# Slipstream fork (cmb-bandnoenv); the mint approved both currencies either way;
# V4: burn, approve_erc20, approve_permit2, swap, revoke_permit2 (m_dynlp_v4base, m_rh_v4lp);
# Aerodrome: approve shares, close, approve, swap, revoke (v3_aero_classic);
# Curve: close, approve, swap (v3_curve_arb).
EXITS = {
    "uniswap_v3": (2, 5),
    "pancakeswap_v3": (2, 5),
    "aerodrome_slipstream": (2, 5),
    "uniswap_v4": (4, 5),
    "aerodrome": (2, 5),
    "curve": (2, 3),
}


@pytest.mark.parametrize(("connector", "exits"), sorted(EXITS.items()))
def test_exchange_exits_count_the_whole_teardown(connector, exits):
    [row, *_] = reports.support(connector)[connector]
    lifecycle = row["gas_lifecycle"]
    assert (lifecycle["trade"]["exit"], lifecycle["liquidity"]["exit"]) == exits
    assert check_shape(connector, "trade")[1] == exits[0]
    assert check_shape(connector, "liquidity")[1] == exits[1]


def test_aerodrome_and_curve_liquidity_flags_count_as_liquidity_in_check():
    for connector, exits in (("aerodrome", (2, 5)), ("curve", (2, 3))):
        selection = SimpleNamespace(account="fund", market="m", liquidity_enabled=True)
        inputs = SimpleNamespace(
            strategy=SimpleNamespace(allocations={"a": selection}, debts={}),
            markets={"m": SimpleNamespace(connector=connector)},
        )
        assert lifecycle_transactions(inputs, ("fund",))[1] == exits[1]


def lifecycle(connector):
    [row, *_] = reports.support(connector)[connector]
    return row["gas_lifecycle"]


def exits_from_table(counts, items=None, granted=()):
    """An exit from the printed table alone, by the rule its fields document."""
    return (
        counts["exit"]
        + sum(counts["exit_per_item"].get(name, 0) * n for name, n in (items or {}).items())
        + sum(counts["exit_when_granted"].get(name, 0) for name in granted)
    )


POOL_LENDERS = ("aave_v3", "sparklend", "compound_v3", "morpho_blue")


@pytest.mark.parametrize("connector", POOL_LENDERS)
def test_a_borrow_exits_by_approving_repaying_and_revoking(connector):
    # Rig B3 fork verdicts (lev_op, spark, cbbtc, weth_compound): the loan's teardown is
    # approve, repay, revoke; the collateral's is one withdraw; planning.py full_repayment.
    lending = lifecycle(connector)["lending"]
    assert exits_from_table(lending) == 1
    assert exits_from_table(lending, granted=("borrow",)) == 3
    supply = SimpleNamespace(account="fund", market="m", borrow=False)
    loan = SimpleNamespace(account="fund", market="m", borrow=True)
    assert lifecycle_transactions(check_inputs(connector, (supply,)), ("fund",))[1] == 1
    assert lifecycle_transactions(check_inputs(connector, (loan,)), ("fund",))[1] == 3


@pytest.mark.parametrize("collateral", [("c",), ("c", "d")])
def test_an_aave_v4_debt_exit_also_disables_each_collateral(collateral):
    # short_avax fork: approve, repay, revoke, then setUsingAsCollateral(false) per collateral.
    debt = lifecycle("aave_v4")["debt"]
    assert exits_from_table(debt, items={"collateral": len(collateral)}) == 3 + len(collateral)
    selection = SimpleNamespace(account="fund", market="m", collateral=collateral)
    inputs = check_inputs("aave_v4", debts=(selection,))
    assert lifecycle_transactions(inputs, ("fund",))[1] == 3 + len(collateral)
    assert debt["exit_per_unwind_cycle"] == 0


# Teardown transactions of the rig's B3 leverage-loop forks on Aave V3 / Compound V3 with a
# V3 pool trade: one cycle (lev_op, spark, cbbtc, 1 round) and two (lev_op 3 rounds,
# weth_compound 1 round), each withdraw part, approve, swap, approve, repay part, ending
# in the full repayment's revoke and the last withdraw.
LOOP_TEARDOWNS = {1: 7, 2: 12}


@pytest.mark.parametrize("connector", ["aave_v3", "compound_v3"])
@pytest.mark.parametrize(("cycles", "observed"), sorted(LOOP_TEARDOWNS.items()))
def test_a_loop_unwound_in_cycles_exits_by_the_documented_cycle_rule(connector, cycles, observed):
    lending = lifecycle(connector)["lending"]
    trade = lifecycle("uniswap_v3")["trade"]["exit"]
    summed = exits_from_table(lending) + exits_from_table(lending, granted=("borrow",)) + trade
    assert summed == 6
    assert summed + 1 + (cycles - 1) * (lending["exit_per_unwind_cycle"] + trade) == observed


def test_an_exit_reserve_finding_names_the_cycle_term_only_for_a_paid_borrow():
    from almanak.check_warnings import unwind_cycle_note

    loan = SimpleNamespace(account="fund", market="m", borrow=True)
    supply = SimpleNamespace(account="fund", market="m", borrow=False)
    note = unwind_cycle_note(check_inputs("aave_v3", (supply, loan)), ("fund",))
    assert "adds 3 plus the swapping allocation's exit" in note
    assert unwind_cycle_note(check_inputs("aave_v3", (loan,)), ("other",)) == ""
    assert unwind_cycle_note(check_inputs("aave_v3", (supply,)), ("fund",)) == ""
    assert unwind_cycle_note(check_inputs("aave_v4", (loan,)), ("fund",)) == ""


def test_a_gmx_position_exit_approves_the_fee_orders_the_close_and_claims_funding():
    # dn_lp_gmx_arb-abB3m: gmx_approval, gmx_create_order, then the keeper's execution
    # (not through the fund's Roles module, so not the payer's), then gmx_claim_funding.
    position = lifecycle("gmx_v2")["position"]
    assert exits_from_table(position) == 3
    selection = SimpleNamespace(account="fund", market="m", kind="gmx_position")
    assert lifecycle_transactions(check_inputs("gmx_v2", (selection,)), ("fund",))[1] == 3
    swap = SimpleNamespace(account="fund", market="m", kind="gmx_swap")
    assert lifecycle_transactions(check_inputs("gmx_v2", (swap,)), ("fund",))[1] == 1
