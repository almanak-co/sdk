"""Offline checks of reference strategy configuration validators and exit sequencing.

Each strategy module is imported in-process and driven with small synthetic config
objects, so these checks need no retained evidence, provider or sandboxed worker.
"""

import importlib.util
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from almanak.configuration.strategy_parameters import StrategyParameters
from almanak.typed_parameters import ParametersRefused
from tests.reference_parameters import TypedHooks

STRATEGIES = Path(__file__).parents[1] / "strategies"


def load(name):
    spec = importlib.util.spec_from_file_location(
        f"offline_reference_{name}", STRATEGIES / name / "strategy.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return TypedHooks(module)


class Config(SimpleNamespace):
    def allocation(self, name):
        return next(a for a in self.positions if a.name == name)


def authored(name, config):
    """`config` with the reference's own `strategy.parameters`, as resolution passes them."""
    raw = yaml.safe_load((STRATEGIES / name / "config.yaml").read_text())
    parameters = raw["strategy"].get("parameters") or {}
    return Config(**{**vars(config), "parameters": parameters})


def ns(**fields):
    return SimpleNamespace(**fields)


def balance(account, asset, amount):
    return ns(key=ns(account=account, asset=asset), amount=amount)


def replace(allocation, **fields):
    return ns(**{**vars(allocation), **fields})


def with_allocation(config, allocation):
    positions = [allocation if a.name == allocation.name else a for a in config.positions]
    return Config(**{**vars(config), "positions": positions})


# Separate earning and borrowing funds over one Compound V3 or Morpho Blue market.

LENDING = (
    "compound_v3_base_lending",
    "compound_v3_ethereum_lending",
    "morpho_blue_base_lending",
    "morpho_blue_ethereum_lending",
)


def lending_reserve(base, market="market"):
    return ns(is_base=base, is_loan=base, market=market, asset="loan-asset" if base else "weth")


def lending_config():
    return Config(
        positions=[
            ns(
                name="earn",
                account="lender",
                reserve=lending_reserve(True),
                borrow=False,
                capital=10,
            ),
            ns(
                name="collateral",
                account="borrower",
                reserve=lending_reserve(False),
                borrow=False,
                capital=10,
            ),
            ns(
                name="loan",
                account="borrower",
                reserve=lending_reserve(True),
                borrow=True,
                capital=0,
            ),
        ],
        asset_metadata={"loan-asset": ns(decimals=6), "weth": ns(decimals=18)},
    )


def lending_refusals():
    config = lending_config()
    earn, collateral, loan = (config.allocation(n) for n in ("earn", "collateral", "loan"))
    return {
        "allocations": Config(**{**vars(config), "positions": [earn, collateral]}),
        "earn_shares_fund": with_allocation(config, replace(earn, account="borrower")),
        "collateral_elsewhere": with_allocation(config, replace(collateral, account="lender")),
        "earn_not_debt_asset": with_allocation(
            config, replace(earn, reserve=lending_reserve(False))
        ),
        "loan_not_debt_asset": with_allocation(
            config, replace(loan, reserve=lending_reserve(False))
        ),
        "collateral_is_debt_asset": with_allocation(
            config, replace(collateral, reserve=lending_reserve(True))
        ),
        "two_markets": with_allocation(
            config, replace(collateral, reserve=lending_reserve(False, "other"))
        ),
        "earn_borrows": with_allocation(config, replace(earn, borrow=True)),
        "loan_cannot_borrow": with_allocation(config, replace(loan, borrow=False)),
        "small_earn": with_allocation(config, replace(earn, capital=9)),
        "small_collateral": with_allocation(config, replace(collateral, capital=9)),
    }


@pytest.mark.parametrize("name", LENDING)
def test_lending_reference_accepts_separate_funds_on_one_market(name):
    load(name).validate_config(authored(name, lending_config()))


@pytest.mark.parametrize("name", LENDING)
def test_lending_reference_accepts_an_optional_loan_asset_reserve(name):
    config = lending_config()
    loan = config.allocation("loan")
    load(name).validate_config(authored(name, with_allocation(config, replace(loan, capital=1))))


@pytest.mark.parametrize("case", sorted(lending_refusals()))
@pytest.mark.parametrize("name", LENDING)
def test_lending_reference_refuses_each_invalid_shape(name, case):
    with pytest.raises(ValueError):
        load(name).validate_config(authored(name, lending_refusals()[case]))


class Actions:
    def __getattr__(self, kind):
        return lambda *args: (kind, *args)


@pytest.mark.parametrize("name", LENDING)
def test_lending_reference_exit_sequence_by_phase(name):
    module = load(name)

    def exit_at(phase):
        config = authored(name, lending_config())
        context = ns(config=config, actions=Actions(), memory=module.State(phase=phase))
        return module.exit_action(context)

    quantity = authored(name, lending_config()).parameters["borrow_quantity"]
    borrowed = int(Decimal(quantity).scaleb(6))
    assert [exit_at(phase) for phase in range(3, 10)] == [
        ("repay_amount", "loan", borrowed // 2),
        ("withdraw_amount", "collateral", 1),
        ("withdraw_amount", "earn", 1),
        ("repay", "loan"),
        ("withdraw", "collateral"),
        ("withdraw", "earn"),
        None,
    ]


# Same-fund Aave borrowing: collateral and debt on one chain and Pool.

AAVE_LENDING_PAIR = (
    "arbitrum_aave_borrow_and_trade",
    "arbitrum_aave_curve_borrow_and_trade",
    "arbitrum_aave_pancake_borrow_and_trade",
    "avalanche_aave_borrow_and_trade",
    "avalanche_aave_borrow_and_trade_v3",
    "base_aave_aerodrome_stable_borrow_and_trade",
    "base_aave_aerodrome_volatile_borrow_and_trade",
    "base_aave_curve_borrow_and_trade",
    "base_aave_pancake_borrow_and_trade",
    "base_aave_slipstream_borrow_and_trade",
    "bnb_aave_borrow_and_trade",
    "bnb_aave_borrow_and_trade_v3",
    "bnb_aave_borrow_and_trade_v4",
    "bnb_aave_pancake_borrow_and_trade",
    "ethereum_aave_pancake_borrow_and_trade",
    "optimism_aave_borrow_and_trade",
    "polygon_aave_borrow_and_trade",
    "polygon_aave_borrow_and_trade_v3",
    "polygon_aave_borrow_and_trade_v4",
)

AAVE_COLLATERAL = ns(chain="chain", reserve=ns(pool="pool"), collateral=True, borrow=False)
AAVE_LOAN = ns(chain="chain", reserve=ns(pool="pool"), capital=0, borrow=True)
AAVE_PAIR_REFUSALS = {
    "other_chain": (replace(AAVE_COLLATERAL, chain="other"), AAVE_LOAN),
    "other_pool": (AAVE_COLLATERAL, replace(AAVE_LOAN, reserve=ns(pool="other"))),
    "not_collateral": (replace(AAVE_COLLATERAL, collateral=False), AAVE_LOAN),
    "collateral_borrows": (replace(AAVE_COLLATERAL, borrow=True), AAVE_LOAN),
    "loan_has_capital": (AAVE_COLLATERAL, replace(AAVE_LOAN, capital=1)),
    "loan_cannot_borrow": (AAVE_COLLATERAL, replace(AAVE_LOAN, borrow=False)),
}


@pytest.mark.parametrize("name", AAVE_LENDING_PAIR)
def test_aave_lending_pair_accepts_collateral_and_debt_on_one_pool(name):
    load(name).validate_lending_pair(AAVE_COLLATERAL, AAVE_LOAN)


@pytest.mark.parametrize("case", sorted(AAVE_PAIR_REFUSALS))
@pytest.mark.parametrize("name", AAVE_LENDING_PAIR)
def test_aave_lending_pair_refuses_each_invalid_pair(name, case):
    with pytest.raises(ValueError):
        load(name).validate_lending_pair(*AAVE_PAIR_REFUSALS[case])


# Same-fund borrowing with a trade of part of the proceeds.

# Each strategy's lending reserve shape: Aave V4 uses a Spoke and names the collateral
# allocation from the debt; Compound V3 and Morpho Blue use one explicit market.
BORROW_AND_TRADE = {
    "avalanche_aave_v4_borrow_and_trade": "spoke",
    "ethereum_aave_v4_borrow_and_trade": "spoke",
    "base_compound_borrow_and_trade": "market",
    "base_morpho_borrow_and_trade": "market",
}


def trade_config(shape, counter_cash=0, counter="usdt"):
    reserve = {"spoke": {"spoke": "spoke"}, "market": {"market": "market"}}[shape]
    collateral = ns(
        name="collateral",
        account="fund",
        chain="chain",
        reserve=ns(asset="weth", **reserve),
        collateral=(),
        borrow=False,
        capital=10,
    )
    loan = ns(
        name="loan",
        account="fund",
        chain="chain",
        reserve=ns(asset="usdc", **reserve),
        collateral=("collateral",),
        borrow=True,
        capital=0,
    )
    trade = ns(
        name="trade",
        account="fund",
        chain="chain",
        range=None,
        capital=ns(asset="usdc", amount=1),
        pool=ns(currency0="usdc", currency1=counter),
    )
    return Config(
        positions=[collateral, loan, trade],
        inventory=[balance("fund", "weth", 10)]
        + ([] if counter == "weth" else [balance("fund", counter, counter_cash)]),
        asset_metadata={"usdc": ns(decimals=6), "weth": ns(decimals=18)},
    )


def trade_refusals(shape, pool_counter):
    config = trade_config(shape, counter=pool_counter)
    collateral, loan, trade = (config.allocation(n) for n in ("collateral", "loan", "trade"))
    other_reserve = replace(loan.reserve, **{shape: "other"})
    cases = {
        "allocations": Config(**{**vars(config), "positions": [collateral, loan]}),
        "two_funds": with_allocation(config, replace(trade, account="other")),
        "other_lending_reserve": with_allocation(config, replace(loan, reserve=other_reserve)),
        "trade_other_chain": with_allocation(config, replace(trade, chain="other")),
        "trade_opens_liquidity": with_allocation(config, replace(trade, range=(0, 1))),
        "trade_other_asset": with_allocation(
            config, replace(trade, capital=ns(asset="weth", amount=1))
        ),
        "trade_whole_borrow": with_allocation(
            config, replace(trade, capital=ns(asset="usdc", amount=10**9))
        ),
        "trade_pool_without_debt": with_allocation(
            config, replace(trade, pool=ns(currency0="dai", currency1=pool_counter))
        ),
        "collateral_cash_missing": Config(**{**vars(config), "inventory": []}),
    }
    if shape == "spoke":
        cases["other_lending_chain"] = with_allocation(config, replace(loan, chain="other"))
        cases["debt_names_no_collateral"] = with_allocation(config, replace(loan, collateral=()))
    else:
        cases["collateral_borrows"] = with_allocation(config, replace(collateral, borrow=True))
        cases["loan_has_capital"] = with_allocation(config, replace(loan, capital=1))
        cases["loan_cannot_borrow"] = with_allocation(config, replace(loan, borrow=False))
    return cases


def pool_counter(name):
    # The Avalanche Aave V4 reference may trade the debt against any counter-asset that
    # starts without fund cash; the others must trade the debt against the collateral.
    return "usdt" if name == "avalanche_aave_v4_borrow_and_trade" else "weth"


@pytest.mark.parametrize("name", sorted(BORROW_AND_TRADE))
def test_borrow_and_trade_accepts_reference_shape(name):
    config = trade_config(BORROW_AND_TRADE[name], counter=pool_counter(name))
    load(name).validate_config(authored(name, config))


@pytest.mark.parametrize(
    ("name", "case"),
    [
        (name, case)
        for name, shape in sorted(BORROW_AND_TRADE.items())
        for case in sorted(trade_refusals(shape, pool_counter(name)))
    ],
)
def test_borrow_and_trade_refuses_each_invalid_shape(name, case):
    config = trade_refusals(BORROW_AND_TRADE[name], pool_counter(name))[case]
    with pytest.raises(ValueError):
        load(name).validate_config(authored(name, config))


def test_avalanche_aave_v4_counter_asset_must_start_without_cash():
    name = "avalanche_aave_v4_borrow_and_trade"
    module = load(name)
    module.validate_config(authored(name, trade_config("spoke", counter="weth")))
    module.validate_config(authored(name, trade_config("spoke", counter_cash=0)))
    with pytest.raises(ValueError, match="counter-asset"):
        module.validate_config(authored(name, trade_config("spoke", counter_cash=1)))


BORROW_QUANTITY_REFERENCES = sorted(
    path.parent.name
    for path in STRATEGIES.glob("*/strategy.py")
    if "borrow_quantity: PositiveDecimal" in path.read_text()
)


class Holder:
    def __init__(self, parameters):
        self.parameters = parameters

    def model_copy(self, update):
        return Holder(update["parameters"])


def knobs(name, **values):
    """The reference's typed parameters for its authored values, with `values` replaced."""
    raw = yaml.safe_load((STRATEGIES / name / "config.yaml").read_text())
    parameters = StrategyParameters({**raw["strategy"]["parameters"], **values})
    return load(name).typed_config(Holder(parameters)).parameters


@pytest.mark.parametrize("name", BORROW_QUANTITY_REFERENCES)
@pytest.mark.parametrize("value", ["Infinity", "-Infinity", "NaN", "0", "-1", "five"])
def test_borrow_quantity_refuses_non_finite_and_non_positive_amounts(name, value):
    with pytest.raises(ParametersRefused, match="strategy.parameters.borrow_quantity"):
        knobs(name, borrow_quantity=value)


@pytest.mark.parametrize("name", BORROW_QUANTITY_REFERENCES)
def test_borrow_quantity_reads_the_authored_amount(name):
    raw = yaml.safe_load((STRATEGIES / name / "config.yaml").read_text())
    authored_value = raw["strategy"]["parameters"]["borrow_quantity"]
    quantity = knobs(name).borrow_quantity
    assert quantity == Decimal(str(authored_value)) and quantity > 0


def test_borrow_quantity_is_the_lending_references_typed_knob():
    assert len(BORROW_QUANTITY_REFERENCES) >= 68


def test_sma_borrow_refuses_an_infinite_borrow_quantity_and_unordered_levels():
    with pytest.raises(ParametersRefused, match="borrow_quantity"):
        knobs("aave_v3_base_sma_borrow", borrow_quantity="Infinity")
    with pytest.raises(ParametersRefused, match="repay_below"):
        knobs("aave_v3_base_sma_borrow", repay_below=1.01)


@pytest.mark.parametrize(
    "name,values,field",
    [
        ("uniswap_v3_base_rsi", {"oversold": 80}, "oversold"),
        ("uniswap_v3_base_rsi", {"window": 14}, "window"),
        ("aave_v3_base_sma_borrow", {"repay_below": 1.2}, "repay_below"),
        ("aave_v3_base_sma_borrow", {"borrow_above": 0.99}, "borrow_above"),
        ("aave_v3_base_sma_borrow", {"window": 47}, "window"),
        ("uniswap_v3_base_atr_breakout", {"window": 2, "channel": 2}, "window"),
        ("uniswap_v3_base_rsi_lp", {"trending": [45, 80]}, "trending"),
        ("uniswap_v3_base_rsi_lp", {"window": 14}, "window"),
    ],
)
def test_cross_knob_refusals_name_the_offending_field(name, values, field):
    with pytest.raises(ParametersRefused) as refused:
        knobs(name, **values)
    assert refused.value.field == f"strategy.parameters.{field}"


@pytest.mark.parametrize("name", BORROW_QUANTITY_REFERENCES)
def test_borrow_quantity_finer_than_the_asset_is_refused_not_truncated(name):
    module = load(name)
    loan = SimpleNamespace(name="loan", reserve=SimpleNamespace(asset="usdc"))

    def amount(quantity):
        config = Config(
            positions=[loan],
            parameters=knobs(name, borrow_quantity=quantity),
            asset_metadata={"usdc": SimpleNamespace(decimals=6)},
        )
        if name == "aave_v3_base_sma_borrow":
            return module.borrow_amount(config)
        return module.borrow_amount(config, "loan")

    assert amount("1.5") == 1_500_000
    with pytest.raises(ParametersRefused, match="exceeds the asset's 6 decimal places") as refused:
        amount("0.0000001")
    assert refused.value.field == "strategy.parameters.borrow_quantity"
