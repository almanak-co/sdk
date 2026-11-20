"""Direct operator actions on Aave V3 and SparkLend Pools: the deployment they build.

Component evidence against retained Safe lifecycle captures and a stub reader. The whole
command on deployed contracts is tests/fork/test_ax_pool_lending.py.
"""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors import pool_lending_direct as pool
from almanak.connectors.composition import connector_from_wire
from almanak.connectors.direct import ALLOCATION, ActionRequest, Wallet, binding
from almanak.connectors.pool_lending_lifecycle import adopt, commit_owned
from almanak.gateway.ethereum import DataError
from almanak.operator_deployment import supports_operator
from tests.aave_v3_helpers import recorded_v3  # noqa: F401
from tests.lending_captures import read_capture
from tests.sparklend_helpers import candidate_spark_support, recorded_spark  # noqa: F401

WALLET = Wallet(account="treasury", address="0x" + "ab" * 20)
WETH, USDC, DAI = "0x" + "11" * 20, "0x" + "22" * 20, "0x" + "33" * 20
POOL = "0x" + "99" * 20
SUPPLY, WITHDRAW = ActionRequest("supply", 5), ActionRequest("withdraw")


class Reader:
    """A Pool with three reserves, one account's user bitmap, and reserve configurations.

    Reserve IDs are 0, 1 and 3: ID 2 was dropped, so the Pool's reserve list, which
    closes that gap, is no index of IDs.
    """

    assets = {0: WETH, 1: USDC, 3: DAI}

    def __init__(self, bitmap, isolated=()):
        self.bitmap, self.isolated = bitmap, isolated

    def call(self, target, signature, block, **options):
        assert target == POOL
        if signature == "getUserConfiguration(address)":
            assert options["args"] == (WALLET.address,)
            return (((self.bitmap,)),)
        if signature == "getConfiguration(address)":
            (asset,) = options["args"]
            # An LTV in the low bits, and a debt ceiling at bit 212 for an isolated reserve.
            return (((8000 | (5 << 212 if asset in self.isolated else 0),)),)
        assert signature == "getReserveAddressById(uint16)"
        (reserve_id,) = options["args"]
        return (self.assets[reserve_id].upper().replace("0X", "0x"),)


def bits(weth=0, usdc=0, dai=0):
    """A user bitmap from the flags of each reserve, at its ID: debt is 1, collateral is 2."""
    return weth | usdc << 2 | dai << 6


@pytest.fixture(params=["aave_v3", "sparklend"])
def recorded(request):
    name = "recorded_v3" if request.param == "aave_v3" else "recorded_spark"
    return request.param, request.getfixturevalue(name)


def test_the_pool_connectors_serve_operator_deployments():
    for name in ("aave_v3", "sparklend"):
        connector = connector_from_wire({"connector": name})
        assert supports_operator(connector.codec.checkpoint)
        assert binding(name).actions == ("supply", "withdraw", "borrow", "repay", "collateral")
        # Every action a strategy may take on a Pool cell is also a direct action.
        assert all(cell.direct_actions == cell.actions for cell in connector.chain_support)
        assert connector.lifecycle.adopt is adopt


def test_a_strategy_checkpoint_keeps_its_wire_form(recorded):
    name, state = recorded
    fixture = "aave-v3" if name == "aave_v3" else "sparklend"
    path = Path(__file__).parent / f"fixtures/{fixture}-safe-lifecycle.json.gz"
    retained = json.loads(read_capture(path))
    wire = state.model_dump(mode="json")
    assert "operator" not in wire and state.operator is False
    assert {key: wire[key] for key in retained} == retained


def test_a_deployment_is_an_operator_deployment_or_retains_its_strategy(recorded):
    _, state = recorded
    wire = state.model_dump(mode="json")
    with pytest.raises(ValueError, match="a strategy deployment retains its strategy"):
        type(state).model_validate_json(json.dumps({**wire, "strategy": None}))
    with pytest.raises(ValueError, match="an operator deployment retains no strategy"):
        type(state).model_validate_json(json.dumps({**wire, "operator": True}))
    operator = {**wire, "operator": True, "strategy": None, "permissions": None}
    assert type(state).model_validate_json(json.dumps(operator)).operator is True


def test_an_operator_deployment_starts_without_a_strategy(recorded):
    name, state = recorded
    connector = connector_from_wire({"connector": name})
    initial = connector.lifecycle.initial(state.config, None, state.capabilities, None)
    assert initial.operator and initial.strategy is None
    assert initial.model_dump(mode="json")["operator"] is True
    assert connector.projection.report(initial)["strategy"] is None
    started = connector.lifecycle.initial(state.config, state.strategy, state.capabilities, None)
    assert not started.operator and started.strategy == state.strategy


def test_adoption_takes_ownership_from_the_observed_position(recorded):
    _, state = recorded
    supply = next(item for item in state.outcomes if item.operation.kind == "supply")
    empty = tuple(
        type(item)(target=item.target, supply_scaled=0, debt_scaled=0, collateral_enabled=False)
        for item in state.owned
    )
    index = next(i for i, item in enumerate(empty) if item.target == supply.operation.target)
    adopted = adopt(empty[index : index + 1], (supply.after,))
    assert adopted == commit_owned(empty[index : index + 1], supply)
    assert adopted[0].supply_scaled == supply.after.position.supplied_scaled > 0
    with pytest.raises(DataError, match="do not cover the configured Pool positions"):
        adopt(empty, (supply.after,) * (len(empty) + 1))
    # An observation of another position cannot be adopted as this one.
    other = next(item for item in empty if item.target != supply.operation.target)
    with pytest.raises(DataError, match="do not cover the configured Pool positions"):
        adopt((other,), (supply.after,))


def test_the_user_bitmap_names_the_reserves_the_account_holds():
    # WETH as collateral, USDC untouched, DAI as debt.
    reader = Reader(bits(weth=pool.COLLATERAL, usdc=0, dai=pool.DEBT))
    assert pool.held_reserves(reader, None, POOL, WALLET.address) == {
        WETH: pool.COLLATERAL,
        DAI: pool.DEBT,
    }
    assert pool.held_reserves(Reader(0), None, POOL, WALLET.address) == {}
    # DAI sits at ID 3, past a dropped reserve: the bitmap is read by ID, not by position
    # in the Pool's reserve list.
    assert pool.held_reserves(Reader(pool.DEBT << 6), None, POOL, WALLET.address) == {
        DAI: pool.DEBT
    }


def test_a_risk_checked_action_needs_a_stated_floor_while_the_account_has_debt():
    debt = {WETH: pool.COLLATERAL, DAI: pool.DEBT}
    floored = {"min_health_factor": 15 * 10**17}
    refused = (
        (SUPPLY, USDC),
        (WITHDRAW, WETH),
        (ActionRequest("withdraw", 3), WETH),
        (ActionRequest("collateral", enable=False), WETH),
        (ActionRequest("collateral", enable=True), USDC),
    )
    for request, asset in refused:
        # The refusal names the reserve the wallet owes.
        expected = rf"has debt in this Pool \({DAI}\): a {request.action} there is risk-checked"
        with pytest.raises(ValueError, match=expected):
            pool.require_floor(request, debt, asset)
        pool.require_floor(ActionRequest(request.action, request.amount, **floored), debt, asset)
        # Nothing on an account without debt needs a floor.
        pool.require_floor(request, {WETH: pool.COLLATERAL}, asset)
    # Releasing a reserve that backs nothing is not risk-checked, and a repayment never is.
    pool.require_floor(WITHDRAW, debt, USDC)
    pool.require_floor(ActionRequest("repay"), debt, DAI)


def test_the_binding_binds_the_pool_exactly_where_the_connector_checks_risk():
    """`risk_checked` restates the connector's `requires_risk` for a request: a supply, a
    borrow and enabling collateral always; with debt, disabling collateral and
    withdrawing a reserve used as collateral."""
    free, debt = {WETH: pool.COLLATERAL}, {WETH: pool.COLLATERAL, DAI: pool.DEBT}
    enable = ActionRequest("collateral", enable=True)
    disable = ActionRequest("collateral", enable=False)
    part = ActionRequest("withdraw", 3)
    checked = {
        (SUPPLY, USDC): (True, True),
        (ActionRequest("borrow", 5), DAI): (True, True),
        (enable, WETH): (True, True),
        (disable, WETH): (False, True),
        (WITHDRAW, WETH): (False, True),
        (part, WETH): (False, True),
        # A reserve that backs nothing, and a repayment, are never checked.
        (WITHDRAW, USDC): (False, False),
        (ActionRequest("repay"), DAI): (False, False),
        (ActionRequest("repay", 3), DAI): (False, False),
    }
    for (request, asset), (without_debt, with_debt) in checked.items():
        assert pool.risk_checked(request, free, asset) is without_debt, request
        assert pool.risk_checked(request, debt, asset) is with_debt, request


def test_one_health_floor_serves_the_whole_pool(market):
    reader = Reader(bits(weth=pool.COLLATERAL, usdc=0, dai=pool.DEBT))
    floor = 15 * 10**17
    borrow = ActionRequest("borrow", 5, min_health_factor=floor)
    document = pool.positions("aave_v3", reader, None, market(DAI), WALLET, borrow)
    requested, held = document["allocations"]
    assert (requested["name"], requested["borrow"]) == (ALLOCATION, True)
    assert "borrow" not in held
    assert [a["minimum_health_factor"] for a in document["allocations"]] == [floor, floor]
    supply = ActionRequest("supply", 5, min_health_factor=floor)
    floored = pool.positions("aave_v3", reader, None, market(USDC), WALLET, supply)
    assert all("borrow" not in a for a in floored["allocations"])
    assert {a["minimum_health_factor"] for a in floored["allocations"]} == {floor}


def test_a_repayment_states_the_lowest_floor_so_none_can_refuse_it(market):
    reader = Reader(bits(weth=pool.COLLATERAL, usdc=0, dai=pool.DEBT))
    document = pool.positions("aave_v3", reader, None, market(DAI), WALLET, ActionRequest("repay"))
    # A repayment is not risk-checked, so it binds its own reserve only.
    (requested,) = document["allocations"]
    assert (requested["minimum_health_factor"], requested["borrow"]) == (10**18 + 1, True)
    stated = ActionRequest("repay", 3, min_health_factor=2 * 10**18)
    document = pool.positions("aave_v3", reader, None, market(DAI), WALLET, stated)
    assert {a["minimum_health_factor"] for a in document["allocations"]} == {2 * 10**18}


@pytest.fixture
def market(monkeypatch):
    """Resolution and price sources of the stub Pool, keyed by the reserve asset."""
    from almanak.caip import ChainId
    from almanak.connectors import lending_lookup

    chain = ChainId("eip155:1")

    def resolved(asset, pool_address=None):
        chosen = SimpleNamespace(chain=chain, pool=pool_address)
        reserve = SimpleNamespace(
            pool=POOL,
            asset=f"eip155:1/erc20:{asset}",
            model_dump=lambda mode: {"pool": POOL, "asset": f"eip155:1/erc20:{asset}"},
        )
        return SimpleNamespace(selection=chosen, reserve=reserve)

    def resolve(reader, selected, selection, block):
        return resolved(selection["asset"].rsplit(":", 1)[1], selection.get("pool"))

    def sources(reader, selected, found, block):
        return {"oracle": "oracle", "price_source": "feed:" + found.reserve.asset[-4:]}, {}

    monkeypatch.setattr(lending_lookup, "family", lambda connector: connector)
    monkeypatch.setattr(lending_lookup, "selection", lambda connector, values: values)
    monkeypatch.setattr(lending_lookup, "resolve", resolve)
    monkeypatch.setattr(lending_lookup, "pool_sources", sources)
    monkeypatch.setattr("almanak.gateway.evm.erc20_address", lambda asset, chain: asset[-42:])
    return resolved


def names(document):
    return [
        (a["name"], a["reserve"]["asset"][-42:], a["collateral"]) for a in document["allocations"]
    ]


def test_a_wallet_with_nothing_in_the_pool_binds_only_the_requested_reserve(market):
    document = pool.positions("aave_v3", Reader(0), None, market(USDC), WALLET, SUPPLY)
    assert document["connector"] == "aave_v3"
    (allocation,) = document["allocations"]
    assert allocation == {
        "name": ALLOCATION,
        "account": "treasury",
        "reserve": {"pool": POOL, "asset": f"eip155:1/erc20:{USDC}"},
        # Authority for the Pool's own enablement of a first supply.
        "collateral": True,
        "oracle": "oracle",
        "price_source": "feed:" + USDC[-4:],
        "native_asset": "eip155:1/slip44:60",
        "minimum_native_balance": 0,
    }


def test_a_wallet_that_holds_the_requested_reserve_binds_it_once(market):
    reader = Reader(bits(weth=pool.COLLATERAL))
    document = pool.positions("sparklend", reader, None, market(WETH), WALLET, SUPPLY)
    assert names(document) == [(ALLOCATION, WETH, True)]


def test_a_supply_binds_every_other_reserve_the_account_holds(market):
    reader = Reader(bits(weth=pool.COLLATERAL, usdc=0, dai=pool.COLLATERAL))
    document = pool.positions("aave_v3", reader, None, market(USDC, POOL), WALLET, SUPPLY)
    assert names(document) == [
        (ALLOCATION, USDC, True),
        ("held_0", WETH, True),
        ("held_1", DAI, True),
    ]
    # A held reserve states the price source the Pool uses for it now.
    assert [a["price_source"] for a in document["allocations"]] == [
        "feed:" + asset[-4:] for asset in (USDC, WETH, DAI)
    ]


def test_a_withdrawal_binds_its_own_reserve_only(market, monkeypatch):
    """No held reserve is resolved, so one the connector cannot verify blocks no exit."""
    from almanak.connectors import lending_lookup

    monkeypatch.setattr(
        lending_lookup, "resolve", lambda *a: pytest.fail("resolved a reserve it does not bind")
    )
    reader = Reader(bits(weth=pool.COLLATERAL, usdc=0, dai=pool.DEBT))
    document = pool.positions("aave_v3", reader, None, market(USDC), WALLET, WITHDRAW)
    assert names(document) == [(ALLOCATION, USDC, True)]
    with pytest.raises(ValueError, match="state --min-health-factor"):
        pool.positions("aave_v3", reader, None, market(USDC), WALLET, SUPPLY)


def test_a_supply_involving_isolated_collateral_is_refused(market):
    """The Pool would not enable it as the connector accounts a first supply."""
    held = bits(weth=pool.COLLATERAL)
    with pytest.raises(ValueError, match="this reserve is isolated collateral in its Pool"):
        pool.positions("aave_v3", Reader(0, isolated=(USDC,)), None, market(USDC), WALLET, SUPPLY)
    with pytest.raises(ValueError, match=f"holds isolated collateral \\({WETH}\\)"):
        pool.positions(
            "aave_v3", Reader(held, isolated=(WETH,)), None, market(USDC), WALLET, SUPPLY
        )
    # Leaving an isolated reserve is served, and so is a supply beside ordinary collateral.
    document = pool.positions(
        "aave_v3", Reader(held, isolated=(WETH,)), None, market(WETH), WALLET, WITHDRAW
    )
    assert names(document) == [(ALLOCATION, WETH, True)]
    beside = pool.positions("aave_v3", Reader(held), None, market(USDC), WALLET, SUPPLY)
    assert names(beside) == [(ALLOCATION, USDC, True), ("held_0", WETH, True)]


def test_an_action_states_its_amount_exactly_once(recorded):
    _, state = recorded
    config = state.config.model_copy(
        update={
            "allocations": (
                state.config.allocations[0].model_copy(update={"name": ALLOCATION}),
                *state.config.allocations[1:],
            )
        }
    )
    assert pool.intent(config, (), SUPPLY).amount.amount == 5
    assert pool.intent(config, (), WITHDRAW).kind == "withdraw"
    assert pool.intent(config, (), ActionRequest("withdraw", 3)).kind == "withdraw_amount"
    assert pool.intent(config, (), ActionRequest("borrow", 3)).kind == "borrow"
    assert pool.intent(config, (), ActionRequest("repay")).kind == "repay"
    assert pool.intent(config, (), ActionRequest("repay", 3)).amount.amount == 3
    released = pool.intent(config, (), ActionRequest("collateral", enable=False))
    assert (released.kind, released.enabled) == ("collateral", False)
    for action in ("supply", "borrow"):
        with pytest.raises(ValueError, match=f"{action} requires --amount"):
            pool.intent(config, (), ActionRequest(action))
    # Every request names the kind its own intent carries.
    for request in (SUPPLY, WITHDRAW, ActionRequest("withdraw", 3), ActionRequest("repay", 3)):
        assert pool.intent(config, (), request).kind == request.kind


def test_the_result_reports_the_requested_position():
    def owned(name, supply):
        return SimpleNamespace(
            target=SimpleNamespace(allocation=name),
            supply_scaled=supply,
            debt_scaled=0,
            collateral_enabled=bool(supply),
        )

    state = SimpleNamespace(owned=(owned("held_0", 9), owned(ALLOCATION, 4)))
    assert pool.position(state) == {
        "supplied_scaled": "4",
        "debt_scaled": "0",
        "collateral_enabled": True,
    }
