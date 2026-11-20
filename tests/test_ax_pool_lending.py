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
        assert binding(name).actions == ("supply", "withdraw")
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


def test_an_action_the_pool_risk_checks_is_refused_while_the_account_has_debt():
    debt = {WETH: pool.COLLATERAL, DAI: pool.DEBT}
    # The refusal names the reserve the wallet owes.
    with pytest.raises(ValueError, match=rf"has debt in this Pool \({DAI}\): a supply"):
        pool.refuse_unbounded_risk(SUPPLY, debt, USDC)
    with pytest.raises(ValueError, match="a withdraw there is risk-checked"):
        pool.refuse_unbounded_risk(WITHDRAW, debt, WETH)
    # Releasing a reserve that backs nothing is not risk-checked, and neither is
    # anything on an account without debt.
    pool.refuse_unbounded_risk(WITHDRAW, debt, USDC)
    pool.refuse_unbounded_risk(SUPPLY, {WETH: pool.COLLATERAL}, USDC)
    pool.refuse_unbounded_risk(WITHDRAW, {WETH: pool.COLLATERAL}, WETH)


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
    with pytest.raises(ValueError, match="has debt in this Pool"):
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
    with pytest.raises(ValueError, match="requires --amount"):
        pool.intent(config, (), ActionRequest("supply"))
    with pytest.raises(ValueError, match="takes no --amount"):
        pool.intent(config, (), ActionRequest("withdraw", 5))


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
