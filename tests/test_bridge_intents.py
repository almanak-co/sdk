"""Bridge requests keep both funds, both assets and the connector's own target type."""

import json

import pytest
from pydantic import TypeAdapter, ValidationError

from almanak.bridge_intents import BridgeIntent, BridgeTarget
from almanak.caip import AssetType
from almanak.connectors.support import EconomicAction
from almanak.models import Account
from tests.bridge_support import DESTINATION_FUND, ORIGIN, RECEIVED, SENT, SOURCE_FUND, amount


class RouteTarget(BridgeTarget):
    sent: AssetType
    received: AssetType

    @property
    def asset(self) -> AssetType:
        return self.sent

    @property
    def destination_asset(self) -> AssetType:
        return self.received


class OtherTarget(RouteTarget):
    pass


def target(cls=RouteTarget, **changes):
    values = {
        "allocation": "move",
        "account": "here",
        "fund": SOURCE_FUND,
        "destination_account": "there",
        "destination_fund": DESTINATION_FUND,
        "sent": SENT,
        "received": RECEIVED,
        **changes,
    }
    return cls(**values)


def test_a_bridge_intent_round_trips_with_its_concrete_target():
    intent = BridgeIntent[RouteTarget](target=target(), amount=amount(SENT, 7))
    adapter = TypeAdapter(BridgeIntent[RouteTarget])
    restored = adapter.validate_json(adapter.dump_json(intent))
    assert restored == intent and type(restored.target) is RouteTarget
    body = json.loads(adapter.dump_json(intent))
    assert body["kind"] == "bridge" == str(EconomicAction.BRIDGE)
    assert body["target"]["fund"]["chain"]["chain_id"] == "eip155:42161"
    assert body["target"]["destination_fund"]["chain"]["chain_id"] == "eip155:8453"


def test_the_target_fund_is_the_source_so_work_routes_on_the_chain_it_leaves():
    assert target().fund.chain == ORIGIN


@pytest.mark.parametrize(
    ("changes", "cause"),
    [
        ({"sent": RECEIVED}, "asset has a different fund chain"),
        ({"received": SENT}, "destination asset has a different fund chain"),
        (
            {"destination_fund": Account(chain=ORIGIN, address="0x" + "22" * 20), "received": SENT},
            "two different chains",
        ),
        ({"destination_account": "here"}, "two different named accounts"),
    ],
)
def test_a_target_refuses_an_inconsistent_route(changes, cause):
    with pytest.raises(ValidationError, match=cause):
        target(**changes)


@pytest.mark.parametrize(
    ("value", "cause"),
    [
        (amount(SENT, 0), "positive uint256"),
        (amount(SENT, 2**256), "positive uint256"),
        (amount(RECEIVED, 7), "sent asset"),
    ],
)
def test_the_amount_is_a_positive_quantity_of_the_sent_asset(value, cause):
    with pytest.raises(ValidationError, match=cause):
        BridgeIntent[RouteTarget](target=target(), amount=value)


def test_an_intent_refuses_any_target_type_but_its_own():
    with pytest.raises(ValidationError, match="exact concrete target type"):
        BridgeIntent[RouteTarget](target=target(OtherTarget), amount=amount(SENT, 7))


def test_a_bridge_target_stays_within_one_environment():
    from tests.bridge_support import DESTINATION, DESTINATION_FUND

    production = DESTINATION.model_copy(update={"environment": "production"})
    with pytest.raises(ValidationError, match="one environment"):
        target(destination_fund=DESTINATION_FUND.model_copy(update={"chain": production}))
