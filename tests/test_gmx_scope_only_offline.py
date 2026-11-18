"""GMX scope-only authority and modeled GM admission, built from literal targets.

Swap grants pin assets, market, keeper fee and Safe receivers but no amount;
modeled GM requests escrow exactly their inputs and report cumulative spending
without bounding it (blueprint 27).
"""

from types import SimpleNamespace

import pytest
from eth_abi import encode

from almanak.caip import AssetType
from almanak.connectors.gmx_v2 import economic_liquidity_requests as requests
from almanak.connectors.gmx_v2 import exchange_permissions, permissions
from almanak.connectors.gmx_v2.exchange_models import ExchangeCapability
from almanak.connectors.gmx_v2.liquidity_operations import CreateDeposit, CreateWithdrawal
from almanak.gateway.ethereum import DataError
from almanak.models import AssetAmount
from tests.test_gmx_exchange_domain import exchange_target

FEE = 7


def conditions(node):
    yield node
    for child in node.children or ():
        yield from conditions(child)


def word(value):
    return "0x" + encode(("uint256",), (value,)).hex()


def capability():
    return ExchangeCapability(target=exchange_target(), maximum_execution_fee=FEE)


def test_swap_branches_pin_the_keeper_fee_and_leave_amounts_unbounded():
    value = capability()
    market = value.target.market
    assert market.long_asset == market.wrapped_native != market.short_asset
    native = list(conditions(exchange_permissions.swap_branch(value, market.wrapped_native)))
    other = list(conditions(exchange_permissions.swap_branch(value, market.short_asset)))
    # A wrapped-native input escrows its keeper fee with it: strictly more than the fee.
    assert any(c.operator == 17 and c.value == word(FEE) for c in native)
    # Another input transfers the exact fee separately, and only a positive amount.
    assert not any(c.operator == 17 and c.value == word(FEE) for c in other)
    assert any(c.operator == 16 and c.value == word(FEE) for c in other)
    assert any(c.operator == 17 and c.value == word(0) for c in other)
    assert native != other


def test_swap_derivation_grants_router_approvals_and_one_multicall():
    value = capability()
    contracts = value.target.market.contracts
    grants = exchange_permissions.derive(value)
    by_signature: dict[str, list] = {}
    for grant in grants:
        by_signature.setdefault(grant.signature, []).append(grant)
    assert set(by_signature) == {"approve(address,uint256)", "multicall(bytes[])"}
    (creation,) = by_signature["multicall(bytes[])"]
    assert creation.target == contracts.exchange_router
    assert creation.condition.operator == 2 and len(creation.condition.children) == 2
    approved = {grant.target for grant in by_signature["approve(address,uint256)"]}
    assert approved == {
        value.target.market.long_asset.reference,
        value.target.market.short_asset.reference,
    }


def test_permission_derivation_accepts_exchange_cancellation_and_skips_other_funds():
    value = capability()
    profile = SimpleNamespace(fund=value.target.fund)
    foreign = SimpleNamespace(target=SimpleNamespace(fund="another fund"))
    grants = permissions.derive(profile, SimpleNamespace(requirements=(foreign, value)))
    signatures = {grant.signature for grant in grants}
    assert {"multicall(bytes[])", "approve(address,uint256)", "cancelOrder(bytes32)"} <= signatures
    assert not signatures & {"cancelDeposit(bytes32)", "cancelWithdrawal(bytes32)"}
    (cancel,) = (grant for grant in grants if grant.signature == "cancelOrder(bytes32)")
    assert cancel.explanation[0].startswith("Accepted dedicated-Safe scope")
    assert (
        permissions.derive(SimpleNamespace(fund="nobody"), SimpleNamespace(requirements=(value,)))
        == ()
    )


def test_permission_derivation_adds_order_claim_and_every_cancellation_for_positions(monkeypatch):
    fund = "fund"
    target = SimpleNamespace(
        fund=fund, market=SimpleNamespace(contracts=SimpleNamespace(exchange_router="router"))
    )
    position = SimpleNamespace(target=target)
    monkeypatch.setattr(
        permissions, "scope", lambda value: SimpleNamespace(exchange_router="router")
    )
    monkeypatch.setattr(permissions, "candidate_order_grants", lambda order: ["order"])
    monkeypatch.setattr(permissions, "claim_grant", lambda value: "claim")
    cancellations = [
        SimpleNamespace(signature=name, explanation=("first", "rest"))
        for name in ("cancelOrder(bytes32)", "cancelDeposit(bytes32)")
    ]
    monkeypatch.setattr(permissions, "candidate_cancellation_grants", lambda router: cancellations)
    monkeypatch.setattr(permissions, "accepted_grant", lambda grant: ("accepted", grant.signature))
    monkeypatch.setattr(permissions, "combine", list)
    granted = permissions.derive(
        SimpleNamespace(fund=fund), SimpleNamespace(requirements=(position,))
    )
    assert granted == [
        "order",
        "claim",
        ("accepted", "cancelOrder(bytes32)"),
        ("accepted", "cancelDeposit(bytes32)"),
    ]


# Modeled GM admission.

LONG = AssetType("eip155:42161/erc20:0x" + "01" * 20)
SHORT = AssetType("eip155:42161/erc20:0x" + "02" * 20)
NATIVE = AssetType("eip155:42161/erc20:0x" + "03" * 20)
SHARE = AssetType("eip155:42161/erc20:0x" + "04" * 20)
TARGET = SimpleNamespace(
    allocation="gm",
    market=SimpleNamespace(long_asset=LONG, short_asset=SHORT, wrapped_native=NATIVE),
)
ALLOCATION = SimpleNamespace(deadline_seconds=300)
CONFIG = SimpleNamespace(allocation=lambda name: ALLOCATION)


def value(asset, units):
    return AssetAmount(asset=asset, amount=units)


class Payload(SimpleNamespace):
    def model_copy(self, update):
        return Payload(**{**vars(self), **update})


def payload(escrow=None):
    return Payload(
        target=TARGET,
        escrow=escrow,
        owned="owned",
        spent=(value(LONG, 1), value(SHORT, 2), value(NATIVE, 3), value(SHARE, 4)),
        allowances=(value(LONG, 10), value(SHORT, 20), value(NATIVE, 9), value(SHARE, 0)),
    )


def deposit():
    return CreateDeposit.model_construct(
        target=TARGET,
        amounts=SimpleNamespace(root=(value(LONG, 10), value(SHORT, 20))),
        execution_fee=value(NATIVE, 5),
    )


def withdrawal():
    return CreateWithdrawal.model_construct(
        target=TARGET, market_tokens=value(SHARE, 8), execution_fee=value(NATIVE, 5)
    )


def risk(*, timestamp=100, market_disabled=False, deposits=False, withdrawals=False):
    return SimpleNamespace(
        block=SimpleNamespace(timestamp=timestamp),
        market="market",
        market_disabled=market_disabled,
        deposits=SimpleNamespace(creation_disabled=deposits),
        withdrawals=SimpleNamespace(creation_disabled=withdrawals),
    )


def pending(*, eligible_at=100, deadline=200, target=TARGET):
    intent = SimpleNamespace(target=target, deadline=deadline)
    return SimpleNamespace(intent=intent, eligible_at=eligible_at, identity="identity")


@pytest.fixture
def modeled(monkeypatch):
    seen = SimpleNamespace(request=None, inventory=[])
    monkeypatch.setattr(requests, "accounting_market", lambda market: "market")
    monkeypatch.setattr(requests, "request_for", lambda config, intent, data: seen.request)
    monkeypatch.setattr(requests, "wallet", lambda target, asset: ("wallet", asset))
    monkeypatch.setattr(requests, "AssetDelta", SimpleNamespace)
    monkeypatch.setattr(requests, "LiquidityEscrow", SimpleNamespace)

    def inventory_after(owned, movements):
        seen.inventory.append((owned, movements))
        return "moved"

    monkeypatch.setattr(requests, "inventory_after", inventory_after)
    return seen


def test_gm_deposit_escrows_exact_inputs_and_reports_cumulative_spending(modeled):
    modeled.request = deposit()
    request = pending()
    updated, movements = requests.admit(CONFIG, payload(), request, risk())
    assert [(m.key, m.amount) for m in movements] == [
        (("wallet", LONG), -10),
        (("wallet", SHORT), -20),
        (("wallet", NATIVE), -5),
    ]
    assert modeled.inventory == [("owned", movements)] and updated.owned == "moved"
    assert updated.spent == (value(LONG, 11), value(SHORT, 22), value(NATIVE, 8), value(SHARE, 4))
    assert updated.allowances == (
        value(LONG, 0),
        value(SHORT, 0),
        value(NATIVE, 0),
        value(SHARE, 0),
    )
    escrow = updated.escrow
    assert escrow.intent is request.intent and escrow.identity == "identity"
    assert escrow.accepted_at == 100 and escrow.request is modeled.request
    assert escrow.inputs == (value(LONG, 10), value(SHORT, 20))
    assert escrow.execution_fee == value(NATIVE, 5)


def test_gm_withdrawal_escrows_its_shares(modeled):
    modeled.request = withdrawal()
    updated, movements = requests.admit(CONFIG, payload(), pending(), risk(deposits=True))
    assert [(m.key, m.amount) for m in movements] == [
        (("wallet", SHARE), -8),
        (("wallet", NATIVE), -5),
    ]
    assert updated.escrow.inputs == (value(SHARE, 8),)
    assert updated.spent[3] == value(SHARE, 12)
    assert updated.allowances[0] == value(LONG, 10)


def test_gm_admission_requires_a_new_exact_target_request_at_its_eligible_time(modeled):
    modeled.request = deposit()
    other = SimpleNamespace(**{**vars(TARGET), "allocation": "other"})
    for args in (
        (payload(), pending(target=other), risk()),
        (payload(escrow="held"), pending(), risk()),
        (payload(), pending(eligible_at=99), risk()),
    ):
        with pytest.raises(ValueError, match="new exact-target request"):
            requests.admit(CONFIG, *args)
    moved = risk()
    moved.market = "another market"
    with pytest.raises(ValueError, match="new exact-target request"):
        requests.admit(CONFIG, payload(), pending(), moved)


def test_gm_admission_refuses_disabled_creation_and_deadlines(modeled):
    modeled.request = deposit()
    for disabled in (risk(market_disabled=True), risk(deposits=True)):
        with pytest.raises(ValueError, match="historically disabled"):
            requests.admit(CONFIG, payload(), pending(), disabled)
    modeled.request = withdrawal()
    with pytest.raises(ValueError, match="historically disabled"):
        requests.admit(CONFIG, payload(), pending(), risk(withdrawals=True))
    with pytest.raises(DataError) as refused:
        requests.admit(CONFIG, payload(), pending(deadline=401), risk())
    assert refused.value.kind == "stale_plan"
    with pytest.raises(ValueError, match="missed its explicit admission deadline"):
        requests.admit(CONFIG, payload(), pending(eligible_at=150, deadline=150), risk())


def test_gm_admission_refuses_inputs_outside_the_configured_assets(modeled):
    modeled.request = withdrawal()
    narrow = payload()
    narrow.spent = narrow.spent[:3]
    with pytest.raises(ValueError, match="outside its configured input assets"):
        requests.admit(CONFIG, narrow, pending(), risk())
