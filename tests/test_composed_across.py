"""Across as a composed component: bridged cash becomes inventory another allocation spends.

The flows are component flows, not processes. Each step is the real coordinator or
gateway code for that step: the deposit is reconciled by the gateway path, and every
settlement page is proposed in its chain's projection, committed, re-derived
independently and merged. The chains are fakes with evenly spaced blocks.
"""

from itertools import product

import pytest
from pydantic import ValidationError

from almanak import external_effects
from almanak.connectors.across.observations import Ownership as AcrossOwnership
from almanak.connectors.composed.authority import declared
from almanak.connectors.composed.execution import ComposedExecution, validate_inventory
from almanak.connectors.composed.inventory import ownership, validate
from almanak.connectors.composed.lifecycle import (
    ComposedLifecycle,
    component_state,
    validate_closure,
)
from almanak.connectors.composed.models import Config
from almanak.connectors.composed.requests import ComposedExternalEffects
from almanak.connectors.composed.state import Checkpoint
from almanak.connectors.compound_v3.domain import Market, Reserve
from almanak.connectors.compound_v3.models import Allocation as CompoundAllocation
from almanak.connectors.compound_v3.operations import SupplyOperation
from almanak.connectors.identifiers import ConnectorId
from almanak.effects import Balance, WalletAsset
from almanak.gateway import external_effects as gateway_effects
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import native_asset
from almanak.models import AssetAmount
from almanak.strategy import StrategyDefinition
from almanak.work import merge, project
from tests.across_support import AMOUNT, BASE, BASE_USDC, BASE_WETH, USDC
from tests.test_across_records import FORK_ARB, FORK_BASE, mined_deposit, observed
from tests.test_across_records import config as config  # noqa: F401 - a fixture
from tests.test_across_settlement import (
    FILL_DEADLINE,
    OFFERED,
    filled,
    flight,
    here,
    refunded,
    there,
)

ACROSS, COMPOUND = ConnectorId("across"), ConnectorId("compound_v3")
COMET, FEED = "0x" + "c0" * 20, "0x" + "fe" * 20
SOURCE = WalletAsset(account="arb", chain=FORK_ARB, asset=USDC)
DESTINATION = WalletAsset(account="base", chain=FORK_BASE, asset=BASE_USDC)


def earn(asset=BASE_USDC):
    market = Market(chain=FORK_BASE, comet=COMET, base_asset=BASE_USDC, base_price_feed=FEED)
    return CompoundAllocation(
        name="earn",
        account="base",
        reserve=Reserve(market=market, asset=asset, price_feed=FEED),
        native_asset=native_asset(BASE.chain_id),
        minimum_native_balance=0,
    )


def balances(source=AMOUNT, destination=0):
    return (Balance(key=SOURCE, amount=source), Balance(key=DESTINATION, amount=destination))


def composed(across, **changes):
    values = {
        "decision_trigger": across.decision_trigger,
        "accounts": across.accounts,
        "allocations": (*across.allocations, earn()),
        "inventory": balances(),
        "fork_bindings": across.fork_bindings,
        **changes,
    }
    return Config(**values)


def checkpoint(parameters, **changes):
    initial = ComposedLifecycle.initial(
        parameters,
        StrategyDefinition(filename="strategy.py", source=""),
        declared(parameters),
        None,
    )
    works = tuple(work.model_copy(update={"initialized": True}) for work in initial.work)
    return initial.model_copy(update={"work": works, **changes})


def inventory(state) -> dict:
    return {balance.key: balance.amount for balance in state.inventory}


def backend(parameters, reader):
    from types import SimpleNamespace

    fake = SimpleNamespace(config=parameters, reader=reader)
    fake.audit_history = lambda state: ComposedExternalEffects.audit(fake, state)
    return fake


def settle(state, reader):
    """One external-effects tick of one chain through the composed binding."""
    fake, local = backend(state.config, reader), project(state, reader.chain)
    page = ComposedExternalEffects.reconcile(fake, local)
    if page is None:
        return state
    committed = external_effects.commit(local, local, page)
    assert gateway_effects.verified_commit(fake, local, committed) == committed
    merged = merge(state, reader.chain, committed)
    return merged.model_copy(update={"delivered": len(merged.outcomes)})


def deposited(monkeypatch, across, **changes):
    """The composed checkpoint after the gateway reconciled one mined deposit."""
    outcome = mined_deposit(monkeypatch, across)
    state = checkpoint(composed(across), **changes)
    local = project(state, FORK_ARB)
    local = local.model_copy(update={**ownership(local, outcome), "outcomes": (outcome,)})
    state = merge(state, FORK_ARB, local)
    return state.model_copy(update={"delivered": 1}), outcome.transfer


def supply(state, amount):
    target = state.config.target("earn")
    return SupplyOperation(target=target, amount=AssetAmount(asset=BASE_USDC, amount=amount))


def expire(state):
    chain = there(FILL_DEADLINE + 10)
    for _ in range(3 * len(state.transfers)):
        state = settle(state, chain)
    return state


def test_a_bridge_names_both_funds_and_its_arrival_asset_must_be_held(config):  # noqa: F811
    parameters = composed(config)
    assert {b.key for b in parameters.inventory} == {SOURCE, DESTINATION}
    assert set(parameters.parameter_components) == {ACROSS, COMPOUND}
    across = parameters.component(ACROSS)
    assert {b.name for b in across.accounts.bindings} == {"arb", "base"}
    assert {b.name for b in parameters.component(COMPOUND).accounts.bindings} == {"base"}
    with pytest.raises(ValidationError, match="cover every configured fund asset"):
        composed(config, inventory=balances()[:1])
    # The receiving account holds another asset only: nothing would measure the arrival.
    elsewhere = WalletAsset(account="base", chain=FORK_BASE, asset=BASE_WETH)
    with pytest.raises(ValidationError, match="arrive in a wallet asset"):
        composed(
            config,
            allocations=(*config.allocations, earn(BASE_WETH)),
            inventory=(balances()[0], Balance(key=elsewhere, amount=0)),
        )
    # A bridge alone has nothing on the receiving account that holds what arrives.
    with pytest.raises(ValidationError, match="arrive in a wallet asset"):
        composed(config, allocations=config.allocations, inventory=balances()[:1])


def test_a_new_checkpoint_owns_the_bridge_and_omits_transfers_until_one_opens(config):  # noqa: F811
    state = checkpoint(composed(config))
    assert [type(o).__name__ for o in state.owned] == ["Ownership", "Ownership"]
    assert isinstance(state.owned[0], AcrossOwnership) and state.transfers == ()
    wire = state.model_dump(mode="json")
    assert "transfers" not in wire
    assert Checkpoint.model_validate_json(state.model_dump_json()) == state


def test_a_retained_bridge_record_carries_no_projected_cash_or_transfers(config):  # noqa: F811
    state = checkpoint(composed(config))
    projected = ComposedLifecycle.decision_owned(state)
    with pytest.raises(ValueError, match="retained bridge ownership"):
        validate(state.model_copy(update={"owned": (projected[0], state.owned[1])}))


def test_a_deposit_leaves_source_cash_and_is_in_no_wallet_until_it_arrives(
    monkeypatch,
    config,  # noqa: F811
):
    state, transfer = deposited(monkeypatch, config)
    assert inventory(state) == {SOURCE: 0, DESTINATION: 0}
    assert state.transfers == (transfer,) and transfer.open
    assert Checkpoint.model_validate_json(state.model_dump_json()) == state
    # The source tick sees the transfer in flight: sent, offered, status and deadline.
    (bridge,) = ComposedLifecycle.decision_owned(project(state, FORK_ARB))
    (flying,) = bridge.in_flight
    assert bridge.inventory == AssetAmount(asset=USDC, amount=0)
    assert (flying.sent.amount, flying.offered.amount) == (AMOUNT, OFFERED)
    assert (flying.resolution, flying.unfilled, flying.fill_deadline) == (
        None,
        False,
        transfer.fill_deadline,
    )
    # The destination tick owns no bridge record, and its cash excludes the offer.
    (lending,) = ComposedLifecycle.decision_owned(project(state, FORK_BASE))
    assert lending.inventory == AssetAmount(asset=BASE_USDC, amount=0)
    with pytest.raises(DataError) as error:
        validate_inventory(project(state, FORK_BASE), supply(state, OFFERED))
    assert error.value.kind == "inventory_unavailable"


def test_the_arrival_credits_the_destination_and_another_allocation_spends_it(
    monkeypatch,
    config,  # noqa: F811
):
    state, transfer = deposited(monkeypatch, config)
    chain = there()
    state = settle(state, chain)
    filled(config, chain, chain.head + 1)
    state = settle(state, chain)
    arrival = state.outcomes[-1]
    assert arrival.kind == "across_arrival" and arrival.received.amount == OFFERED
    assert inventory(state) == {SOURCE: 0, DESTINATION: OFFERED}
    assert state.transfers[0].resolution.received.amount == OFFERED
    local = project(state, FORK_BASE)
    assert local.outcomes == (arrival,) and local.outcomes[0].strategy_result().kind == (
        "bridge_arrival"
    )
    validate_inventory(local, supply(state, OFFERED))
    with pytest.raises(DataError, match="exceeds retained fund inventory"):
        validate_inventory(local, supply(state, OFFERED + 1))
    # Resolved transfers are history, not flight: the decision no longer lists them.
    (bridge,) = ComposedLifecycle.decision_owned(project(state, FORK_ARB))
    assert bridge.in_flight == ()
    assert Checkpoint.model_validate_json(state.model_dump_json()) == state
    ComposedExternalEffects.audit(backend(state.config, chain), local)


def test_a_refund_makes_source_cash_whole_and_credits_nothing_on_the_destination(
    monkeypatch,
    config,  # noqa: F811
):
    state, transfer = deposited(monkeypatch, config)
    chain = here(FILL_DEADLINE + 50)
    assert settle(state, chain) == state
    state = expire(state)
    assert state.transfers[0].unfilled and state.transfers[0].open and len(state.outcomes) == 1
    assert inventory(state) == {SOURCE: 0, DESTINATION: 0}
    state = settle(state, chain)
    refunded(config, chain, chain.head + 3, [AMOUNT])
    state = settle(state, chain)
    refund = state.outcomes[-1]
    assert refund.kind == "across_refund" and refund.returned.amount == AMOUNT
    assert inventory(state) == {SOURCE: AMOUNT, DESTINATION: 0}
    assert project(state, FORK_ARB).outcomes[-1].strategy_result().kind == "bridge_refund"
    with pytest.raises(DataError, match="exceeds retained fund inventory"):
        validate_inventory(project(state, FORK_BASE), supply(state, 1))


def test_each_component_sees_only_its_own_pages_outcomes_and_transfers(
    monkeypatch,
    config,  # noqa: F811
):
    state, transfer = deposited(monkeypatch, config)
    chain = there()
    state = settle(state, chain)
    filled(config, chain, chain.head + 1)
    state = settle(state, chain)
    across, lending = component_state(state, ACROSS), component_state(state, COMPOUND)
    assert across.transfers == state.transfers and lending.transfers == ()
    assert across.external_scans == state.external_scans and lending.external_scans == ()
    assert len(across.outcomes) == 2 and lending.outcomes == ()
    assert (across.requests, lending.requests) == ((), ())
    assert {p.kind for p in state.external_scans} == {"across_scan"}
    assert (
        ComposedExternalEffects.page_type.model_validate_json(
            state.external_scans[-1].model_dump_json()
        )
        == (state.external_scans[-1])
    )


def test_a_page_of_an_unconfigured_connector_is_refused(config):  # noqa: F811
    from types import SimpleNamespace

    state = checkpoint(composed(config))
    foreign = SimpleNamespace(connector=ConnectorId("gmx_v2"))
    with pytest.raises(ValueError, match="requires its configured connector"):
        ComposedExternalEffects.reconcile(
            backend(state.config, there()), project(state, FORK_BASE), reference=foreign
        )


def test_an_open_transfer_holds_closure_on_both_chains(monkeypatch, config):  # noqa: F811
    state, _ = deposited(monkeypatch, config, mode="teardown")
    with pytest.raises(DataError) as error:
        ComposedLifecycle.validate_closure(project(state, FORK_ARB), (observed(config),))
    assert error.value.kind == "transfer_in_flight"
    # The destination chain holds no bridge record, yet the transfer is owed there too.
    local = project(state, FORK_BASE)
    lending_only = local.model_copy(update={"owned": (), "inventory": ()})
    with pytest.raises(DataError) as error:
        validate_closure(lending_only, ())
    assert error.value.kind == "transfer_in_flight"


def test_planning_asks_the_component_quote_through_this_gateways_transport(
    monkeypatch,
    config,  # noqa: F811
):
    from types import SimpleNamespace

    from almanak.connectors.across import execution

    seen = {}

    def quoted(backend, state, intent, before):
        seen.update(config=backend.config, transfers=state.transfers, intent=intent)
        return "planned"

    monkeypatch.setattr(execution.AcrossExecution, "quoted_operation", staticmethod(quoted))
    state = checkpoint(composed(config))
    intent = state.config.component(ACROSS).bridge("move", AMOUNT)
    fake = SimpleNamespace(config=state.config, venue_transports={})
    assert ComposedExecution.quoted_operation(fake, state, intent, observed(config)) == "planned"
    assert seen["config"] == state.config.component(ACROSS) and seen["intent"] == intent
    assert set(ComposedExecution.venue_transports()) == set(execution.venue_transports())


FATES = ("arrive", "refund", "wait")


@pytest.mark.parametrize("fates", list(product(FATES, repeat=3)), ids="-".join)
def test_inventory_is_conserved_over_every_sequence_of_settlements(config, fates):  # noqa: F811
    """Three equal transfers each arrive, are refunded or stay in flight, beside a
    refund-leaf payment that matches nothing. After every page, what the funds own in
    wallets plus what is in flight plus the fees arrivals paid equals what they began with.
    """
    transfers = tuple(flight(config, serial) for serial in range(3))
    state = checkpoint(composed(config), transfers=transfers, inventory=balances(source=0))
    total = 3 * AMOUNT

    def conserved(current):
        in_flight = sum(t.sent.amount for t in current.transfers if t.open)
        fees = sum(
            t.sent.amount - t.resolution.received.amount
            for t in current.transfers
            if getattr(t.resolution, "received", None) is not None
        )
        assert sum(inventory(current).values()) + in_flight + fees == total
        assert Checkpoint.model_validate_json(current.model_dump_json()) == current

    destination = there()
    for serial, fate in enumerate(fates):
        if fate == "arrive":
            filled(config, destination, destination.head + 1, serial, tx=f"0x{serial + 1:064x}")
        for _ in range(4):
            state = settle(state, destination)
            conserved(state)
    arrived = fates.count("arrive")
    assert inventory(state) == {SOURCE: 0, DESTINATION: arrived * OFFERED}
    if "refund" in fates:
        destination.head = destination.at(FILL_DEADLINE + 10)
        # The fake answers one status for every relay; the fills above are all read.
        destination.views["fillStatuses(bytes32)"] = 0
        for _ in range(12):
            state = settle(state, destination)
            conserved(state)
    source = here(FILL_DEADLINE + 50)
    state = settle(state, source)
    # A stranger's dust refund naming the fund is retained and credits nothing.
    refunded(config, source, source.head + 2, [5], tx="0x" + "d0" * 32)
    state = settle(state, source)
    conserved(state)
    refunds = fates.count("refund")
    if refunds:
        # "wait" transfers were proven unfilled too; the leaf pays only the refunded ones.
        refunded(config, source, source.head + 2, [refunds * AMOUNT])
        state = settle(state, source)
        conserved(state)
        assert inventory(state)[SOURCE] == refunds * AMOUNT
    assert inventory(state)[DESTINATION] == arrived * OFFERED


def test_the_report_and_dashboard_show_value_in_flight_apart_from_cash(
    monkeypatch,
    config,  # noqa: F811
):
    from almanak.connectors.composed.dashboard import project as dashboard
    from almanak.connectors.composed.reporting import report

    state, transfer = deposited(monkeypatch, config)
    listed = report(state)
    (row,) = listed["transfers"]
    assert (row["quote_source"], row["resolution"]) == ("across_api", None)
    assert listed["unattributed_refund_payments"] == []
    # The amount in flight is in no inventory balance, and is not reported as one.
    assert [b["amount"] for b in listed["inventory"]] == ["0", "0"]
    assert "must not be added" in listed["transfer_scope"]
    assert listed["positions"][0]["position_scope"].startswith("A bridge holds nothing")
    context, _ = dashboard(state)
    (shown,) = context.datasets["transfers"].rows
    assert (shown["status"], shown["quote_source"]) == ("open", "across_api")
    assert context.values[shown["received"].ref].amount is None
    assert context.values[shown["sent"].ref].amount == str(AMOUNT)
    cash = [context.values[r["amount"].ref].amount for r in context.datasets["fund_inventory"].rows]
    assert cash == ["0", "0"]
    assert [p.allocation for p in context.positions] == ["earn"]
    chain = there()
    state = settle(state, chain)
    filled(config, chain, chain.head + 1)
    state = settle(state, chain)
    context, _ = dashboard(state)
    (shown,) = context.datasets["transfers"].rows
    assert shown["status"] == "arrived"
    assert context.values[shown["received"].ref].amount == str(OFFERED)
    assert report(state)["inventory"][1]["amount"] == str(OFFERED)


def test_a_composed_fork_names_the_constructed_quote_on_the_source_chain_only(config):  # noqa: F811
    from almanak.connectors.across.fork import QUOTE_VENUE, ForkTransport, route_transports
    from almanak.connectors.composed.fork import ComposedFork

    parameters = composed(config)
    venues = ComposedFork.venues(parameters, None)
    assert set(venues) == {FORK_ARB} and set(venues[FORK_ARB]) == {QUOTE_VENUE}
    assert isinstance(route_transports(venues[FORK_ARB])[QUOTE_VENUE], ForkTransport)
    assert route_transports({}) == {}
    with pytest.raises(ValueError, match="only its constructed quote"):
        route_transports({"other": {}})


def test_a_fork_workspace_provisions_an_account_that_only_receives():
    from types import SimpleNamespace

    from almanak.gateway.fork_workspace import select_origin
    from tests.across_support import ARBITRUM, allocation

    funds = (SimpleNamespace(account="arb"), SimpleNamespace(account="base"))
    forks = (SimpleNamespace(chain=ARBITRUM), SimpleNamespace(chain=BASE))
    fixture = SimpleNamespace(allocations=(allocation(),), funds=funds, operators=(), forks=forks)
    fixture.model_copy = lambda update: SimpleNamespace(**update)
    source, destination = select_origin(fixture, ARBITRUM), select_origin(fixture, BASE)
    assert [f.account for f in source.funds] == ["arb"] and len(source.allocations) == 1
    # No allocation acts on the destination chain, yet its receiving account is funded.
    assert [f.account for f in destination.funds] == ["base"] and destination.allocations == ()


def test_the_composed_bridge_reference_stays_out_of_the_production_rehearsal():
    from tests.strategy_fork.rehearsal import declares_production, production_references

    # Across declares no production support, so no market scope of this reference does.
    assert not declares_production("arbitrum_base_bridge_and_lend")
    assert "arbitrum_base_bridge_and_lend" not in production_references()
