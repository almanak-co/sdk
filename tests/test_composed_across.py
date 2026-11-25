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
from almanak.connectors.composed.execution import (
    ComposedExecution,
    held_snapshot,
    held_wallets,
    planned,
    validate_inventory,
)
from almanak.connectors.composed.inventory import ownership, repayment_top_up, validate
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
from almanak.connectors.compound_v3.operations import RepayOperation, SupplyOperation
from almanak.connectors.identifiers import ConnectorId
from almanak.effects import Balance, WalletAsset
from almanak.gateway import external_effects as gateway_effects
from almanak.gateway.ethereum import DataError
from almanak.gateway.evm import native_asset
from almanak.models import AssetAmount, Model
from almanak.strategy import StrategyDefinition
from almanak.transfers import Arrived
from almanak.work import merge, project
from tests.across_support import AMOUNT, BASE, BASE_USDC, BASE_WETH, USDC
from tests.test_across_records import (
    FORK_ARB,
    FORK_BASE,
    deposit,
    mined_deposit,
    observed,
    pending,
)
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


def earn(asset=BASE_USDC, name="earn", account="base", chain=FORK_BASE, base=BASE_USDC):
    market = Market(chain=chain, comet=COMET, base_asset=base, base_price_feed=FEED)
    return CompoundAllocation(
        name=name,
        account=account,
        reserve=Reserve(market=market, asset=asset, price_feed=FEED),
        native_asset=native_asset(chain.chain_id),
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
    # Naming the delivered asset in the inventory does not make an allocation hold it:
    # the cause reported is the bridge's, not a surplus inventory entry.
    with pytest.raises(ValidationError, match="arrive in a wallet asset"):
        composed(
            config,
            allocations=(*config.allocations, earn(BASE_WETH)),
            inventory=(*balances(), Balance(key=elsewhere, amount=0)),
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


class Debt(Model):
    borrowed_base: int


class Wallet(Model):
    """What a full repay reads of a Comet snapshot: the debt and the wallet balance."""

    wallet_balance: int
    account: Debt


def indebted(across, **changes):
    """The composed bridge with Comet debt in the bridged asset on both accounts."""
    owe = earn(USDC, name="owe", account="arb", chain=FORK_ARB, base=USDC)
    return composed(across, allocations=(*across.allocations, earn(), owe), **changes)


def repay(state, allocation, bound):
    target = state.config.target(allocation)
    amount = AssetAmount(asset=target.asset, amount=bound)
    return RepayOperation(target=target, amount=amount, full=True)


def wallet(balance, debt):
    return Wallet(wallet_balance=balance, account=Debt(borrowed_base=debt))


def spent(state, allocation, amount):
    """The top-up a mined full repay that spent `amount` books against the inventory."""
    from types import SimpleNamespace

    operation = repay(state, allocation, amount)
    key = WalletAsset(
        account=operation.target.account,
        chain=operation.target.fund.chain,
        asset=operation.target.asset,
    )
    outcome = SimpleNamespace(operation=operation, success=True)
    return repayment_top_up(state.inventory, outcome, (SimpleNamespace(key=key, amount=-amount),))


def test_a_full_repay_never_absorbs_a_mined_and_unrecorded_arrival(config):  # noqa: F811
    """The fill is mined after the tick's scan found nothing and before the repay's
    snapshot. The wallet holds it; no record does. A full repay of debt in that asset
    reads only the retained inventory, so the arrival is credited once."""
    retained, debt = 40, 100
    state = checkpoint(
        indebted(config), transfers=(flight(config),), inventory=balances(0, retained)
    )
    local = project(state, FORK_BASE)
    assert held_wallets(local) == {SOURCE, DESTINATION}
    before = wallet(retained + OFFERED, debt)
    # Unheld, this repay would pass and book 60 of the arrival as an operator top-up.
    free = local.model_copy(update={"transfers": ()})
    validate_inventory(free, repay(state, "earn", debt), before)
    assert spent(free, "earn", debt)[0].amount == debt - retained
    with pytest.raises(DataError, match="not counted while a bridge transfer") as error:
        validate_inventory(local, repay(state, "earn", debt), before)
    assert error.value.kind == "repayment_funding"
    # The lending component plans and validates from the wallet cut to the inventory.
    target = state.config.target("earn")
    assert held_snapshot(local, target, before).wallet_balance == retained
    assert planned(local, target, before, lambda seen: seen.wallet_balance) == retained
    # A repay the retained inventory covers proceeds, and absorbs nothing.
    covered = wallet(retained + OFFERED, retained)
    validate_inventory(local, repay(state, "earn", retained), covered)
    assert spent(local, "earn", retained) == ()
    # An approval standing from before the transfer opened cannot widen the repay.
    with pytest.raises(DataError, match="standing repay approval"):
        validate_inventory(local, repay(state, "earn", retained + 1), covered)
    # The arrival is recorded once, and with it the top-up path works again.
    chain = there()
    filled(config, chain, chain.head + 1)
    for _ in range(3):
        state = settle(state, chain)
    assert inventory(state)[DESTINATION] == retained + OFFERED
    local = project(state, FORK_BASE)
    assert held_wallets(local) == set() and held_snapshot(local, target, before) is before
    topped = wallet(retained + OFFERED + 7, retained + OFFERED + 7)
    validate_inventory(local, repay(state, "earn", retained + OFFERED + 7), topped)
    assert spent(local, "earn", retained + OFFERED + 7)[0].amount == 7


def test_a_full_repay_never_absorbs_a_mined_and_unrecorded_refund(config):  # noqa: F811
    retained, debt = 40, 100
    state = checkpoint(
        indebted(config), transfers=(flight(config),), inventory=balances(retained, 0)
    )
    state = expire(state)
    assert state.transfers[0].unfilled and state.transfers[0].open
    local = project(state, FORK_ARB)
    target = state.config.target("owe")
    # The refund leaf paid the fund before the source scan recorded it.
    before = wallet(retained + AMOUNT, debt)
    with pytest.raises(DataError, match="not counted while a bridge transfer"):
        validate_inventory(local, repay(state, "owe", debt), before)
    assert held_snapshot(local, target, before).wallet_balance == retained
    validate_inventory(local, repay(state, "owe", retained), wallet(retained + AMOUNT, retained))
    chain = here(FILL_DEADLINE + 50)
    state = settle(state, chain)
    refunded(config, chain, chain.head + 3, [AMOUNT])
    state = settle(state, chain)
    assert inventory(state)[SOURCE] == retained + AMOUNT
    local = project(state, FORK_ARB)
    assert held_wallets(local) == set()
    validate_inventory(local, repay(state, "owe", debt), before)
    assert spent(local, "owe", debt) == ()


def test_a_deposit_another_chain_has_signed_holds_the_wallet_it_delivers_into(
    config,  # noqa: F811
):
    """A signed deposit can be mined and filled before its outcome opens the transfer.
    The destination chain's view names the wallet until then; nothing retains the name."""
    signed = pending(config, deposit(config), observed(config))
    state = checkpoint(indebted(config), inventory=balances(AMOUNT, 40))
    signing = tuple(
        work.model_copy(update={"pending": signed, "decision": signed.decision})
        if work.chain == FORK_ARB
        else work
        for work in state.work
    )
    state = state.model_copy(update={"work": signing})
    assert state.inbound == () and "inbound" not in state.model_dump(mode="json")
    assert Checkpoint.model_validate_json(state.model_dump_json()) == state
    local = project(state, FORK_BASE)
    assert local.inbound == (DESTINATION,) and project(state, FORK_ARB).inbound == ()
    with pytest.raises(DataError, match="not counted while a bridge transfer"):
        validate_inventory(local, repay(state, "earn", 100), wallet(40 + OFFERED, 100))
    # The view's field is not merged back, and a whole checkpoint cannot carry it.
    assert merge(state, FORK_BASE, local).inbound == ()
    with pytest.raises(ValidationError, match="never retained"):
        Checkpoint.model_validate({**state.model_dump(), "inbound": (DESTINATION,)})


def test_a_held_planner_names_the_hold_instead_of_a_top_up(config):  # noqa: F811
    state = checkpoint(indebted(config), transfers=(flight(config),), inventory=balances(0, 40))
    local, target = project(state, FORK_BASE), state.config.target("earn")

    def refuse(seen):
        raise DataError("repayment_funding", f"send the shortfall beyond {seen.wallet_balance}")

    with pytest.raises(DataError, match="not counted while a bridge transfer"):
        planned(local, target, wallet(500, 100), refuse)
    # Unheld, the connector's own refusal and its top-up remedy stand.
    free = local.model_copy(update={"transfers": ()})
    with pytest.raises(DataError, match="send the shortfall beyond 500"):
        planned(free, target, wallet(500, 100), refuse)


def test_only_a_deployment_with_a_bridge_declares_the_venue_transport(config):  # noqa: F811
    from almanak.connectors.across import execution

    bridged = composed(config)
    assert set(ComposedExecution.venue_transports(bridged)) == set(execution.venue_transports())
    lending = bridged.component(COMPOUND)
    alone = Config(
        decision_trigger=bridged.decision_trigger,
        accounts=lending.accounts,
        allocations=(earn(),),
        inventory=balances()[1:],
        fork_bindings=tuple(b for b in bridged.fork_bindings if b.chain == FORK_BASE),
    )
    assert ComposedExecution.venue_transports(alone) == {}


FATES = ("arrive", "refund", "wait")


@pytest.mark.parametrize("fates", list(product(FATES, repeat=3)), ids="-".join)
def test_inventory_is_conserved_over_every_sequence_of_settlements(config, fates):  # noqa: F811
    """Three equal transfers each arrive, are refunded or stay in flight, beside a
    refund-leaf payment that matches nothing. After every page, what the funds own in
    wallets plus what is in flight plus the fees arrivals paid equals what they began with.
    Both funds also owe Comet debt in the bridged asset: whenever a wallet holds tokens
    of a transfer that no page has recorded yet, a full repay reads only the inventory.
    """
    transfers = tuple(flight(config, serial) for serial in range(3))
    state = checkpoint(indebted(config), transfers=transfers, inventory=balances(source=0))
    total = 3 * AMOUNT

    def held(current, chain, key, allocation, paid):
        """`paid` is what transfers have paid this wallet on chain so far."""
        unrecorded = paid - inventory(current)[key]
        assert unrecorded >= 0
        if unrecorded:
            before = wallet(inventory(current)[key] + unrecorded, total)
            seen = held_snapshot(project(current, chain), current.config.target(allocation), before)
            assert seen.wallet_balance == inventory(current)[key]

    def conserved(current):
        in_flight = sum(t.sent.amount for t in current.transfers if t.open)
        fees = sum(
            t.sent.amount - t.resolution.received.amount
            for t in current.transfers
            if getattr(t.resolution, "received", None) is not None
        )
        assert sum(inventory(current).values()) + in_flight + fees == total
        assert Checkpoint.model_validate_json(current.model_dump_json()) == current

    destination, paid = there(), 0
    for serial, fate in enumerate(fates):
        if fate == "arrive":
            filled(config, destination, destination.head + 1, serial, tx=f"0x{serial + 1:064x}")
            paid += OFFERED
        held(state, FORK_BASE, DESTINATION, "earn", paid)
        for _ in range(4):
            state = settle(state, destination)
            conserved(state)
            held(state, FORK_BASE, DESTINATION, "earn", paid)
        # Each wallet by its own key: a total alone would not notice an arrival
        # credited to the source. The transfers not yet reached are still open.
        assert inventory(state) == {SOURCE: 0, DESTINATION: paid}
        assert sum(t.open for t in state.transfers) == 3 - paid // OFFERED
        arrived_so_far = [isinstance(t.resolution, Arrived) for t in state.transfers]
        assert arrived_so_far == [f == "arrive" for f in fates[: serial + 1]] + [False] * (
            2 - serial
        )
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
    # So is a payment the SpokePool made in another amount than its leaf names, even
    # where the leaf names exactly what a waiting transfer sent.
    refunded(config, source, source.head + 2, [AMOUNT], payments=[AMOUNT - 1], tx="0x" + "d3" * 32)
    state = settle(state, source)
    conserved(state)
    assert inventory(state)[SOURCE] == 0
    strays = [p.amount.amount for page in state.external_scans for p in page.unsolicited]
    # A refund is read only while a transfer is proven unfilled.
    assert strays == ([5, AMOUNT - 1] if "refund" in fates else [])
    refunds = fates.count("refund")
    if refunds:
        # "wait" transfers were proven unfilled too; the leaf pays only the refunded ones.
        # A second stranger's payment shares the refund's block, ahead of it in log order.
        number = source.head + 2
        refunded(config, source, number, [7], tx="0x" + "d4" * 32)
        refunded(config, source, number, [refunds * AMOUNT])
        held(state, FORK_ARB, SOURCE, "owe", refunds * AMOUNT)
        state = settle(state, source)
        held(state, FORK_ARB, SOURCE, "owe", refunds * AMOUNT)
        conserved(state)
        assert inventory(state)[SOURCE] == refunds * AMOUNT
        last = state.external_scans[-1]
        assert [p.amount.amount for p in last.unsolicited] == [7] and last.outcome is not None
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
    assert "unattributed_refund_payments" not in context.datasets
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


def test_an_unattributed_refund_payment_has_its_own_dashboard_table(config):  # noqa: F811
    """A stranger's refund-leaf payment is in the wallet and attributed to nothing: the
    dashboard lists it with its block, apart from the transfer amounts and from fund cash."""
    from almanak.connectors.composed.dashboard import project as dashboard
    from almanak.connectors.composed.reporting import report

    unfilled = flight(config).model_copy(update={"unfilled": True})
    state = checkpoint(composed(config), transfers=(unfilled,), inventory=balances(source=0))
    chain = here(FILL_DEADLINE + 50)
    state = settle(state, chain)
    refunded(config, chain, chain.head + 2, [5], tx="0x" + "d0" * 32)
    state = settle(state, chain)
    page = state.external_scans[-1]
    context, _ = dashboard(state)
    (paid,) = context.datasets["unattributed_refund_payments"].rows
    assert (paid["allocation"], paid["account"]) == ("move", "arb")
    assert (paid["block"], paid["transaction"]) == (str(page.end.number), "0x" + "d0" * 32)
    value = context.values[paid["amount"].ref]
    assert (value.amount, value.asset, value.block) == ("5", USDC, page.end)
    # It is no transfer amount and no cash: the transfer stays open and cash stays zero.
    (shown,) = context.datasets["transfers"].rows
    assert shown["status"] == "unfilled" and context.values[shown["returned"].ref].amount is None
    assert paid["amount"].ref not in {shown[name].ref for name in ("sent", "offered")}
    cash = [context.values[r["amount"].ref].amount for r in context.datasets["fund_inventory"].rows]
    assert cash == ["0", "0"]
    (listed,) = report(state)["unattributed_refund_payments"]
    assert (listed["block"], listed["amount"]["amount"]) == (page.end.number, 5)


def test_a_refund_paid_in_another_amount_than_its_leaf_is_listed_with_both(config):  # noqa: F811
    """The composed report and dashboard show the paid and the leaf amount, credit no
    inventory and keep the transfer among those awaiting a refund."""
    from almanak.connectors.composed.dashboard import project as dashboard
    from almanak.connectors.composed.reporting import report

    unfilled = flight(config).model_copy(update={"unfilled": True})
    state = checkpoint(composed(config), transfers=(unfilled,), inventory=balances(source=0))
    chain = here(FILL_DEADLINE + 50)
    state = settle(state, chain)
    refunded(config, chain, chain.head + 2, [AMOUNT], tx="0x" + "d3" * 32, payments=[AMOUNT - 1])
    state = settle(state, chain)
    assert state.transfers[0].open and state.external_scans[-1].outcome is None
    listed = report(state)
    (payment,) = listed["unattributed_refund_payments"]
    assert (payment["amount"]["amount"], payment["leaf_amount"]["amount"]) == (AMOUNT - 1, AMOUNT)
    assert listed["awaiting_refund"] == [unfilled.reference]
    assert "never applied" in listed["awaiting_refund_note"]
    assert listed["inventory"][0]["amount"] == "0"
    context, _ = dashboard(state)
    (paid,) = context.datasets["unattributed_refund_payments"].rows
    assert context.values[paid["amount"].ref].amount == str(AMOUNT - 1)
    assert context.values[paid["leaf_amount"].ref].amount == str(AMOUNT)
    cash = [context.values[r["amount"].ref].amount for r in context.datasets["fund_inventory"].rows]
    assert cash == ["0", "0"]


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


def test_both_bridge_references_are_in_the_production_rehearsal_and_the_weekly_matrix():
    """The source chain declares production, so both references are rehearsed by default
    and the hosted qualification holds their cell. Every other pinned chain declares its
    modes with no reference behind it, an exception accepted for Across alone, so the
    weekly selection leaves those cells out by name and owes them no reference."""
    from almanak.connectors.across import support
    from scripts.ci import fork_qualification_matrix as matrix
    from tests.strategy_fork import rehearsal

    bridges = {"across_usdc_arbitrum_base_bridge", "arbitrum_base_bridge_and_lend"}
    assert support.PRODUCTION_SOURCES == support.FORK_SOURCES
    assert not bridges & rehearsal.UNREHEARSED
    assert all(rehearsal.declares_production(name) for name in bridges)
    assert bridges <= set(rehearsal.production_references())
    weekly = matrix.production_references()
    assert set(weekly[("across", "eip155:42161")]) == bridges
    unproven = matrix.declared_without_proof()
    assert unproven == {("across", str(c)) for c in support.declared_without_proof()}
    assert ("across", "eip155:8453") in unproven and not unproven & set(weekly)
    assert {c for c in unproven if c[0] != "across"} == set()
    # The QA catalog leaves the same cells out, and still owes the proven one.
    from tests.qa import catalog

    left_out = catalog.declared_without_proof()
    assert {(name, str(chain)) for name, chain in left_out} == unproven
    owed = {(str(o.connector), str(o.chain)) for o in catalog.required()}
    assert ("across", "eip155:42161") in owed and not owed & unproven
    entries = matrix.matrix(sorted(bridges), rehearsal=True, lifecycle=False)
    assert all(entry["rehearsal"] and entry["nodes"] for entry in entries)


def test_without_the_declaration_no_bridge_reference_is_rehearsed(monkeypatch):
    """The declaration alone selects them: with it withdrawn, neither is a production
    reference and the cell leaves the weekly matrix."""
    from almanak.connectors.across import support
    from scripts.ci import fork_qualification_matrix as matrix
    from tests.strategy_fork import rehearsal

    monkeypatch.setattr(support, "PRODUCTION_SOURCES", ())
    bridges = {"across_usdc_arbitrum_base_bridge", "arbitrum_base_bridge_and_lend"}
    assert not any(rehearsal.declares_production(name) for name in bridges)
    assert not bridges & set(rehearsal.production_references())
    assert ("across", "eip155:42161") not in matrix.production_references()


def test_the_support_row_states_the_modes_it_declares_and_no_other(monkeypatch):
    from almanak.connectors.across import support
    from almanak.connectors.support import RunMode

    source, destination = support.candidate_support()[:2]
    assert source.modes == (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST)
    text = " | ".join(source.restrictions)
    assert "fork runs, production runs and economic backtests only" in text
    assert "no production run" not in text
    assert "a production run signs the venue's own quote" in text
    assert "a fork run asks no venue" in text and "a backtest models" in text
    # Every pinned chain declares the same modes; only the unproven ones say so.
    assert destination.modes == source.modes
    assert support.WITHOUT_PROOF[0] in destination.restrictions
    assert support.WITHOUT_PROOF[0] not in source.restrictions
    # Each mode's sentence follows its declaration and nothing else.
    monkeypatch.setattr(support, "PRODUCTION_SOURCES", ())
    without = " | ".join(support.candidate_support()[0].restrictions)
    assert "fork runs and economic backtests only" in without and "no production run" in without
    assert "a production run" not in without
    monkeypatch.setattr(support, "BACKTEST_STARTS", {})
    alone = " | ".join(support.candidate_support()[0].restrictions)
    assert (
        "fork runs only, as the source of a route to another pinned chain; no production run"
        in alone
    )
    assert "a backtest models" not in alone


def test_the_composed_fork_keeper_settles_transfers_through_the_bridge_component(
    monkeypatch,
    config,  # noqa: F811
):
    from almanak.connectors.across.settlement import ExternalEffects
    from almanak.connectors.composed.requests import ComposedRequests

    seen = []

    class Settler:
        def align(self, state, seconds):
            return seconds + 5

        def settle(self, state):
            seen.append((state.config, state.transfers))
            return ({"advanced_seconds": 0},)

    monkeypatch.setattr(ExternalEffects, "fork_keeper", staticmethod(lambda *a: Settler()))
    keeper = ComposedRequests.fork_keeper(None, None)
    quiet = checkpoint(composed(config))
    assert not ComposedRequests.fork_unsettled(quiet) and not ComposedRequests.unresolved(quiet)
    state, transfer = deposited(monkeypatch, config)
    # Request handling still asks about GMX requests alone; only the fork keeper is owed.
    assert ComposedRequests.fork_unsettled(state) and not ComposedRequests.unresolved(state)
    assert keeper.align(state, 60) == 65
    assert tuple(keeper.settle(state)) == ({"advanced_seconds": 0},)
    assert seen == [(state.config.component(ACROSS), (transfer,))]
