"""A reviewed SpokePool pin change refuses new deposits and traps nothing.

After the pin in code changes, on the source or on the destination chain, a deployment
with transfers in flight and a standing approval still loads, replays its retained
resolution, reconciles an arrival and a refund, and revokes. Only a new deposit waits.
"""

from types import SimpleNamespace

import pytest

from almanak.configuration import parameters
from almanak.connectors.across import execution, resolution
from almanak.connectors.across import quote as venue
from almanak.connectors.across.domain import SPOKE_POOLS
from almanak.connectors.across.operations import RevokeOperation
from almanak.connectors.across.state import Checkpoint
from almanak.gateway.bootstrap import MarketFacts, complete_named_market, resolve_named_market
from almanak.gateway.ethereum import DataError
from almanak.gateway.execution import VenueAnswers
from almanak.work import project
from tests.across_settlement_support import settle
from tests.across_support import AMOUNT, ARBITRUM, BASE, BASE_USDC, USDC, quote
from tests.test_across_authoring import (
    BLOCK,
    VALUES,
    Reader,
    SymbolReader,
    review,
    selection,
)
from tests.test_across_records import FORK_ARB, FORK_BASE, observed
from tests.test_across_records import config as config  # noqa: F401 - a fixture
from tests.test_across_settlement import (
    FILL_DEADLINE,
    filled,
    flight,
    flying,
    here,
    refunded,
    there,
)

SIDES = pytest.mark.parametrize("changed", [ARBITRUM, BASE], ids=["source", "destination"])


def repin(monkeypatch, chain):
    """A reviewed upgrade ships: the pin in code names another implementation."""
    later = SPOKE_POOLS[chain.chain_id].model_copy(
        update={"implementation": "0x" + "99" * 20, "implementation_code_hash": "0x" + "ab" * 32}
    )
    monkeypatch.setitem(SPOKE_POOLS, chain.chain_id, later)


def backend(config):  # noqa: F811
    return SimpleNamespace(
        config=config,
        guard=lambda: None,
        venue_transports=execution.venue_transports(),
        venue_answers=VenueAnswers(),
    )


def decided(state, intent, before):
    """The source chain's view with `intent` as its retained decision and `before` as
    the allowance its records account for."""
    local = project(state, FORK_ARB)
    decision = SimpleNamespace(intent=intent, identity="0" * 32)
    return local.model_copy(update={"decision": decision, "baseline": (before,)})


@SIDES
def test_a_deployment_restarts_settles_and_revokes_after_a_pin_change(
    monkeypatch,
    config,  # noqa: F811
    changed,
):
    arriving, expiring = flight(config), flight(config, 1)
    retained = flying(config, arriving, expiring).model_dump_json()
    repin(monkeypatch, changed)

    # Restart: the checkpoint and its routes load, with the pools they were resolved on.
    state = Checkpoint.model_validate_json(retained)
    route = state.config.target("move").route
    assert route.destination_pool != SPOKE_POOLS[BASE.chain_id] or changed == ARBITRUM

    # The arrival of one transfer is reconciled on the destination chain.
    destination = there()
    state = settle(state, config, destination)
    filled(config, destination, destination.head + 5)
    destination.head += 30
    state = settle(state, config, destination)
    (arrival,) = state.outcomes
    assert arrival.strategy_result().transfer == arriving.reference
    state = state.model_copy(update={"delivered": 1})

    # The other is proven unfilled there and its refund measured on the source chain.
    # The fake serves one fill status for every relay; the open transfer's is unfilled.
    destination.views["fillStatuses(bytes32)"] = 0
    destination.head = destination.at(FILL_DEADLINE + 10)
    for _ in range(6):
        state = settle(state, config, destination)
    assert [transfer.unfilled for transfer in state.transfers] == [False, True]
    source = here(FILL_DEADLINE + 50)
    state = settle(state, config, source)
    refunded(config, source, source.head + 100, [AMOUNT])
    state = settle(state, config, source)
    refund = state.outcomes[-1]
    assert refund.transfers == (expiring.reference,) and refund.returned.amount == AMOUNT
    assert not any(transfer.open for transfer in state.transfers)

    # A standing approval is revoked: a revocation increases no exposure.
    standing = observed(config, allowance=AMOUNT)
    revoke = config.revoke("move")
    planned = execution.quoted_operation(
        backend(config), decided(state, revoke, standing), revoke, standing
    )
    assert planned == RevokeOperation(target=config.target("move"), final=True)
    execution.validate_operation(
        backend(config), decided(state, revoke, standing), planned, standing
    )

    # A new deposit, and the approval before it, wait for the review of the upgrade.
    monkeypatch.setattr(venue, "fetch", lambda *a, **k: quote())
    bridge = config.bridge("move", AMOUNT)
    for allowance in (0, AMOUNT):
        before = observed(config, allowance=allowance)
        local = decided(state, bridge, before)
        step = execution.quoted_operation(backend(config), local, bridge, before)
        assert step.kind == ("deposit" if allowance else "approve")
        with pytest.raises(DataError, match="new deposits wait for its review") as refusal:
            execution.validate_operation(backend(config), local, step, before)
        assert refusal.value.kind == "deployment_changed"
        assert str(changed.chain_id) in str(refusal.value)
    assert Checkpoint.model_validate_json(state.model_dump_json()) == state


def resolved_facts(monkeypatch):
    """A two-chain market as first resolution retains it, and its destination reader."""
    review(monkeypatch)
    monkeypatch.setitem(VALUES, "symbol()", "USDC")
    anchor = BLOCK.model_copy(update={"chain": BASE, "number": 30_000_000})
    source = resolution.resolve_market(Reader(), selection(), BLOCK)
    partial = MarketFacts(name="usdc", resolved=source, evidence=(), symbols={USDC: "USDC"})
    facts = complete_named_market(SymbolReader(anchor), partial, anchor)
    monkeypatch.setattr(
        parameters, "RetainedResolutionReader", lambda chain, evidence: SymbolReader(anchor)
    )
    return facts, anchor


@SIDES
def test_retained_resolution_replays_after_a_pin_change_and_a_new_one_is_refused(
    monkeypatch, changed
):
    facts, anchor = resolved_facts(monkeypatch)
    assert parameters.replayed_market(Reader(), facts) == (facts.resolved, facts.symbols)
    repin(monkeypatch, changed)
    # Replay verifies that the facts follow from the retained reads, not today's pin.
    assert parameters.replayed_market(Reader(), facts) == (facts.resolved, facts.symbols)
    assert type(facts.resolved).model_validate_json(facts.resolved.model_dump_json())
    assert facts.resolved.route.destination_pool.implementation == (
        facts.resolved.destination_pool.implementation
    )
    # The same reads no longer resolve a new market.
    with pytest.raises(DataError, match=f"{changed.chain_id} runs another") as refusal:
        resolution.admit_route(facts.resolved)
    assert refusal.value.kind == "deployment_changed"
    partial = MarketFacts(
        name="usdc",
        resolved=resolution.resolve_market(Reader(), selection(), BLOCK),
        evidence=(),
        symbols={USDC: "USDC"},
    )
    with pytest.raises(DataError, match="runs another implementation"):
        if changed == ARBITRUM:
            market = SimpleNamespace(name="usdc", selection=selection())
            resolve_named_market(RecordingReader(), market, BLOCK, {})
        else:
            complete_named_market(SymbolReader(anchor), partial, anchor)


class RecordingReader(Reader):
    """The source reader as first resolution uses it: it retains what it read."""

    def __init__(self):
        super().__init__()
        self.evidence = []


def test_first_resolution_holds_both_pools_and_replay_stays_a_consistency_check(monkeypatch):
    facts, anchor = resolved_facts(monkeypatch)
    market = SimpleNamespace(name="usdc", selection=selection())
    first = resolve_named_market(RecordingReader(), market, BLOCK, {})
    assert first.resolved.pool == facts.resolved.pool and first.symbols == {USDC: "USDC"}
    assert facts.symbols == {USDC: "USDC", BASE_USDC: "USDC"}
    # Replay still refuses facts its retained reads do not give.
    other = facts.resolved.destination_pool.model_copy(update={"deposits_paused": True})
    forged = facts.model_copy(
        update={"resolved": facts.resolved.model_copy(update={"destination_pool": other})}
    )
    observed_facts, _ = parameters.replayed_market(Reader(), forged)
    assert observed_facts != forged.resolved


def test_a_route_of_another_destination_spoke_pool_does_not_load(monkeypatch):
    facts, _ = resolved_facts(monkeypatch)
    route = facts.resolved.route
    moved = route.destination_pool.model_copy(update={"address": "0x" + "55" * 20})
    with pytest.raises(ValueError, match="another destination SpokePool"):
        type(route)(**{**dict(route), "destination_pool": moved})
    assert FORK_BASE.chain_id == BASE.chain_id
