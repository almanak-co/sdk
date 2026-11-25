"""A reviewed SpokePool pin change refuses new deposits and traps nothing.

After the pin in code changes, on the source or on the destination chain, a deployment
with transfers in flight and a standing approval still loads, replays its retained
resolution, reconciles an arrival and a refund, and revokes. Only a new deposit waits.
"""

import json
from types import SimpleNamespace

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak.configuration import parameters
from almanak.connectors.across import execution, planning, resolution
from almanak.connectors.across import quote as venue
from almanak.connectors.across.domain import SPOKE_POOLS
from almanak.connectors.across.operations import RevokeOperation, plan_call
from almanak.connectors.across.state import Checkpoint
from almanak.gateway.bootstrap import complete_named_market, resolve_named_market
from almanak.gateway.ethereum import DataError, EthereumReader
from almanak.gateway.execution import VenueAnswers
from almanak.gateway.resolution_evm import ResolutionReader
from almanak.models import Block
from almanak.work import project
from tests.across_settlement_support import settle
from tests.across_support import AMOUNT, ARBITRUM, BASE, BASE_USDC, USDC, quote
from tests.test_across_authoring import CODE, review, selection
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
# What a reviewed pin change can replace: the implementation behind the proxy, that
# implementation's code, or the proxy itself.
KINDS = pytest.mark.parametrize("kind", ["implementation", "code_hash", "proxy"])
CHANGES = {
    "implementation": {"implementation": "0x" + "99" * 20},
    "code_hash": {"implementation_code_hash": "0x" + "ab" * 32},
    "proxy": {"address": "0x" + "77" * 20},
}


def repin(monkeypatch, chain, kind="implementation"):
    """A reviewed change ships: the pin in code names another pool than the one in use."""
    later = SPOKE_POOLS[chain.chain_id].model_copy(update=CHANGES[kind])
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


def asked(chain):
    """Record every contract a fake chain is asked about: its views and its log queries."""
    seen, call, rpc = set(), chain.call, chain._rpc

    def recorded_call(address, *args, **kwargs):
        seen.add(address)
        return call(address, *args, **kwargs)

    def recorded_rpc(method, params, **kwargs):
        if method == "eth_getLogs":
            seen.add(params[0]["address"])
        return rpc(method, params, **kwargs)

    chain.call, chain._rpc = recorded_call, recorded_rpc
    return seen


@SIDES
@KINDS
def test_a_deployment_restarts_settles_and_revokes_after_a_pin_change(
    monkeypatch,
    config,  # noqa: F811
    changed,
    kind,
):
    arriving, expiring = flight(config), flight(config, 1)
    retained = flying(config, arriving, expiring).model_dump_json()
    used = config.target("move").route
    repin(monkeypatch, changed, kind)

    # Restart: the checkpoint and its routes load, with the pools they were resolved on.
    state = Checkpoint.model_validate_json(retained)
    route = state.config.target("move").route
    assert route == used
    pins = (SPOKE_POOLS[ARBITRUM.chain_id].address, SPOKE_POOLS[BASE.chain_id])
    assert ((route.spoke_pool, route.destination_pool) != pins) == (
        kind == "proxy" or changed == BASE
    )

    # The arrival of one transfer is reconciled on the destination chain. The fake
    # chains hold the logs of the pools the deployment used, under their own addresses.
    destination = there()
    there_asked = asked(destination)
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
    here_asked = asked(source)
    state = settle(state, config, source)
    refunded(config, source, source.head + 100, [AMOUNT])
    state = settle(state, config, source)
    refund = state.outcomes[-1]
    assert refund.transfers == (expiring.reference,) and refund.returned.amount == AMOUNT
    assert not any(transfer.open for transfer in state.transfers)
    # Fill logs and `fillStatuses` were asked of the pool the transfers were sent to;
    # refund payments of the sent token, and `getRelayerRefund` of the pool that owes them.
    assert there_asked == {used.destination_pool.address}
    assert here_asked == {used.asset.reference, used.spoke_pool}

    # A standing approval is revoked: a revocation increases no exposure, and its
    # spender is the pool the approval was given to.
    standing = observed(config, allowance=AMOUNT)
    revoke = config.revoke("move")
    planned = execution.quoted_operation(
        backend(config), decided(state, revoke, standing), revoke, standing
    )
    assert planned == RevokeOperation(target=config.target("move"), final=True)
    assert used.spoke_pool[2:] in plan_call(planned).data
    execution.validate_operation(
        backend(config), decided(state, revoke, standing), planned, standing
    )

    # A new deposit, and the approval before it, wait for the review of the change.
    # Planning refuses before the venue is asked, and the gateway's own validation of a
    # step planned elsewhere refuses too.
    monkeypatch.setattr(venue, "fetch", lambda *a, **k: pytest.fail("the venue was asked"))
    bridge = config.bridge("move", AMOUNT)
    for allowance in (0, AMOUNT):
        before = observed(config, allowance=allowance)
        local = decided(state, bridge, before)
        with pytest.raises(DataError, match="new deposits wait for its review") as refusal:
            execution.quoted_operation(backend(config), local, bridge, before)
        assert refusal.value.kind == "deployment_changed"
        step = planning.operation(
            config, local, bridge, before, quote=quote() if allowance else None
        )
        assert step.kind == ("deposit" if allowance else "approve")
        with pytest.raises(DataError, match="new deposits wait for its review") as refusal:
            execution.validate_operation(backend(config), local, step, before)
        assert refusal.value.kind == "deployment_changed"
        assert str(changed.chain_id) in str(refusal.value)
        assert ("is not the reviewed one" in str(refusal.value)) == (kind == "proxy")
    assert Checkpoint.model_validate_json(state.model_dump_json()) == state


class Node(EthereumReader):
    """One chain as a JSON-RPC node answers it: a SpokePool proxy and a token at one block.

    Only the raw transport is faked, so the recording reader retains real requests and
    the retained reader replays them through the real decoding.
    """

    VIEWS = {
        "depositQuoteTimeBuffer()": ("uint32", 3600),
        "fillDeadlineBuffer()": ("uint32", 21_600),
        "pausedDeposits()": ("bool", False),
        "decimals()": ("uint8", 6),
        "symbol()": ("string", "USDC"),
    }

    def __init__(self, block, pool, token):
        super().__init__(block.chain, "https://node.invalid")
        self.block, self.pool, self.token, self.asked = block, pool, token, []

    def _rpc(self, method, params, *, allow_none=False):
        self.asked.append((method, params))
        if method == "eth_chainId":
            return hex(int(str(self.block.chain.chain_id).split(":")[1]))
        if method == "eth_getBlockByNumber":
            assert params == [hex(self.block.number), False]
            return {
                "number": hex(self.block.number),
                "hash": self.block.hash,
                "timestamp": hex(self.block.timestamp),
            }
        assert params[-1] == {"blockHash": self.block.hash, "requireCanonical": True}
        if method == "eth_getStorageAt":
            assert params[0] == self.pool.address
            return "0x" + "00" * 12 + self.pool.implementation[2:]
        if method == "eth_getCode":
            assert params[0] == self.pool.implementation
            return CODE
        assert method == "eth_call" and params[0]["to"] in (self.pool.address, self.token)
        for signature, (kind, value) in self.VIEWS.items():
            if params[0]["data"] == "0x" + keccak(text=signature)[:4].hex():
                return "0x" + encode((kind,), (value,)).hex()
        raise AssertionError(f"an unexpected call: {params[0]}")


class Recorder(ResolutionReader, Node):
    """First resolution's reader over the fake node: it retains what it read."""


HERE = Block(chain=ARBITRUM, number=527_000_000, hash="0x" + "a1" * 32, timestamp=1_795_500_000)
THERE = Block(chain=BASE, number=61_500_000, hash="0x" + "b2" * 32, timestamp=1_795_500_001)


def nodes(kind=Recorder):
    return (
        kind(HERE, SPOKE_POOLS[ARBITRUM.chain_id], USDC.reference),
        kind(THERE, SPOKE_POOLS[BASE.chain_id], BASE_USDC.reference),
    )


def first_resolution(monkeypatch):
    """A two-chain market as the gateway first resolves it, each chain through its own
    recording reader, and the result as retained parameters hold it."""
    review(monkeypatch)
    source, destination = nodes()
    market = SimpleNamespace(name="usdc", selection=selection())
    with source, destination:
        facts = resolve_named_market(source, market, HERE, {})
        facts = complete_named_market(destination, facts, THERE)
    assert facts.evidence and facts.destination_evidence
    return facts


def retained(*markets):
    anchors = SimpleNamespace(checkpoints=(SimpleNamespace(chain=ARBITRUM),))
    return SimpleNamespace(anchors=anchors, markets=markets)


def reads(rows, method, address):
    """The retained rows of one method that name one contract."""
    named = [row for row in rows if row.method == method and address in json.dumps(row.params)]
    assert named
    return named


@SIDES
@KINDS
def test_retained_resolution_replays_after_a_pin_change_and_a_new_one_is_refused(
    monkeypatch, changed, kind
):
    """The real replay path: recorded reads, the retained reader, `replay_facts`."""
    facts = first_resolution(monkeypatch)
    parameters.replay_facts(retained(facts))
    before = dict(SPOKE_POOLS)
    repin(monkeypatch, changed, kind)
    # Replay asks the retained reads about the pools the route was resolved on, and
    # verifies that the facts follow from them. Today's pin is not consulted.
    parameters.replay_facts(retained(facts))
    resolved = type(facts.resolved).model_validate_json(facts.resolved.model_dump_json())
    assert resolved == facts.resolved
    route = resolved.route
    assert route.spoke_pool == before[ARBITRUM.chain_id].address
    assert route.destination_pool == before[BASE.chain_id]
    # The retained facts no longer pass first resolution's check.
    with pytest.raises(DataError, match="new deposits wait for its review") as refusal:
        resolution.admit_route(facts.resolved)
    assert refusal.value.kind == "deployment_changed"
    assert str(changed.chain_id) in str(refusal.value)
    # A chain that still runs what was resolved does not resolve a new market, unless
    # the change is a new proxy: then the new market is read at the new address.
    market = SimpleNamespace(name="usdc", selection=selection())
    source, destination = (
        Recorder(HERE, before[ARBITRUM.chain_id], USDC.reference),
        Recorder(THERE, before[BASE.chain_id], BASE_USDC.reference),
    )
    reader, resolve = (
        (source, lambda: resolve_named_market(source, market, HERE, {}))
        if changed == ARBITRUM
        else (destination, lambda: complete_named_market(destination, facts, THERE))
    )
    with source, destination, pytest.raises((DataError, AssertionError)) as refused:
        resolve()
    if kind == "proxy":
        assert reader.asked[-1][1][0] == SPOKE_POOLS[changed.chain_id].address
    else:
        assert refused.value.kind == "deployment_changed"


@SIDES
def test_replay_refuses_a_route_whose_pool_reads_are_not_retained(monkeypatch, changed):
    facts = first_resolution(monkeypatch)
    field = "evidence" if changed == ARBITRUM else "destination_evidence"
    pool = SPOKE_POOLS[changed.chain_id]
    rows = getattr(facts, field)
    for method in ("eth_getStorageAt", "eth_call"):
        missing = reads(rows, method, pool.address)[0]
        kept = tuple(row for row in rows if row is not missing)
        with pytest.raises(DataError, match="lacks a required read") as refusal:
            parameters.replay_facts(retained(facts.model_copy(update={field: kept})))
        assert refusal.value.kind == "resolution_evidence"
    # Retained facts that name another pool than the reads were made of do not replay:
    # nobody can move a deployment to a pool by editing its retained route.
    name = "pool" if changed == ARBITRUM else "destination_pool"
    moved = getattr(facts.resolved, name).model_copy(update={"address": "0x" + "55" * 20})
    forged = facts.model_copy(update={"resolved": facts.resolved.model_copy(update={name: moved})})
    with pytest.raises(DataError, match="lacks a required read"):
        parameters.replay_facts(retained(forged))
    # And facts the reads do not give are refused as before.
    paused = getattr(facts.resolved, name).model_copy(update={"deposits_paused": True})
    forged = facts.model_copy(update={"resolved": facts.resolved.model_copy(update={name: paused})})
    with pytest.raises(DataError, match="resolved facts differ from retained reads"):
        parameters.replay_facts(retained(forged))


def test_first_resolution_holds_both_pools(monkeypatch):
    facts = first_resolution(monkeypatch)
    assert facts.symbols == {USDC: "USDC", BASE_USDC: "USDC"}
    assert facts.resolved.pool.address == SPOKE_POOLS[ARBITRUM.chain_id].address
    assert facts.resolved.route.destination_pool == SPOKE_POOLS[BASE.chain_id]
    # The destination's reads are retained apart and name the destination pool.
    assert reads(facts.destination_evidence, "eth_getStorageAt", SPOKE_POOLS[BASE.chain_id].address)
    for chain in (ARBITRUM, BASE):
        with monkeypatch.context() as patch:
            repin(patch, chain, "code_hash")
            source, destination = nodes()
            market = SimpleNamespace(name="usdc", selection=selection())
            with source, destination, pytest.raises(DataError, match=f"{chain.chain_id} runs"):
                partial = resolve_named_market(source, market, HERE, {})
                complete_named_market(destination, partial, THERE)


def test_a_route_keeps_the_pools_it_was_resolved_on(monkeypatch):
    """A retained route of other pools than today's pins loads: it is the deployment's."""
    route = first_resolution(monkeypatch).resolved.route
    for chain in (ARBITRUM, BASE):
        with monkeypatch.context() as patch:
            repin(patch, chain, "proxy")
            assert type(route).model_validate_json(route.model_dump_json()) == route
    assert FORK_BASE.chain_id == BASE.chain_id
