"""Authentic keeper reports settle only the reusable strategy's retained fork requests."""

import json

from almanak.connectors.gmx_v2 import fork_keeper
from almanak.connectors.gmx_v2.events import decode_event
from almanak.models import Chain
from tests.fork.support import wait_state


def entered(state):
    return (
        requests_settled(state)
        and positions_open(state)
        and set(state.memory.values.get("opened_at", {}))
        == {o.target.allocation for o in state.owned}
    )


def requests_settled(state):
    return (
        bool(state.requests)
        and not any(r.unresolved for r in state.requests)
        and state.delivered == len(state.outcomes)
        and idle_work(state)
        and all(idle_work(work) for work in state.work)
    )


def idle_work(work):
    return all(value is None for value in (work.decision, work.plan, work.pending))


def positions_open(state):
    return all(o.position is not None for o in state.owned)


def keeper_count(state):
    # A composed state also holds other connectors' outcomes, which carry no
    # `kind` discriminator; only GMX request outcomes count as keeper settlements.
    return sum(getattr(o, "kind", None) == "gmx_request_outcome" for o in state.outcomes)


def assert_holding(node, state):
    assert entered(state)
    assert all(o.position.size_usd_e30 == 1000 * 10**30 for o in state.owned)
    assert all(not o.accounting_issues for o in state.outcomes)
    assert keeper_count(state) == 1


def assert_closed(node, state):
    assert state.mode == "closed" and state.delivered == len(state.outcomes)
    assert_final_effects(state)
    assert requests_settled(state)
    assert keeper_count(state) == 2


def assert_final_effects(state):
    assert all(o.position is None for o in state.owned)
    assert all(not o.accounting_issues for o in state.outcomes)
    assert all(not o.needs_cleanup for o in state.closure)


def drive_external(node, journal, running, source_for, *, block_interval=1, timeout=180):
    state, request = pending_request(node, journal, running, block_interval, timeout)
    source = source_for(request)
    target = state.config.target(request.allocation)
    assert target.fund == request.fund and target.market.share_asset == request.market
    receipt = execute_keeper(node, target, request, source)
    retain_keeper(node, journal, request, source, receipt, block_interval=block_interval)
    assert receipt["status"] == "0x1"
    events = (
        decode_event(log, emitter=target.market.contracts.event_emitter) for log in receipt["logs"]
    )
    event = next(
        event for event in events if event and event.name == request.kind.title() + "Executed"
    )
    assert event.topics[0] == request.key


def pending_request(node, journal, running, block_interval=1, timeout=180):
    node._validate_origin()
    node.rpc("anvil_setBlockTimestampInterval", [block_interval])
    state = wait_state(journal, has_unresolved, running, timeout=timeout)
    requests = [r for r in state.requests if r.unresolved]
    assert len(requests) == 1
    request = requests[0]
    assert request.fund.chain in identified_fork_chains(node)
    return state, request


def identified_fork_chains(node):
    """Chains whose requests this identified fork may settle with an impersonated keeper.

    `_validate_origin` has already bound the node to its instance, fork block and
    signing chain ID. A fork-bound fund carries the node's own Anvil identity. A
    production rehearsal re-identifies the node as its origin chain
    (`anvil_setChainId`) and binds mainnet accounts to it, so the origin chain is
    accepted only while the node signs as that chain.
    """
    local = Chain(
        chain_id=node.origin.chain.chain_id, environment="anvil:" + node.metadata["instanceId"]
    )
    if node.rpc.signing_chain == node.origin.chain.chain_id:
        return (local, node.origin.chain)
    return (local,)


def execute_keeper(node, target, request, source):
    return fork_keeper.execute(node.rpc, target, request, source)


def routed(node, contracts, report):
    return fork_keeper.routed(node.rpc, contracts, report)


def retain_keeper(node, journal, request, source, receipt, *, block_interval=1):
    path = journal.path.parent / "external-execution.json"
    retained = json.loads(path.read_text()) if path.exists() else []
    row = {
        "request": request.model_dump(mode="json"),
        "receipt": receipt,
        "oracle_source": source["oracle_source"],
        "reports": source["reports"],
        "fixture_authority": "Identified fork, existing keeper, authentic signed reports",
        "block_timestamp_interval_seconds": block_interval,
    }
    if receipt["status"] != "0x1":
        row["trace"] = node.rpc(
            "debug_traceTransaction", [receipt["transactionHash"], {"tracer": "callTracer"}]
        )
    path.write_text(json.dumps([*retained, row], indent=2) + "\n")


def has_unresolved(state):
    return any(r.unresolved for r in state.requests)
