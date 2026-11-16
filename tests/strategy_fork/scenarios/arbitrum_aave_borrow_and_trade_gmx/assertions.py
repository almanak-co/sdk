"""One fund borrows, settles two GMX requests, repays and releases collateral."""

import json

from almanak.connectors.composed.inventory import cash_evidence
from almanak.connectors.composed.lifecycle import component_state
from almanak.connectors.composed.models import wallet_key
from almanak.connectors.identifiers import ConnectorId
from tests import gmx_fork, gmx_position_path, gmx_swap_permissions
from tests.fork.support import wait_state
from tests.strategy_fork.aave_chain_assertions import settled
from tests.strategy_fork.aave_v3_authority import assert_permissions as lending_permissions
from tests.strategy_fork.scenarios.aave_v3_ethereum_lending.assertions import (
    assert_zero_obligations,
)

crash_operation = "gmx_create_swap"
crash_stage = "broadcast"
# The rehearsal's closure wait starts once the return request settles, at about
# the revoke decision. On 5b4de702 the two passing Safe lifecycles took at most
# 753 s and 861 s from that decision to the retained closed state, and the
# rehearsal expired at 600 s with only the withdrawal left. 861 s with 35 percent
# headroom, in 300 s steps.
teardown_timeout = 1200
# A composed hold evaluation observes both connectors. On 1d9cb8f7 the rehearsal
# hold saw one evaluation start 4 s into its 30 s default window and the next one,
# due 5 s later, still not started when the window closed, so one evaluation took
# more than 21 s. Two scheduled evaluations need the 180 s the composed Aave
# siblings declare.
hold_timeout = 180
# The entry request exists only after the supply, the borrow and the request step.
# Hosted run 35943488873 (accf84af) committed the borrow decision 167 s after the
# supply decision, so the keeper driver's 180 s default expired before the request.
# The sibling Arbitrum Curve Safe lifecycle bounds the remaining one- and
# two-transaction steps at 123 s and 188 s; this bound is at least 1.35 times that
# estimate, in 300 s steps, and also covers settlement delivery.
request_timeout = 900


def entered(state):
    return settled(state) and state.memory.values.get("phase") == 3


def assert_permissions(node, config):
    return lending_permissions(node, config.component(ConnectorId("aave_v3")))


def assert_holding(node, state):
    assert entered(state) and state.mode == "running"
    assert min(state.owned[0].supply_scaled, state.owned[1].debt_scaled) > 0
    assert len(state.requests) == 1 and state.requests[0].status == "executed"
    assert int(state.memory.values["trade_output"]["amount"]) > 0
    assert_bookkeeping(state)


def assert_bookkeeping(state):
    assert_outcomes_complete(state)
    owned = state.owned[-1]
    assert owned.market_tokens.amount == 0
    shared = {balance.key: balance.amount for balance in state.inventory}
    assert_owned_inventory(owned, shared)
    assert_requests_resolved(state)


def assert_outcomes_complete(state):
    for outcome in state.outcomes:
        assert not outcome.accounting_issues
        assert outcome.success


def assert_owned_inventory(owned, shared):
    for value in owned.inventory:
        assert value.amount == shared[wallet_key(owned.target, value.asset)]


def assert_requests_resolved(state):
    for request in state.requests:
        assert not request.unresolved


def assert_closed(node, state):
    assert state.mode == "closed" and settled(state)
    assert len(state.requests) == 2 and all(r.status == "executed" for r in state.requests)
    assert state.memory.values["trade_output"] is None
    assert_bookkeeping(state)
    assert_closure(state)
    assert_completed_sequence(state)
    view = component_state(state, ConnectorId("gmx_v2"))
    assert len(view.requests) == 2 and len(view.owned) == 1


def assert_closure(state):
    for observation in state.closure[:2]:
        assert_zero_obligations(observation)
    assert not state.closure[-1].needs_cleanup
    measured = cash_evidence(state.closure)
    for balance in state.inventory:
        assert measured[balance.key][1] >= balance.amount


def assert_completed_sequence(state):
    completed = [o.decision.intent.kind for o in state.outcomes if o.strategy_result().complete]
    # Request-sized approvals leave no residual on a clean exit; assert_closure proves any
    # residual a crash left was revoked.
    assert [kind for kind in completed if kind != "revoke_exchange"] == [
        "supply",
        "borrow",
        "swap_exact_input",
        "swap_exact_input",
        "repay",
        "withdraw",
    ]


def drive_external(node, journal, running):
    drive_external_chain(node, journal, running, "arbitrum")


def drive_external_chain(node, journal, running, chain):
    count = gmx_fork.keeper_count(journal.load()) + 1
    gmx_fork.drive_external(
        node,
        journal,
        running,
        lambda request: gmx_position_path.source_for(request, chain),
        block_interval=0,
        timeout=request_timeout,
    )
    state = wait_state(
        journal,
        lambda state: (
            gmx_fork.keeper_count(state) == count and state.delivered == len(state.outcomes)
        ),
        running,
        timeout=request_timeout,
    )
    checks = gmx_swap_permissions.assert_permissions(
        node, component_state(state, ConnectorId("gmx_v2"))
    )
    if checks is not None:
        path = journal.path.parent / "external-execution.json"
        values = json.loads(path.read_text())
        values[-1]["installed_permission_negative_checks"] = checks
        path.write_text(json.dumps(values, indent=2) + "\n")
