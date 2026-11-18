"""The reusable GM reference owns fungible shares and both actual withdrawal currencies."""

import json

from tests import gmx_fork, gmx_liquidity_permissions, gmx_position_path, gmx_swap_path
from tests.fork.support import wait_state


def entered(state):
    return gmx_fork.requests_settled(state) and gmx_position_path.delivered_count(state, 3)


def assert_holding(node, state):
    assert entered(state)
    assert_request_history(state, ("deposit", "deposit", "withdrawal"))
    assert_accounting(state)
    assert_owned_holding(state)


def assert_request_history(state, kinds):
    assert tuple(request.kind for request in state.requests) == kinds
    assert all(request.status == "executed" for request in state.requests)


def assert_accounting(state):
    assert all(not outcome.accounting_issues for outcome in state.outcomes)


def assert_owned_holding(state):
    assert all(owned.market_tokens.amount > 0 for owned in state.owned)
    assert all(
        state.memory.values["settled"][owned.target.allocation] == 3 for owned in state.owned
    )


def assert_closed(node, state):
    assert state.mode == "closed" and gmx_position_path.delivered_count(state, 4)
    assert_request_history(state, ("deposit", "deposit", "withdrawal", "withdrawal"))
    assert_accounting(state)
    assert all(owned.market_tokens.amount == 0 for owned in state.owned)
    assert_cleanup(state)
    assert_remaining_inventory(state)


def assert_cleanup(state):
    gmx_swap_path.assert_revocation(state)


def assert_remaining_inventory(state):
    for owned in state.owned:
        assert all(
            value.amount > 0 for value in owned.inventory if value.asset in owned.target.assets
        )


def drive_external(node, journal, running, chain, *, source_for=None):
    selected_source = source_for or (lambda request: gmx_position_path.source_for(request, chain))
    target = 4 if journal.load().mode == "teardown" else 3
    settled = journal.load()
    for count in range(gmx_fork.keeper_count(journal.load()) + 1, target + 1):
        gmx_fork.drive_external(
            node,
            journal,
            running,
            selected_source,
            block_interval=15 if chain == "avalanche" else 10,
        )
        settled = wait_state(
            journal,
            lambda state, count=count: gmx_position_path.delivered_count(state, count),
            running,
        )
    retain_permission_checks(node, journal, settled)


def retain_permission_checks(node, journal, settled):
    permissions = gmx_liquidity_permissions.assert_permissions(node, settled)
    if permissions is not None:
        path = journal.path.parent / "external-execution.json"
        values = json.loads(path.read_text())
        values[-1]["installed_permission_negative_checks"] = permissions
        path.write_text(json.dumps(values, indent=2) + "\n")
