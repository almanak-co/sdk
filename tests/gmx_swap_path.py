"""The reusable swap reference owns real keeper-settled output before its return request."""

import json

from tests import gmx_fork, gmx_position_path, gmx_swap_permissions
from tests.fork.support import wait_state


def entered(state):
    return (
        gmx_fork.requests_settled(state)
        and gmx_fork.keeper_count(state) == 1
        and state.delivered == len(state.outcomes)
    )


def assert_holding(node, state):
    assert entered(state)
    assert len(state.requests) == 1
    assert_swap_history(state)
    assert_bookkeeping(state)
    assert_owned_output(state)


def assert_swap_history(state):
    assert all(request.status == "executed" for request in state.requests)
    assert all(
        request.events[0].event().value("uint256", "orderType") == 0 for request in state.requests
    )


def assert_bookkeeping(state):
    assert all(not outcome.accounting_issues for outcome in state.outcomes)
    assert all(owned.market_tokens.amount == 0 for owned in state.owned)


def assert_owned_output(state):
    for owned in state.owned:
        allocation = state.config.allocation(owned.target.allocation)
        proceeds = next(
            value
            for value in owned.inventory
            if value.asset == allocation.entry_minimum_output.asset
        )
        assert proceeds.amount >= allocation.entry_minimum_output.amount


def assert_closed(node, state):
    assert state.mode == "closed" and state.delivered == len(state.outcomes)
    assert all(not snapshot.needs_cleanup for snapshot in state.closure)
    assert_closed_requests(state)
    assert_bookkeeping(state)
    assert_swap_history(state)
    assert_returned_inventory(state)
    assert_revocation(state)


def assert_closed_requests(state):
    assert gmx_fork.requests_settled(state) and gmx_fork.keeper_count(state) == 2


def assert_returned_inventory(state):
    for owned in state.owned:
        assert_no_output(owned)


def assert_no_output(owned):
    assert all(
        value.amount == 0
        for value in owned.inventory
        if value.asset in owned.target.assets and value.asset != owned.target.funding_asset
    )


def assert_revocation(state):
    """Request-sized approvals leave no allowance, so a closure revokes only a residual one.

    A revocation's strategy result completes only with the last revocation of its exit.
    """
    assert all(not snapshot.needs_cleanup for snapshot in state.closure)
    revocations = [o for o in state.outcomes if o.decision.intent.kind == "revoke_exchange"]
    assert not revocations or revocations[-1].strategy_result().complete


def drive_external(node, journal, running, chain):
    count = gmx_fork.keeper_count(journal.load()) + 1
    gmx_fork.drive_external(
        node, journal, running, lambda request: gmx_position_path.source_for(request, chain)
    )
    settled = wait_state(
        journal,
        lambda state: (
            gmx_fork.keeper_count(state) == count and state.delivered == len(state.outcomes)
        ),
        running,
    )
    permissions = gmx_swap_permissions.assert_permissions(node, settled)
    if permissions is not None:
        path = journal.path.parent / "external-execution.json"
        values = json.loads(path.read_text())
        values[-1]["installed_permission_negative_checks"] = permissions
        path.write_text(json.dumps(values, indent=2) + "\n")
