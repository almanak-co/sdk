"""Fixture timestamps keep each independent collateral request eligible for authentic reports."""

import gzip
import json

from tests import gmx_collateral_permissions, gmx_fork, gmx_position_path
from tests.fork.support import wait_state


def entered(state):
    return gmx_fork.requests_settled(state) and gmx_position_path.delivered_count(state, 5)


def assert_holding(node, state):
    assert entered(state)
    assert all(not outcome.accounting_issues for outcome in state.outcomes)
    assert gmx_position_path.request_types(state) == (2, 2, 2, 4, 4)
    gmx_position_path.assert_remaining_position(state)
    assert_collateral_changes(state)


def assert_collateral_changes(state):
    rows = [o for o in state.outcomes if o.kind == "gmx_request_outcome"]
    added, withdrawn = rows[2:4]
    for outcome in (added, withdrawn):
        assert outcome.operation.size_delta_usd_e30 == 0
        assert outcome.operation.collateral_delta.amount > 0
        assert outcome.before.position.size_usd_e30 == outcome.after.position.size_usd_e30
    assert added.after.position.collateral.amount > added.before.position.collateral.amount
    assert withdrawn.after.position.collateral.amount < withdrawn.before.position.collateral.amount
    assert withdrawn.assets[0].amount > 0


def assert_closed(node, state):
    assert state.mode == "closed" and gmx_position_path.delivered_count(state, 6)
    gmx_fork.assert_final_effects(state)
    assert gmx_fork.requests_settled(state)
    assert gmx_position_path.request_types(state) == (2, 2, 2, 4, 4, 4)
    assert_collateral_changes(state)


def drive_external(node, journal, running, chain):
    current = journal.load()
    target = 6 if current.mode == "teardown" else 5
    for count in range(gmx_fork.keeper_count(current) + 1, target + 1):
        if count > 1 and target == 5:
            advance_for_report(node, journal, chain)
        gmx_fork.drive_external(
            node, journal, running, lambda request: gmx_position_path.source_for(request, chain)
        )
        wait_state(
            journal,
            lambda state, count=count: gmx_position_path.delivered_count(state, count),
            running,
        )
    retain_permissions(node, journal)


def advance_for_report(node, journal, chain):
    proof = json.loads(
        gzip.decompress((gmx_position_path.ROOT / (chain + "-signed-reports.json.gz")).read_bytes())
    )
    current = int(node.rpc("eth_getBlockByNumber", ["latest", False])["timestamp"], 16)
    timestamp = next_fixture_timestamp(proof, current)
    node.rpc("evm_setNextBlockTimestamp", [timestamp])
    node.rpc("evm_mine", [])
    update_last_fixture(
        journal,
        {
            "next_request_fixture_timestamp": timestamp,
            "next_request_timestamp_reason": (
                "New chain observation before the next authentic report, "
                "followed by approval and creation blocks"
            ),
        },
    )


def next_fixture_timestamp(proof, current):
    adjustments = {b["asset"]: b["timestamp_adjustment"] for b in proof["bindings"]}
    times = [
        min(r["observed_at"] - adjustments[r["asset"]] for r in row["reports"])
        for row in proof["selected"]
    ]
    return next(value - 3 for value in times if value - 3 > current)


def retain_permissions(node, journal):
    result = gmx_collateral_permissions.assert_permissions(node, journal.load())
    if result is not None:
        update_last_fixture(journal, {"installed_permission_negative_checks": result})


def update_last_fixture(journal, values):
    path = journal.path.parent / "external-execution.json"
    retained = json.loads(path.read_text())
    retained[-1].update(values)
    path.write_text(json.dumps(retained, indent=2) + "\n")
