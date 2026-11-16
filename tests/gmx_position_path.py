"""One request at a time, with report eligibility retained across a multi-step position path."""

import gzip
import json

from tests import gmx_fork
from tests.fork.support import wait_state
from tests.retained_fixtures import retained

ROOT = retained("2026-09-08-gmx-position-paths")


def entered(state):
    return gmx_fork.requests_settled(state) and delivered_count(state, 3)


def delivered_count(state, count):
    return gmx_fork.keeper_count(state) == count and state.delivered == len(state.outcomes)


def assert_holding(node, state):
    assert entered(state)
    assert all(not outcome.accounting_issues for outcome in state.outcomes)
    assert request_types(state) == (2, 2, 4)
    assert_remaining_position(state)


def assert_remaining_position(state):
    for owned in state.owned:
        allocation = state.config.allocation(owned.target.allocation)
        assert owned.position is not None
        assert owned.position.size_usd_e30 == allocation.entry_size_usd_e30


def assert_closed(node, state):
    assert state.mode == "closed" and delivered_count(state, 4)
    gmx_fork.assert_final_effects(state)
    assert gmx_fork.requests_settled(state)
    assert request_types(state) == (2, 2, 4, 4)


def request_types(state):
    return tuple(
        outcome.operation.order_type
        for outcome in state.outcomes
        if outcome.kind == "gmx_request_outcome"
    )


def drive_external(node, journal, running, source_for):
    current = journal.load()
    target = 4 if current.mode == "teardown" else 3
    for count in range(gmx_fork.keeper_count(current) + 1, target + 1):
        gmx_fork.drive_external(node, journal, running, source_for, block_interval=10)
        wait_state(journal, lambda state, count=count: delivered_count(state, count), running)


def source_for(request, chain):
    proof = json.loads(gzip.decompress((ROOT / (chain + "-signed-reports.json.gz")).read_bytes()))
    timestamp = request.events[0].identity.block.timestamp
    adjustments = {b["asset"]: b["timestamp_adjustment"] for b in proof["bindings"]}
    return next(
        row
        for row in proof["selected"]
        if eligible(row, adjustments, timestamp, proof["request_expiration_seconds"])
    )


def eligible(row, adjustments, timestamp, expiration):
    times = [report["observed_at"] - adjustments[report["asset"]] for report in row["reports"]]
    return min(times) > timestamp and max(times) <= timestamp + expiration
