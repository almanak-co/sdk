"""The Avalanche GM teardown starts where a retained signed report can settle its close request."""

import pytest

from tests.fork import test_gmx_liquidity_recovery as recovery
from tests.gmx_position_path import eligible


class Node:
    def __init__(self, timestamp):
        self.timestamp, self.calls = timestamp, []

    def rpc(self, method, params=None):
        self.calls.append((method, params))
        return {"timestamp": hex(self.timestamp)}


def eligible_times(timestamp):
    """Report times of the retained rows that can settle a request created at `timestamp`."""
    original, rows = recovery.reports("avalanche")
    adjustments = {b["asset"]: b["timestamp_adjustment"] for b in original["bindings"]}
    expiration = original["request_expiration_seconds"]
    times = recovery.report_times("avalanche")
    return [
        time
        for time, row in zip(times, rows, strict=True)
        if eligible(row, adjustments, timestamp, expiration)
    ]


def test_retained_avalanche_reports_leave_the_failed_close_request_unsettled():
    # The close withdrawal of the failed 19 October run was created at 1788828403.
    assert eligible_times(1788828403) == []


def test_alignment_moves_the_fork_clock_before_a_retained_report():
    node = Node(1788828358)

    recovery.align_teardown(node, "avalanche")

    (_, (method, [aligned]), mine) = node.calls
    assert method == "evm_setNextBlockTimestamp" and mine == ("evm_mine", None)
    selected = 1788829185
    assert aligned == selected - recovery.TEARDOWN_LEAD
    for blocks in range(recovery.TEARDOWN_BLOCKS + 1):
        assert selected in eligible_times(aligned + 15 * blocks)


def test_alignment_refuses_a_fork_clock_past_every_retained_report():
    node = Node(max(recovery.report_times("avalanche")))

    with pytest.raises(AssertionError, match="leaves no retained avalanche report"):
        recovery.align_teardown(node, "avalanche")
    assert node.calls == [("eth_getBlockByNumber", ["latest", False])]


def test_late_avalanche_reports_follow_the_reference_rows():
    times = recovery.report_times("avalanche")
    assert times == sorted(times) and len(times) == 13
    assert times[6:] == [
        1788829429,
        1788829756,
        1788829898,
        1788829938,
        1788830137,
        1788830193,
        1788830239,
    ]
