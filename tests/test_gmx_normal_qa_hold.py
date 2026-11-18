"""Normal QA tolerates append-only empty scans and waits for the complete entry transition."""

from types import SimpleNamespace

import pytest

from almanak.connectors.gmx_v2.liquidation_types import Scan, scope
from almanak.connectors.identifiers import ConnectorId
from tests.gmx_evidence import holding
from tests.gmx_fork import entered, requests_settled
from tests.strategy_fork.harness import validate_hold_scans


def empty_scan(state):
    observed = state.outcomes[-1].after
    return Scan(
        connector=ConnectorId("gmx_v2"),
        scope=scope(observed.target),
        target=observed.target,
        start=observed.block,
        end=observed.block,
        events=(),
    )


def test_settled_request_is_not_entry_until_decision_and_memory_commit():
    state = holding()
    assert entered(state)
    assert not entered(state.model_copy(update={"decision": state.outcomes[-1].decision}))
    assert not entered(
        state.model_copy(update={"memory": state.memory.model_copy(update={"values": {}})})
    )
    assert not entered(state.model_copy(update={"delivered": state.delivered - 1}))


def test_hold_accepts_only_append_only_empty_pages():
    state = holding()
    scan = empty_scan(state)
    previous = SimpleNamespace(external_scans=())
    following = SimpleNamespace(external_scans=(scan,))
    validate_hold_scans(previous, following)
    validate_hold_scans(following, following)
    with pytest.raises(AssertionError, match="retained external scan history"):
        validate_hold_scans(following, previous)
    altered = scan.model_copy(update={"events": (object(),)})
    with pytest.raises(AssertionError, match="retained external scan history"):
        validate_hold_scans(following, SimpleNamespace(external_scans=(altered,)))
    with pytest.raises(AssertionError, match="economic outcome"):
        validate_hold_scans(previous, SimpleNamespace(external_scans=(object(),)))
    with pytest.raises(AssertionError, match="economic outcome"):
        validate_hold_scans(
            previous,
            SimpleNamespace(external_scans=(scan.model_copy(update={"outcome": object()}),)),
        )


def test_all_reference_predicates_wait_for_the_complete_request_transition():
    from almanak.connectors.gmx_v2.state import Work

    state = holding()
    decision = state.outcomes[-1].decision
    assert requests_settled(state)
    assert not requests_settled(state.model_copy(update={"decision": decision}))
    assert not requests_settled(state.model_copy(update={"delivered": state.delivered - 1}))
    work = Work(chain=state.owned[0].target.fund.chain, decision=decision)
    assert not requests_settled(state.model_copy(update={"work": (work,)}))
