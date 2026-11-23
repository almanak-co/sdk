"""Composed checkpoints decode each record as its allocation's connector type."""

import json
from pathlib import Path
from typing import get_args

import pytest
from pydantic import TypeAdapter, ValidationError

from almanak.connectors.composed import state
from almanak.connectors.composed.codec import codec
from almanak.connectors.composition import installed_connectors
from almanak.connectors.support import RunMode
from almanak.connectors.target_routing import target_connector
from tests.curve_retired import current_json as curve_current_json
from tests.lending_captures import read_capture
from tests.uniswap_support import scope_only_json

FIXTURES = Path(__file__).with_name("fixtures")
CAPTURES = (
    "composed-v4-closed-fork.json.gz",
    "composed-base-safe-lifecycle.json.gz",
    "composed-multichain-safe-lifecycle.json.gz",
    "spark-composed-before-repay.json.gz",
)


def capture(name):
    # Captures predate scope-only authority (blueprint 27): their lending, exchange and
    # Curve budgets are read in today's interface; every other fact stays as captured.
    return curve_current_json(scope_only_json(read_capture(FIXTURES / name)))


def live(connector) -> bool:
    return any(set(s.modes) - {RunMode.BACKTEST} for s in connector.chain_support)


def test_every_connector_routes_to_members_of_the_retained_unions():
    # A Hyperliquid deployment trades through one Safe of its own and joins no composition,
    # and an Across deployment runs alone; a backtest-only connector keeps no live
    # checkpoint records.
    live_ids = {str(name) for name, c in installed_connectors() if live(c)}
    assert set(state.RECORDS) == live_ids - {"hyperliquid", "across"}
    for records in state.RECORDS.values():
        for record, union in zip(records, state.UNIONS, strict=True):
            assert record in get_args(union)


@pytest.mark.parametrize("name", CAPTURES)
def test_routed_records_match_the_full_union_decode(name):
    raw = capture(name)
    decoded = state.Checkpoint.model_validate_json(raw)
    wire = json.loads(raw)
    union = TypeAdapter(state.Outcome)
    assert decoded.outcomes
    for record, outcome in zip(wire["outcomes"], decoded.outcomes, strict=True):
        expected = union.validate_json(json.dumps(record))
        assert type(outcome) is type(expected) and outcome == expected
        target = outcome.decision.intent.target
        assert target_connector(target) == target_connector(
            decoded.config.target(target.allocation)
        )
    assert state.Checkpoint.model_validate_json(decoded.model_dump_json()) == decoded


def spy(monkeypatch):
    requested = []
    original = state.record_adapter

    def record_adapter(record_type):
        requested.append(record_type)
        return original(record_type)

    monkeypatch.setattr(state, "record_adapter", record_adapter)
    return requested


def test_routable_records_never_try_the_full_union(monkeypatch):
    raw = capture("composed-v4-closed-fork.json.gz")
    requested = spy(monkeypatch)
    decoded = state.Checkpoint.model_validate_json(raw)
    assert len(requested) == len(decoded.outcomes)
    assert not set(requested) & set(state.UNIONS)


def test_active_decision_plan_and_pending_are_routed(monkeypatch):
    decoded = state.Checkpoint.model_validate_json(capture("composed-v4-closed-fork.json.gz"))
    outcome = decoded.outcomes[-1]
    binding = codec()
    plan = binding.plan(
        **{f: getattr(outcome, f) for f in ("decision", "operation", "before", "call")},
        attempt_id="a" * 32,
        execution=outcome.execution,
        nonce=outcome.nonce,
        gas_estimate=1,
        gas=1,
        gas_price=1,
    )
    pending = binding.pending(
        **{f: getattr(plan, f) for f in type(plan).model_fields},
        raw_transaction="0x12",
        tx_hash="0x" + "12" * 32,
    )
    earlier = decoded.model_copy(update={"outcomes": decoded.outcomes[:-1], "delivered": 0})
    for update in (
        {"decision": outcome.decision},
        {"decision": outcome.decision, "plan": plan},
        {"decision": outcome.decision, "pending": pending},
    ):
        active = earlier.model_copy(update=update)
        body = active.model_dump_json()
        requested = spy(monkeypatch)
        restored = state.Checkpoint.model_validate_json(body)
        monkeypatch.undo()
        assert restored == active
        assert len(requested) == len(earlier.outcomes) + len(update)
        assert not set(requested) & set(state.UNIONS)
        for field, value in update.items():
            assert type(getattr(restored, field)) is type(value)


@pytest.mark.parametrize(
    "tamper",
    [
        lambda record: record["decision"]["intent"]["target"].update(allocation="unknown"),
        lambda record: record["decision"]["intent"].pop("target"),
        lambda record: record.update(nonce="not a nonce"),
    ],
)
def test_unroutable_or_invalid_records_keep_the_union_refusal(tamper):
    wire = json.loads(capture("composed-v4-closed-fork.json.gz"))
    tamper(wire["outcomes"][0])
    with pytest.raises(ValidationError):
        state.Checkpoint.model_validate_json(json.dumps(wire))
    with pytest.raises(ValidationError):
        TypeAdapter(state.Outcome).validate_json(json.dumps(wire["outcomes"][0]))


def test_composed_codec_is_built_once():
    assert codec() is codec()


def rewound_work():
    """The two-chain capture with each chain's final withdrawal active and pending again."""
    closed = state.Checkpoint.model_validate_json(
        capture("composed-multichain-safe-lifecycle.json.gz")
    )
    final = {o.decision.intent.target.fund.chain: o for o in closed.outcomes[-2:]}
    work = []
    for item in closed.work:
        outcome = final[item.chain]
        pending = codec().pending(
            **{f: getattr(outcome, f) for f in ("decision", "operation", "before", "call")},
            attempt_id="a" * 32,
            execution=outcome.execution,
            nonce=outcome.nonce,
            gas_estimate=1,
            gas=1,
            gas_price=1,
            raw_transaction="0x12",
            tx_hash="0x" + "12" * 32,
        )
        work.append(
            item.model_copy(
                update={"closed": False, "decision": outcome.decision, "pending": pending}
            )
        )
    return closed.model_copy(
        update={
            "mode": "running",
            "work": tuple(work),
            "outcomes": closed.outcomes[:-2],
            "delivered": len(closed.outcomes) - 2,
            "closure": (),
        }
    )


def test_chain_work_records_are_routed_by_their_own_target(monkeypatch):
    active = rewound_work()
    assert len({w.chain for w in active.work if w.decision and w.pending}) == 2
    body = active.model_dump_json()
    requested = spy(monkeypatch)
    restored = state.Checkpoint.model_validate_json(body)
    monkeypatch.undo()
    assert restored == active
    assert not set(requested) & set(state.UNIONS)
    decision, _, pending, _ = state.RECORDS["aave_v3"]
    assert requested.count(decision) == requested.count(pending) == 2
    union = TypeAdapter(state.Work)
    for item, decoded in zip(json.loads(body)["work"], restored.work, strict=True):
        expected = union.validate_json(json.dumps(item))
        assert decoded == expected
        assert decoded.model_fields_set == expected.model_fields_set
        for field in ("decision", "plan", "pending"):
            assert type(getattr(decoded, field)) is type(getattr(expected, field))


@pytest.mark.parametrize(
    "tamper",
    [
        lambda work: work["decision"]["intent"].pop("target"),
        lambda work: work["pending"]["decision"]["intent"]["target"]["reserve"].update(
            kind="unknown"
        ),
        lambda work: work["pending"].update(nonce="not a nonce"),
        lambda work: work.update(unexpected=True),
    ],
)
def test_unroutable_or_invalid_chain_work_keeps_the_union_refusal(tamper):
    wire = json.loads(rewound_work().model_dump_json())
    tamper(wire["work"][0])
    with pytest.raises(ValidationError):
        state.Checkpoint.model_validate_json(json.dumps(wire))
    with pytest.raises(ValidationError):
        TypeAdapter(state.Work).validate_json(json.dumps(wire["work"][0]))
