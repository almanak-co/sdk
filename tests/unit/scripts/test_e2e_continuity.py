"""Actual SDK summary fields with synthetic hold clocks; no sustained-run claim."""

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
import structlog.testing

from almanak.framework.intents import Intent
from almanak.framework.runner.runner_models import IterationResult, IterationStatus
from almanak.framework.runner.runner_state import emit_iteration_summary
from qa_lab.e2e_continuity import EXPECTED_HOLD, decode_event, recorded_hold_iterations

IDENTITY = "deployment:4a9f5de1c786"
START = datetime(2026, 9, 7, tzinfo=UTC)


@pytest.fixture
def stream(tmp_path, monkeypatch):
    records = []
    for number in range(1, 5):
        runner = SimpleNamespace(config=SimpleNamespace(dry_run=False), _total_iterations=number)
        result = IterationResult(
            status=IterationStatus.HOLD,
            intent=Intent.hold(reason=EXPECTED_HOLD),
            deployment_id=IDENTITY,
            duration_ms=50.0,
        )
        with structlog.testing.capture_logs() as emitted:
            monkeypatch.setattr(
                "almanak.framework.runner.runner_state.logger",
                structlog.get_logger("almanak.framework.runner.strategy_runner"),
            )
            emit_iteration_summary(runner, result, chain="arbitrum")
        event = next(event for event in emitted if event.get("event_type") == "iteration_summary")
        records.append(
            {
                **event,
                "logger": "almanak.framework.runner.strategy_runner",
                "timestamp": (START + timedelta(seconds=number * 60)).isoformat(),
            }
        )
    return tmp_path / "runner-events.jsonl", records


def evaluate(stream):
    path, records = stream
    path.write_text("".join(json.dumps(record) + "\n" for record in records))
    return recorded_hold_iterations(
        path,
        deployment_id=IDENTITY,
        start=START.isoformat(),
        end=(START + timedelta(seconds=300)).isoformat(),
        maximum_gap_seconds=90,
    )


def test_expected_sdk_hold_summaries_cover_the_measured_interval(stream):
    result = evaluate(stream)
    assert result["status"] == "PASS"
    assert result["scope"] == "recorded_hold_decisions"
    assert result["iterations"] == 4
    assert result["maximum_observed_gap_seconds"] == 60


@pytest.mark.parametrize(
    "mutation",
    ["drop", "restart", "stall", "unexpected_hold", "error", "dry_run", "sent", "naive", "slow", "missing_error"],
)
def test_recorded_hold_contradictions_cannot_certify_continuity(stream, mutation):
    _, records = stream
    record = records[1]
    if mutation == "drop":
        records.pop(1)
    elif mutation == "restart":
        record["iteration"] = 1
    elif mutation == "stall":
        records[:] = records[:2]
    elif mutation == "unexpected_hold":
        record["hold_reason"] = "market data unavailable"
    elif mutation == "error":
        record["error"] = "failed persistence"
    elif mutation == "dry_run":
        record["dry_run"] = True
    elif mutation == "sent":
        record["txs_sent"] = 1
    elif mutation == "naive":
        record["timestamp"] = record["timestamp"].replace("+00:00", "")
    elif mutation == "slow":
        record["duration_ms"] = 100000
    else:
        del record["error"]
    with pytest.raises(ValueError):
        evaluate(stream)


@pytest.mark.parametrize("line", ['{"status":"FAIL","status":"PASS"}', '{"duration_ms":NaN}', "[]"])
def test_ambiguous_json_cannot_be_normalized_into_a_valid_record(line):
    with pytest.raises(ValueError):
        decode_event(line)


def test_continuity_obligation_requires_admitted_hold_and_owned_stream(stream):
    from qa_lab.e2e_card import canonical
    from qa_lab.e2e_continuity import validate_runner_continuity

    evaluate(stream)
    path, _ = stream
    bundle = path.parent
    contract = {"runner_continuity": {"schema_version": 1, "maximum_gap_seconds": 90}}
    state = {"deployment_id": IDENTITY}
    with pytest.raises(ValueError, match="independently admitted"):
        validate_runner_continuity(bundle, contract, state, hold=None)
    samples = bundle / "hold-observations"
    samples.mkdir()
    (samples / "000000-sample.json").write_bytes(canonical({"finished_at": START.isoformat()}))
    (samples / "000001-sample.json").write_bytes(
        canonical({"started_at": (START + timedelta(seconds=300)).isoformat()})
    )
    result = validate_runner_continuity(bundle, contract, state, hold={"status": "PASS"})
    assert result["iterations"] == 4
    assert len(result["source_artifacts"]) == 3
    path.unlink()
    with pytest.raises(ValueError, match="unmeasured"):
        validate_runner_continuity(bundle, contract, state, hold={"status": "PASS"})
