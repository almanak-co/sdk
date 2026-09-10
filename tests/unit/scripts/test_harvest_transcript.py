"""Native tool provenance must retain every call or refuse incomplete input."""

import json
import sys

import pytest

from qa_lab.harvest_transcript import harvest, main, render


def source(tmp_path, events):
    path = tmp_path / "native.jsonl"
    meta = {"type": "session_meta", "payload": {"id": "session", "source": "cli"}}
    path.write_bytes(b"".join((json.dumps(event) + "\n").encode() for event in [meta, *events]))
    return path


def call(kind="custom_tool_call", **overrides):
    payload = {"type": kind, "name": "functions.exec", "call_id": "call-1", "input": "x" * 8000}
    payload.update(overrides)
    return {"type": "response_item", "timestamp": "2026-09-10T09:00:00Z", "payload": payload}


def test_full_inputs_and_arbitrary_tool_names_retained(tmp_path):
    events = [call(), call("function_call", call_id="call-2", name="new_tool", arguments='{"x":1}')]
    path = source(tmp_path, events)
    result = harvest(path, start=0, end=path.stat().st_size, expected_session="session")
    assert result["tool_call_count"] == 2
    assert result["calls"][0]["input"] == "x" * 8000
    assert "x" * 8000 in render(result)
    assert result["calls"][1]["input"] == '{"x":1}'
    assert result["purity"] == "UNMEASURED"
    assert result["records"][0]["start"] == 0
    assert result["records"][-1]["end"] == path.stat().st_size


@pytest.mark.parametrize("start_delta,end_delta", [(1, 0), (0, -1), (0, 1)])
def test_partial_or_missing_records_refused(tmp_path, start_delta, end_delta):
    path = source(tmp_path, [call()])
    with pytest.raises(ValueError):
        harvest(path, start=start_delta, end=path.stat().st_size + end_delta, expected_session="session")


@pytest.mark.parametrize("events", [[call(), call()], [call("future_tool_call")], [call(call_id=None)], []])
def test_unknown_duplicate_or_empty_capture_refused(tmp_path, events):
    path = source(tmp_path, events)
    with pytest.raises(ValueError):
        harvest(path, start=0, end=path.stat().st_size, expected_session="session")


def test_other_session_cannot_supply_provenance(tmp_path):
    path = source(tmp_path, [call()])
    with pytest.raises(ValueError, match="identity mismatch"):
        harvest(path, start=0, end=path.stat().st_size, expected_session="other")


def test_claude_tool_blocks_are_not_discarded(tmp_path):
    event = {
        "message": {
            "content": [{"type": "tool_use", "name": "Bash", "id": "b", "input": {"command": "uv run almanak"}}]
        }
    }
    path = source(tmp_path, [event])
    result = harvest(path, start=0, end=path.stat().st_size, expected_session="session")
    assert result["calls"][0]["input"] == {"command": "uv run almanak"}


@pytest.mark.parametrize("suffix", [".json", ".md", ""])
def test_cli_writes_distinct_complete_exports(tmp_path, monkeypatch, suffix):
    path = source(tmp_path, [call()])
    output = tmp_path / ("transcript" + suffix)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "harvest",
            "--source",
            str(path),
            "--session-id",
            "session",
            "--start",
            "0",
            "--end",
            str(path.stat().st_size),
            "--output",
            str(output),
        ],
    )
    main()
    result = json.loads(output.with_suffix(".json").read_text())
    assert result["tool_call_count"] == 1
    assert result["calls"][0]["input"] == "x" * 8000
    assert output.with_suffix(".md").read_text() == render(result)


@pytest.mark.parametrize("suffix", [".json", ".md"])
@pytest.mark.parametrize("symlink", [False, True])
def test_cli_existing_target_refuses_without_partial_export(tmp_path, monkeypatch, suffix, symlink):
    path = source(tmp_path, [call()])
    output = tmp_path / "transcript.json"
    occupied = output.with_suffix(suffix)
    if symlink:
        occupied.symlink_to(tmp_path / "missing")
    else:
        occupied.write_text("preserve")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "harvest",
            "--source",
            str(path),
            "--session-id",
            "session",
            "--start",
            "0",
            "--end",
            str(path.stat().st_size),
            "--output",
            str(output),
        ],
    )
    with pytest.raises(SystemExit) as refused:
        main()
    assert refused.value.code == 2
    assert not output.with_suffix(".md" if suffix == ".json" else ".json").exists()
    assert occupied.is_symlink() if symlink else occupied.read_text() == "preserve"
