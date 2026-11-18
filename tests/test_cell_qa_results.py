"""Hosted cell QA evidence is scanned before upload, and only a clean single-source merge passes."""

import gzip
import json
from types import SimpleNamespace

import pytest

from almanak.caip import ChainId
from scripts import qa_bench
from scripts.ci import cell_qa_evidence, cell_qa_summary
from scripts.ci.cell_qa_evidence import leaked, secret_values
from scripts.ci.cell_qa_matrix import plan
from scripts.ci.cell_qa_summary import aggregate, cell_ledger, summary
from tests.qa.inputs import freeze_inputs, frozen_origins

KEY = "alchemy-secret-value"
SOURCE = {
    "commit": "c0ffee",
    "tree_sha256": "tree",
    "lock_sha256": "lock",
    "python": "3.12.13",
    "tools": {"anvil": "anvil 1"},
}
FROZEN = {"input_time": 1790000000, "resolved": {"eip155:56": {"number": 7}}, "failures": {}}
CASE = "fork/pinned/uniswap_v3_bnb_lp/eoa/configured"
NODE = "tests/fork/test_x.py::test_y[bnb_aave_borrow_and_trade_v3]"


def test_secret_values_come_from_named_environment_variables_only():
    values = secret_values(["A", "B", "C"], {"A": KEY, "B": "short", "C": "", "D": "other-long"})
    assert values == (KEY.encode(),)
    assert set(secret_values(["A"], {"A": "key/with space"})) == {
        b"key/with space",
        b"key%2Fwith%20space",
    }


def test_scan_finds_keys_in_bytes_gzip_members_names_and_links(tmp_path):
    secrets = (KEY.encode(),)
    (tmp_path / "clean.json").write_text("{}")
    (tmp_path / "raw.log").write_text(f"https://rpc/v2/{KEY}")
    (tmp_path / "source.tar.gz").write_bytes(gzip.compress(KEY.encode()))
    (tmp_path / f"{KEY}.json").write_text("{}")
    (tmp_path / "link").symlink_to(tmp_path / "clean.json")
    assert leaked(tmp_path, secrets) == sorted(["raw.log", "source.tar.gz", f"{KEY}.json", "link"])
    with pytest.raises(ValueError, match="vacuous"):
        leaked(tmp_path, ())
    with pytest.raises(ValueError, match="not a directory"):
        leaked(tmp_path / "missing", secrets)


def test_scan_command_reports_paths_and_exit_status(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("CELL_QA_KEY", KEY)
    arguments = ["scan", "--directory", str(tmp_path), "--secret-env", "CELL_QA_KEY"]
    (tmp_path / "clean.json").write_text("{}")
    assert cell_qa_evidence.main(arguments) == 0
    (tmp_path / "bad.json").write_text(KEY)
    assert cell_qa_evidence.main(arguments) == 1
    error = capsys.readouterr().err
    assert "bad.json" in error and KEY not in error


def test_node_export_uses_the_bench_evidence_boundary(tmp_path):
    env = tmp_path / "provider.env"
    env.write_text(f"ALCHEMY_API_KEY={KEY}\n")
    source = tmp_path / "artifacts"
    (source / "case").mkdir(parents=True)
    (source / "case" / "pytest.log").write_text(f"endpoint {KEY}")
    (source / "case" / "gateway-auth.token").write_text("token-value")
    (source / "case" / "checkpoint.json").write_text("{}")
    (source / "case" / "leak.json").write_text(KEY)
    omitted = tmp_path / "omitted.json"
    omitted.write_text("[]")
    arguments = ["export", "--source", str(source), "--destination", str(tmp_path / "public")]
    arguments += ["--env-file", str(env), "--omitted", str(omitted)]
    assert cell_qa_evidence.main(arguments) == 0
    public = tmp_path / "public" / "case"
    assert KEY not in (public / "pytest.log").read_text()
    assert (public / "checkpoint.json").is_file() and not (public / "gateway-auth.token").exists()
    assert json.loads(omitted.read_text()) == ["case/leak.json"]
    arguments[2] = str(tmp_path / "absent")
    assert cell_qa_evidence.main(arguments) == 0
    assert json.loads(omitted.read_text()) == ["case/leak.json"]


def test_identity_and_record_commands_write_the_job_record(tmp_path, monkeypatch):
    monkeypatch.setattr(cell_qa_evidence, "bench_identity", lambda: {**SOURCE, "files": {"a": "b"}})
    monkeypatch.setattr(cell_qa_evidence, "tool_versions", lambda: SOURCE["tools"])
    before = tmp_path / "before.json"
    assert cell_qa_evidence.main(["identity", "--output", str(before)]) == 0
    assert json.loads(before.read_text()) == SOURCE
    output = tmp_path / "job.json"
    arguments = ["record", "--entry", json.dumps({"id": CASE}), "--outcome", "success"]
    arguments += ["--scan", "passed", "--before", str(before), "--output", str(output)]
    assert cell_qa_evidence.main(arguments) == 0
    job = json.loads(output.read_text())
    assert job["identity_before"] == SOURCE and job["identity_after"] is None
    assert job["omitted"] == [] and job["entry"] == {"id": CASE}


def test_frozen_inputs_round_trip_into_bench_origins(tmp_path, monkeypatch):
    from tests.qa import inputs

    monkeypatch.setattr(
        inputs,
        "resolve_origins",
        lambda cases, env, directory: ({ChainId("eip155:56"): {"number": 7}}, {}),
    )
    monkeypatch.setattr(inputs, "utc_now", lambda: 1790000000)
    assert freeze_inputs([], None, tmp_path) == FROZEN
    now, origins, failures = frozen_origins(tmp_path / "frozen-inputs.json", tmp_path / "private")
    assert (now, origins, failures) == (1790000000, {ChainId("eip155:56"): {"number": 7}}, {})
    written = json.loads((tmp_path / "private" / "origins.json").read_text())
    assert written == {"resolved": FROZEN["resolved"], "failures": {}}
    for bad in ({**FROZEN, "extra": 1}, {**FROZEN, "input_time": "now"}, []):
        (tmp_path / "bad.json").write_text(json.dumps(bad))
        with pytest.raises(ValueError, match="frozen input"):
            frozen_origins(tmp_path / "bad.json", tmp_path)


def test_bench_uses_frozen_inputs_instead_of_resolving(tmp_path, monkeypatch):
    frozen = tmp_path / "frozen.json"
    frozen.write_text(json.dumps(FROZEN))
    monkeypatch.setattr(qa_bench, "resolve_origins", lambda *a: pytest.fail("resolved again"))
    args = SimpleNamespace(frozen_inputs=frozen, output=tmp_path, env_file=None)
    metadata = {"input_time": 1}
    origins, failures = qa_bench.run_origins(args, metadata, [])
    assert origins == {ChainId("eip155:56"): {"number": 7}} and failures == {}
    assert metadata["input_time"] == FROZEN["input_time"]
    assert len(metadata["frozen_inputs_sha256"]) == 64
    monkeypatch.setattr(qa_bench, "resolve_origins", lambda *a: ({}, {}))
    args.frozen_inputs = None
    assert qa_bench.run_origins(args, metadata, []) == ({}, {})


def plan_of(entries, cell=None, requested=False, nodes=(), excluded=()):
    rows = [
        {
            "id": entry_id,
            "kind": kind,
            "lane": "faults" if kind == "node" else "fork",
            "reference": "",
            "index": index,
            "artifact": f"cell-qa-entry-{index:03d}",
        }
        for index, (entry_id, kind) in enumerate(entries)
    ]
    return {
        "commit": SOURCE["commit"],
        "cell": cell,
        "lanes": ["fork", "backtest"],
        "reverse_fault": {"requested": requested, "nodes": list(nodes)},
        "entries": rows,
        "excluded": list(excluded),
    }


def write_job(directory, entry, scan="passed", omitted=(), before=SOURCE, after=SOURCE):
    directory.mkdir(parents=True)
    job = {"entry": entry, "scan": scan, "omitted": list(omitted)}
    job.update(identity_before=before, identity_after=after)
    (directory / "job.json").write_text(json.dumps(job))


def write_case(directory, case_id, status="passed", **changes):
    public = directory / "public"
    public.mkdir(parents=True)
    results = {
        "identity": {"commit": SOURCE["commit"], "tree_sha256": SOURCE["tree_sha256"]},
        "selected_cases": [case_id],
        "input_time": FROZEN["input_time"],
        "origins": FROZEN["resolved"],
        "cases": [
            {"id": "other", "status": "skipped", "category": "not_selected", "wall_seconds": None},
            {"id": case_id, "status": status, "category": "", "wall_seconds": 12.5},
        ],
    }
    (public / "results.json").write_text(json.dumps({**results, **changes}))


def write_node(directory, body='<testcase name="n" time="3.25"/>'):
    (directory / "public" / "node").mkdir(parents=True)
    (directory / "public" / "node" / "junit.xml").write_text(f"<testsuite>{body}</testsuite>")


def passing(tmp_path):
    value = plan_of([(CASE, "case"), (NODE, "node")], requested=True, nodes=[NODE])
    first, second = (tmp_path / e["artifact"] for e in value["entries"])
    write_job(first, value["entries"][0])
    write_case(first, CASE)
    write_job(second, value["entries"][1])
    write_node(second)
    return value, first, second


def statuses(value, tmp_path):
    payload = aggregate(value, FROZEN, tmp_path, SOURCE)
    return payload, [(r["status"], r["category"]) for r in payload["cases"]]


def test_every_planned_entry_passing_on_one_source_is_the_only_pass(tmp_path):
    value, _, _ = passing(tmp_path)
    payload, rows = statuses(value, tmp_path)
    assert rows == [("passed", ""), ("passed", "")] and payload["status"] == "passed"
    assert (
        payload["cases"][0]["wall_seconds"] == 12.5 and payload["cases"][1]["wall_seconds"] == 3.25
    )
    assert payload["source"] == {k: SOURCE[k] for k in ("commit", "tree_sha256", "lock_sha256")}
    assert payload["runtime"] == {"python": "3.12.13", "tools": SOURCE["tools"]}
    text = summary(payload)
    assert "Result: **passed**" in text and "explicit selection" in text
    assert "does not establish" in text and "retained evidence" in text


@pytest.mark.parametrize(
    "damage,expected",
    [
        (lambda first, second: (first / "job.json").unlink(), ("failed", "no_result")),
        (lambda first, second: (first / "public/results.json").unlink(), ("failed", "no_result")),
        (lambda first, second: rewrite(first, scan="refused"), ("blocked", "unsafe_evidence")),
        (lambda first, second: rewrite(first, omitted=["a.json"]), ("blocked", "unsafe_evidence")),
        (lambda first, second: rewrite(first, after=None), ("blocked", "identity_missing")),
        (
            lambda first, second: rewrite(first, after={**SOURCE, "tree_sha256": "changed"}),
            ("blocked", "source_changed"),
        ),
        (
            lambda first, second: rewrite(
                first,
                before={**SOURCE, "commit": "other"},
                after={**SOURCE, "commit": "other"},
            ),
            ("blocked", "source_identity_mismatch"),
        ),
        (
            lambda first, second: case_again(first, selected_cases=[CASE, "other"]),
            ("blocked", "selection_mismatch"),
        ),
        (
            lambda first, second: case_again(first, identity={"tree_sha256": "other"}),
            ("blocked", "source_identity_mismatch"),
        ),
        (lambda first, second: case_again(first, input_time=1), ("blocked", "input_mismatch")),
        (lambda first, second: case_again(first, origins={}), ("blocked", "input_mismatch")),
        (lambda first, second: case_again(first, status="failed"), ("failed", "")),
    ],
)
def test_a_damaged_case_entry_keeps_the_cell_incomplete(tmp_path, damage, expected):
    value, first, second = passing(tmp_path)
    damage(first, second)
    payload, rows = statuses(value, tmp_path)
    assert rows[0] == expected and payload["status"] == "incomplete_or_failed"


RECORD_KEYS = {"before": "identity_before", "after": "identity_after"}


def rewrite(directory, **changes):
    job = json.loads((directory / "job.json").read_text())
    job.update({RECORD_KEYS.get(key, key): value for key, value in changes.items()})
    (directory / "job.json").write_text(json.dumps(job))


def case_again(directory, status="passed", **changes):
    (directory / "public/results.json").unlink()
    (directory / "public").rmdir()
    write_case(directory, CASE, status, **changes)


@pytest.mark.parametrize(
    "body,expected",
    [
        ('<testcase name="n"><skipped/></testcase>', ("skipped", "pytest_skipped")),
        ('<testcase name="n"><failure/></testcase>', ("failed", "case_failed")),
        ('<testcase name="n"><error/></testcase>', ("failed", "case_failed")),
        ("", ("failed", "collection")),
        ('<testcase name="a"/><testcase name="b"/>', ("failed", "collection")),
        (None, ("failed", "no_result")),
    ],
)
def test_a_skipped_failed_or_absent_node_is_not_a_pass(tmp_path, body, expected):
    value, _, second = passing(tmp_path)
    (second / "public/node/junit.xml").unlink()
    if body is not None:
        (second / "public/node/junit.xml").write_text(f"<testsuite>{body}</testsuite>")
    payload, rows = statuses(value, tmp_path)
    assert rows[1] == expected and payload["status"] == "incomplete_or_failed"


def test_a_single_entry_downloaded_without_its_directory_is_found(tmp_path):
    value = plan_of([(CASE, "case")])
    write_job(tmp_path / "flat", value["entries"][0])
    write_case(tmp_path / "flat", CASE)
    payload, rows = statuses(value, tmp_path / "flat")
    assert rows == [("passed", "")] and payload["status"] == "passed"
    other = plan_of([("fork/other", "case")])
    _, rows = statuses(other, tmp_path / "flat")
    assert rows == [("failed", "no_result")]


def test_runtime_drift_between_jobs_is_refused(tmp_path):
    value, _, second = passing(tmp_path)
    drifted = {**SOURCE, "tools": {"anvil": "anvil 2"}}
    rewrite(second, before=drifted, after=drifted)
    _, rows = statuses(value, tmp_path)
    assert rows[1] == ("blocked", "runtime_mismatch")
    assert cell_qa_summary.runtime_identity([], tmp_path) == {"python": None, "tools": None}


def test_a_requested_reverse_fault_that_does_not_exist_is_a_gap(tmp_path):
    value, _, _ = passing(tmp_path)
    value["reverse_fault"] = {"requested": True, "nodes": []}
    payload = aggregate(value, FROZEN, tmp_path, SOURCE)
    assert payload["status"] == "incomplete_or_failed"
    assert "- Problem: no reverse-fault node" in summary(payload)
    assert "the plan has no entries" in cell_qa_summary.problems_of(plan_of([]), [])


def test_hosted_uniswap_v3_bnb_plan_closes_every_obligation_of_the_cell():
    selection = SimpleNamespace(
        connector="uniswap_v3",
        chain="eip155:56",
        cases="",
        lanes="fork,backtest",
        reverse_fault=False,
        nodes="",
        commit="abc",
    )
    value = plan(selection)
    rows = [{"id": e["id"], "status": "passed"} for e in value["entries"]]
    ledger = cell_ledger(value["cell"], rows, value["excluded"])
    assert ledger and all(item["status"] == "passed" for item in ledger)
    rows[0]["status"] = "failed"
    ledger = cell_ledger(value["cell"], rows, value["excluded"])
    assert any(value["entries"][0]["id"] in item["outstanding_case_ids"] for item in ledger)
    payload = {
        "cell": value["cell"],
        "status": "incomplete_or_failed",
        "source": SOURCE,
        "frozen_inputs": FROZEN,
        "problems": [],
        "cases": [],
        "excluded": [{"id": "model-actions/x", "lane": "pytest", "reason": "lane not selected"}],
        "cell_obligations": ledger,
        "local_gates_outstanding": ["archival"],
    }
    text = summary(payload)
    assert "`uniswap_v3` on `eip155:56`" in text and "outstanding:" in text
    assert "model-actions/x" in text and "closed by this run" in text
    assert cell_ledger(None, rows, []) is None


def test_main_merges_snapshots_the_source_and_fails_an_incomplete_cell(tmp_path, monkeypatch):
    value, _, second = passing(tmp_path / "entries")
    (tmp_path / "plan.json").write_text(json.dumps(value))
    (tmp_path / "frozen.json").write_text(json.dumps(FROZEN))
    monkeypatch.setattr(cell_qa_summary, "identity", lambda: SOURCE)
    monkeypatch.setattr(cell_qa_summary, "bench_identity", lambda: {"files": {}})
    monkeypatch.setenv("CELL_QA_KEY", KEY)
    arguments = [
        "--plan",
        str(tmp_path / "plan.json"),
        "--frozen-inputs",
        str(tmp_path / "frozen.json"),
    ]
    arguments += ["--entries", str(tmp_path / "entries"), "--output", str(tmp_path / "out")]
    arguments += ["--secret-env", "CELL_QA_KEY", "--run-url", "https://example/run"]
    assert cell_qa_summary.main(arguments) == 0
    result = json.loads((tmp_path / "out/results.json").read_text())
    assert result["status"] == "passed" and result["run_url"] == "https://example/run"
    assert (tmp_path / "out/source.tar.gz").is_file() and (tmp_path / "out/summary.md").is_file()
    (second / "job.json").unlink()
    assert cell_qa_summary.main([*arguments[:6], "--output", str(tmp_path / "again")]) == 1
    monkeypatch.setattr(cell_qa_summary, "identity", lambda: {**SOURCE, "commit": "other"})
    with pytest.raises(SystemExit, match="planned commit"):
        cell_qa_summary.main(arguments)
