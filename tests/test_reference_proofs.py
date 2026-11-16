"""The proof driver plans provider steps under the lock and records every outcome offline."""

import json
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts import reference_proofs as proofs

CHECK_SUCCESS = json.dumps(
    {
        "valid": True,
        "ready": False,
        "normalized": {},
        "stages": [
            {"stage": name, "status": "passed", "message": ""}
            for name in (
                "configuration",
                "operator_configuration",
                "resolution",
                "parameters",
                "sandbox",
                "strategy",
                "capabilities",
                "data_dependencies",
                "fixture",
            )
        ]
        + [
            {
                "stage": "online_admission",
                "status": "not_checked",
                "message": (
                    "Operator execution ceilings, fixture and installed authority "
                    "remain unadmitted."
                ),
            }
        ],
    }
)


def plan_for(tmp_path, **extra):
    argv = [
        "--label",
        "exchange",
        "--env-file",
        str(tmp_path / "operator.env"),
        "--lock",
        str(tmp_path / "provider.lock"),
        "--evidence",
        str(tmp_path / "evidence"),
        "--revision",
        "abc1234",
    ]
    for key, values in extra.items():
        for value in values:
            argv.extend([f"--{key}", value])
    return proofs.build_plan(proofs.arguments(argv))


def test_plan_orders_reference_production_candidate_and_bench_steps(tmp_path):
    plan = plan_for(
        tmp_path,
        reference=["pool_a"],
        production=["pool_b"],
        candidate=["pool_c"],
        **{"bench-case": ["fork/pinned/pool_a/eoa/configured"]},
    )
    names = [step.name for step in plan.steps]
    assert names == [
        "resolve-pool_a",
        "capture-pool_a",
        "compare-pool_a",
        "check-fork-pool_a",
        "check-production-pool_b",
        "refuse-pool_c",
        "bench",
    ]
    online = {step.name for step in plan.steps if step.online}
    assert online == {"resolve-pool_a", "check-fork-pool_a", "check-production-pool_b", "bench"}
    refusals = [step.name for step in plan.steps if step.expect_refusal]
    assert refusals == ["refuse-pool_c"]
    assert (
        plan.steps[0].output
        == tmp_path / "evidence/exchange-resolutions-abc1234/pool_a/resolution.json"
    )
    assert plan.steps[4].output.name == "pool_b.production.json"
    assert "--production" in plan.steps[4].command
    assert plan.steps[-1].command[-2:] == ("--case", "fork/pinned/pool_a/eoa/configured")
    assert plan.harness == tmp_path / "evidence" / proofs.HARNESS_CAPTURES
    assert str(plan.env_file) in plan.steps[3].command


def test_dry_run_prints_the_steps_without_running_anything(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(proofs.subprocess, "run", lambda *a, **k: pytest.fail("ran a step"))
    argv = [
        "--label",
        "x",
        "--reference",
        "pool_a",
        "--env-file",
        "env",
        "--lock",
        str(tmp_path / "lock"),
        "--revision",
        "r1",
        "--dry-run",
    ]
    assert proofs.main(argv) == 0
    printed = json.loads(capsys.readouterr().out)
    assert [step["name"] for step in printed][:2] == ["resolve-pool_a", "capture-pool_a"]


def test_refusal_steps_pass_only_when_the_check_refuses_a_chain(tmp_path):
    plan = plan_for(tmp_path, candidate=["pool_c"])
    (step,) = plan.steps
    refusing = step.__class__(
        step.name,
        (
            sys.executable,
            "-c",
            "import sys; print('connector does not support eip155:56'); sys.exit(1)",
        ),
        step.output,
        expect_refusal=True,
    )
    name, ok = proofs.run_step(plan, refusing, tmp_path / "provider.lock", skip_done=False)
    assert (name, ok) == ("refuse-pool_c", True)
    assert "does not support" in step.output.read_text()
    accepting = step.__class__(
        step.name, (sys.executable, "-c", "print('valid')"), step.output, expect_refusal=True
    )
    assert proofs.run_step(plan, accepting, tmp_path / "provider.lock", skip_done=False)[1] is False
    log = (tmp_path / "evidence/exchange-proofs-abc1234.log").read_text().splitlines()
    assert [json.loads(line)["ok"] for line in log] == [True, False]


def test_skip_done_leaves_a_successful_matching_output_alone(tmp_path, monkeypatch):
    plan = plan_for(tmp_path, reference=["pool_a"])
    original = plan.steps[3]
    step = original.__class__(
        original.name,
        original.command,
        original.output,
        inputs=(),
    )
    monkeypatch.setattr(proofs, "source_digest", lambda: "source-a")
    monkeypatch.setattr(
        proofs,
        "run_owned",
        lambda *a, **k: SimpleNamespace(returncode=0, stdout=CHECK_SUCCESS, stderr=""),
    )
    assert proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=False)[1] is True
    monkeypatch.setattr(proofs, "run_owned", lambda *a, **k: pytest.fail("ran a step"))
    assert proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=True) == (
        step.name,
        True,
    )


def test_skip_done_retries_failed_and_partial_outputs(tmp_path, monkeypatch):
    plan = plan_for(tmp_path, reference=["pool_a"])
    original = plan.steps[3]
    step = original.__class__(original.name, original.command, original.output, inputs=())
    monkeypatch.setattr(proofs, "source_digest", lambda: "source-a")
    results = iter(
        [
            SimpleNamespace(returncode=1, stdout="failed: transport", stderr=""),
            SimpleNamespace(returncode=0, stdout=CHECK_SUCCESS, stderr=""),
        ]
    )
    calls = []

    def run(*args, **kwargs):
        calls.append(args)
        return next(results)

    monkeypatch.setattr(proofs, "run_owned", run)
    assert proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=False)[1] is False
    assert not proofs.completion_path(step).exists()
    assert proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=True)[1] is True
    assert len(calls) == 2
    attempts = [
        json.loads(line)
        for line in (tmp_path / "evidence/exchange-proofs-abc1234.log").read_text().splitlines()
    ]
    assert attempts[0]["ok"] is False
    assert "transport" in attempts[0]["stdout_tail"]

    step.output.write_text("{")
    monkeypatch.setattr(
        proofs,
        "run_owned",
        lambda *a, **k: SimpleNamespace(returncode=0, stdout=CHECK_SUCCESS, stderr=""),
    )
    assert proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=True)[1] is True


def test_skip_done_retries_when_inputs_or_source_change(tmp_path, monkeypatch):
    plan = plan_for(tmp_path)
    source = ["source-a"]
    input_file = tmp_path / "input.json"
    output = tmp_path / "output.json"
    input_file.write_text('{"version": 1}')
    step = proofs.Step("probe", ("probe",), output, inputs=(input_file,))
    calls = []

    def run(*args, **kwargs):
        calls.append(args)
        output.write_text('{"complete": true}')
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(proofs, "source_digest", lambda: source[0])
    monkeypatch.setattr(proofs, "run_owned", run)
    assert proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=False)[1] is True
    assert proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=True)[1] is True
    input_file.write_text('{"version": 2}')
    assert proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=True)[1] is True
    source[0] = "source-b"
    assert proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=True)[1] is True
    assert len(calls) == 3


def test_bench_source_change_cannot_create_completion(tmp_path, monkeypatch):
    plan = plan_for(tmp_path)
    package = tmp_path / "bench"
    output = package / "public/results.json"
    step = proofs.Step("bench", ("bench",), output, expected_cases=("selected",), package=package)
    monkeypatch.setattr(proofs, "source_digest", lambda: "source-a")

    def run(*args, **kwargs):
        output.parent.mkdir(parents=True)
        output.write_text(
            json.dumps(
                {
                    "source_changed": True,
                    "selected_cases": ["selected"],
                    "cases": [
                        {"id": "selected", "status": "blocked", "category": "source_changed"}
                    ],
                }
            )
        )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(proofs, "run_owned", run)
    assert proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=False)[1] is False
    assert not proofs.completion_path(step).exists()


@pytest.mark.parametrize(
    ("name", "payload"),
    [
        ("resolve-probe", {"context": {}, "result": None}),
        (
            "compare-probe",
            {"schema": "almanak.reference-conversion-comparison.v1", "equal": False},
        ),
    ],
)
def test_semantically_incomplete_success_is_not_resumable(tmp_path, monkeypatch, name, payload):
    plan = plan_for(tmp_path)
    output = tmp_path / "result.json"
    step = proofs.Step(name, ("probe",), output)
    monkeypatch.setattr(proofs, "source_digest", lambda: "source-a")

    def run(*args, **kwargs):
        output.write_text(json.dumps(payload))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(proofs, "run_owned", run)
    assert proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=False)[1] is False
    assert not proofs.completion_path(step).exists()


@pytest.mark.parametrize(
    ("name", "payload"),
    [
        (
            "capture-probe",
            {"schema": "almanak.author-hook-baseline.v1", "content_sha256": "digest"},
        ),
        (
            "compare-probe",
            {"schema": "almanak.reference-conversion-comparison.v1", "equal": True},
        ),
    ],
)
def test_retry_preserves_existing_file_before_real_process(name, payload, tmp_path, monkeypatch):
    plan = plan_for(tmp_path)
    output = tmp_path / f"{name}.json"
    output.write_text('{"partial": true}')
    command = (
        sys.executable,
        "-c",
        "import pathlib, sys\n"
        "path = pathlib.Path(sys.argv[1])\n"
        "if path.exists(): raise SystemExit(9)\n"
        "path.write_text(sys.argv[2])",
        str(output),
        json.dumps(payload),
    )
    step = proofs.Step(name, command, output)
    monkeypatch.setattr(proofs, "source_digest", lambda: "source-a")

    assert proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=True)[1] is True
    archived = plan.directory("attempts") / name / "0001" / output.name
    assert json.loads(archived.read_text()) == {"partial": True}
    assert json.loads(output.read_text()) == payload


def test_retry_preserves_existing_bench_package_before_real_process(tmp_path, monkeypatch):
    plan = plan_for(tmp_path)
    package = tmp_path / "bench"
    output = package / "public/results.json"
    output.parent.mkdir(parents=True)
    output.write_text('{"partial": true}')
    payload = {
        "selected_cases": ["selected"],
        "cases": [{"id": "selected", "status": "passed"}],
    }
    command = (
        sys.executable,
        "-c",
        "import pathlib, sys\n"
        "root = pathlib.Path(sys.argv[1])\n"
        "if root.exists(): raise SystemExit(9)\n"
        "(root / 'public').mkdir(parents=True)\n"
        "(root / 'public/results.json').write_text(sys.argv[2])",
        str(package),
        json.dumps(payload),
    )
    step = proofs.Step("bench", command, output, expected_cases=("selected",), package=package)
    monkeypatch.setattr(proofs, "source_digest", lambda: "source-a")

    assert proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=True)[1] is True
    archived = plan.directory("attempts") / "bench/0001/bench/public/results.json"
    assert json.loads(archived.read_text()) == {"partial": True}
    assert json.loads(output.read_text()) == payload


def test_retry_preserves_entire_resolution_package_before_real_process(tmp_path, monkeypatch):
    plan = plan_for(tmp_path)
    package = tmp_path / "resolution"
    output = package / "resolution.json"
    (package / "private").mkdir(parents=True)
    output.write_text('{"result": null}')
    (package / "private/read-gateway.log").write_text("failed provider attempt")
    payload = {"binding": {"digest": "binding"}, "result": {"markets": []}, "result_digest": "x"}
    command = (
        sys.executable,
        "-c",
        "import pathlib, sys\n"
        "root = pathlib.Path(sys.argv[1])\n"
        "if root.exists(): raise SystemExit(9)\n"
        "root.mkdir()\n"
        "(root / 'resolution.json').write_text(sys.argv[2])",
        str(package),
        json.dumps(payload),
    )
    step = proofs.Step("resolve-probe", command, output, package=package)
    monkeypatch.setattr(proofs, "source_digest", lambda: "source-a")

    assert proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=True)[1] is True
    archived = plan.directory("attempts") / "resolve-probe/0001/resolution"
    assert (archived / "private/read-gateway.log").read_text() == "failed provider attempt"
    assert json.loads((archived / "resolution.json").read_text()) == {"result": None}


def test_bench_selection_matches_exact_set_without_order_dependency():
    payload = {
        "selected_cases": ["case-b", "case-a"],
        "cases": [
            {"id": "case-a", "status": "passed"},
            {"id": "case-b", "status": "passed"},
        ],
    }
    assert proofs.valid_bench(payload, ("case-a", "case-b")) is True
    payload["selected_cases"] = ["case-a", "case-a"]
    assert proofs.valid_bench(payload, ("case-a", "case-b")) is False


@pytest.mark.parametrize("mutation", ["missing", "failed", "unknown"])
def test_actual_check_contract_rejects_incomplete_or_unknown_stages(tmp_path, mutation):
    payload = json.loads(CHECK_SUCCESS)
    if mutation == "missing":
        payload["stages"] = payload["stages"][1:]
    elif mutation == "failed":
        payload["stages"][0]["status"] = "failed"
    else:
        payload["stages"].append({"stage": "unexpected", "status": "passed", "message": ""})
    output = tmp_path / "check.json"
    output.write_text(json.dumps(payload))
    assert proofs.valid_output(proofs.Step("check-fork-probe", (), output)) is False


def test_timeout_is_retained_as_failure_and_clears_marker(tmp_path, monkeypatch):
    plan = plan_for(tmp_path)
    output = tmp_path / "check.json"
    step = proofs.Step("check-probe", ("probe",), output, timeout=3)
    monkeypatch.setattr(proofs, "source_digest", lambda: "source-a")
    output.write_text(CHECK_SUCCESS)
    proofs.write_completion(step, proofs.step_identity(step))
    monkeypatch.setattr(
        proofs,
        "run_owned",
        lambda *a, **k: (_ for _ in ()).throw(
            subprocess.TimeoutExpired("probe", 3, output="partial", stderr="waiting")
        ),
    )

    assert proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=False)[1] is False
    assert not proofs.completion_path(step).exists()
    log = (tmp_path / "evidence/exchange-proofs-abc1234.log").read_text()
    assert "timed out after 3 seconds" in log


@pytest.mark.skipif(sys.platform != "linux", reason="Linux descendant registry uses /proc")
def test_timeout_stops_nested_child_in_independent_session(tmp_path, monkeypatch):
    plan = plan_for(tmp_path)
    pid_file = tmp_path / "nested.pid"
    child = (
        "import os, pathlib, sys, time; "
        "os.setsid(); pathlib.Path(sys.argv[1]).write_text(str(os.getpid())); time.sleep(60)"
    )
    command = (
        sys.executable,
        "-c",
        "import subprocess, sys, time; "
        "subprocess.Popen([sys.executable, '-c', sys.argv[1], sys.argv[2]]); time.sleep(60)",
        child,
        str(pid_file),
    )
    step = proofs.Step("probe", command, tmp_path / "unused.json", timeout=1)
    monkeypatch.setattr(proofs, "source_digest", lambda: "source-a")

    assert proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=False)[1] is False
    pid = int(pid_file.read_text())
    remaining = proofs.process_identity(pid)
    assert remaining is None or remaining[3] == "Z"


def test_timeout_kills_term_ignoring_child_in_owned_group(tmp_path, monkeypatch):
    plan = plan_for(tmp_path)
    pid_file = tmp_path / "nested.pid"
    child = (
        "import os, pathlib, signal, sys, time; "
        "signal.signal(signal.SIGTERM, signal.SIG_IGN); "
        "fields=pathlib.Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split(); "
        "pathlib.Path(sys.argv[1]).write_text(f'{os.getpid()}:{fields[19]}'); time.sleep(60)"
    )
    command = (
        sys.executable,
        "-c",
        "import subprocess, sys, time; "
        "subprocess.Popen([sys.executable, '-c', sys.argv[1], sys.argv[2]]); time.sleep(60)",
        child,
        str(pid_file),
    )
    step = proofs.Step("probe", command, tmp_path / "unused.json", timeout=1)
    monkeypatch.setattr(proofs, "source_digest", lambda: "source-a")

    assert proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=False)[1] is False
    pid, started = pid_file.read_text().split(":")
    assert proofs.same_process((int(pid), started)) is False


def test_interruption_records_failure_before_propagating(tmp_path, monkeypatch):
    plan = plan_for(tmp_path)
    step = proofs.Step("probe", ("probe",), tmp_path / "unused.json")
    monkeypatch.setattr(proofs, "source_digest", lambda: "source-a")
    monkeypatch.setattr(
        proofs,
        "run_owned",
        lambda *a, **k: (_ for _ in ()).throw(
            proofs.ProcessInterrupted("partial output", "child stopped")
        ),
    )

    with pytest.raises(KeyboardInterrupt):
        proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=False)
    attempt = json.loads(
        (tmp_path / "evidence/exchange-proofs-abc1234.log").read_text().splitlines()[-1]
    )
    assert attempt["exit"] == 130
    assert attempt["ok"] is False
    assert "interrupted" in attempt["stderr_tail"]


def test_unconfirmed_cleanup_records_failure_and_halts_queue(tmp_path, monkeypatch):
    plan = plan_for(tmp_path)
    step = proofs.Step("probe", ("probe",), tmp_path / "unused.json")
    monkeypatch.setattr(proofs, "source_digest", lambda: "source-a")
    monkeypatch.setattr(
        proofs,
        "run_owned",
        lambda *a, **k: (_ for _ in ()).throw(
            proofs.ProcessCleanupFailed("partial", "term ignored", (1234,))
        ),
    )

    with pytest.raises(RuntimeError, match="provider queue halted"):
        proofs.run_step(plan, step, tmp_path / "provider.lock", skip_done=False)
    attempt = json.loads(
        (tmp_path / "evidence/exchange-proofs-abc1234.log").read_text().splitlines()[-1]
    )
    assert attempt["exit"] == 125
    assert attempt["ok"] is False
    assert "1234" in attempt["stderr_tail"]


def test_online_steps_hold_the_lock_while_they_run(tmp_path):
    plan = plan_for(tmp_path, reference=["pool_a"])
    lock = tmp_path / "provider.lock"
    probe = (
        sys.executable,
        "-c",
        "import fcntl, sys\n"
        "handle = open(sys.argv[1], 'a')\n"
        "fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)",
        str(lock),
    )
    step = plan.steps[0].__class__("resolve-probe", probe, tmp_path / "out", online=True)
    name, ok = proofs.run_step(plan, step, lock, skip_done=False)
    assert ok is False, "the probe could not take the lock the driver was holding"
