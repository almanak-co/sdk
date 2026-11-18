"""Provider-backed proofs for converted references, one provider job at a time.

Per supported reference: one read-gateway resolution with its fork operator
example, the public hook capture over that retained resolution, the comparison
against the batch harness capture, and `almanak check --fork --online`. Named
references also get `check --production --online`; candidates get the offline
check that must refuse; the QA bench runs the named cases last. Provider steps
hold the lock file; evidence lands under the ignored evidence directory in
directories labeled by the source revision. No credential value is ever read
by this tool: the operator env file is only passed on by path.
"""

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence"
HARNESS_CAPTURES = "author-hooks-harness-7fc7925"
REFUSAL = "connector does not support"


class ProcessInterrupted(Exception):
    def __init__(self, stdout: str, stderr: str):
        self.stdout = stdout
        self.stderr = stderr


class ProcessCleanupFailed(Exception):
    def __init__(self, stdout: str, stderr: str, survivors: tuple[int, ...]):
        self.stdout = stdout
        self.stderr = stderr
        self.survivors = survivors


@dataclass(frozen=True)
class Step:
    name: str
    command: tuple[str, ...]
    output: Path
    online: bool = False
    expect_refusal: bool = False
    timeout: int = 5400
    inputs: tuple[Path, ...] = ()
    expected_cases: tuple[str, ...] = ()
    package: Path | None = None


@dataclass
class Plan:
    label: str
    revision: str
    evidence: Path
    env_file: Path
    operators: Path
    harness: Path
    bench_timeout: int
    steps: list[Step] = field(default_factory=list)

    def directory(self, kind: str) -> Path:
        return self.evidence / f"{self.label}-{kind}-{self.revision}"


def main(argv=None) -> int:
    args = arguments(argv)
    plan = build_plan(args)
    if args.dry_run:
        print(json.dumps([describe(step) for step in plan.steps], indent=2))
        return 0
    outcomes = [run_step(plan, step, args.lock, args.skip_done) for step in plan.steps]
    failed = [name for name, ok in outcomes if not ok]
    print(json.dumps({"steps": len(outcomes), "failed": failed}))
    return 1 if failed else 0


def arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--label", required=True, help="evidence directory prefix, e.g. exchange")
    parser.add_argument("--reference", action="append", default=[])
    parser.add_argument("--production", action="append", default=[])
    parser.add_argument("--candidate", action="append", default=[])
    parser.add_argument("--bench-case", action="append", default=[])
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, default=EVIDENCE)
    parser.add_argument("--operators", type=Path, default=ROOT / "examples/operators")
    parser.add_argument("--harness", type=Path, default=None)
    parser.add_argument("--bench-timeout", type=int, default=7200)
    parser.add_argument("--revision", default=None)
    parser.add_argument("--skip-done", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def build_plan(args) -> Plan:
    revision = args.revision or current_revision()
    plan = Plan(
        label=args.label,
        revision=revision,
        evidence=args.evidence,
        env_file=args.env_file,
        operators=args.operators,
        harness=args.harness or args.evidence / HARNESS_CAPTURES,
        bench_timeout=args.bench_timeout,
    )
    for name in args.reference:
        plan.steps.extend(reference_steps(plan, name))
    for name in args.production:
        plan.steps.append(check_step(plan, name, "production"))
    for name in args.candidate:
        plan.steps.append(candidate_step(plan, name))
    if args.bench_case:
        plan.steps.append(bench_step(plan, args.bench_case))
    return plan


def current_revision() -> str:
    output = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True
    )
    return output.stdout.strip() or "unknown"


def reference_steps(plan: Plan, name: str) -> list[Step]:
    resolution = plan.directory("resolutions") / name / "resolution.json"
    capture = plan.directory("hooks") / f"{name}.json"
    comparison = plan.directory("comparisons") / f"{name}.json"
    strategy = ROOT / "strategies" / name
    return [
        Step(
            f"resolve-{name}",
            (
                *python(),
                "-m",
                "scripts.reference_proofs",
                "resolve",
                name,
                str(resolution.parent),
                str(plan.env_file),
                str(plan.operators),
            ),
            resolution,
            online=True,
            inputs=(
                strategy / "config.yaml",
                strategy / "strategy.py",
                plan.operators / f"{name}.fork.yaml",
            ),
            package=resolution.parent,
        ),
        Step(
            f"capture-{name}",
            (
                *python(),
                "-m",
                "scripts.capture_author_hooks",
                "--config",
                str(strategy / "config.yaml"),
                "--strategy",
                str(strategy / "strategy.py"),
                "--resolution",
                str(resolution),
                "--output",
                str(capture),
            ),
            capture,
            inputs=(strategy / "config.yaml", strategy / "strategy.py", resolution),
        ),
        Step(
            f"compare-{name}",
            (
                *python(),
                "-m",
                "scripts.compare_reference_conversion",
                "--before",
                str(plan.harness / f"{name}.json"),
                "--after",
                str(capture),
                "--output",
                str(comparison),
            ),
            comparison,
            inputs=(plan.harness / f"{name}.json", capture),
        ),
        check_step(plan, name, "fork"),
    ]


def check_step(plan: Plan, name: str, mode: str) -> Step:
    suffix = "" if mode == "fork" else f".{mode}"
    return Step(
        f"check-{mode}-{name}",
        (
            *python(),
            "-m",
            "almanak.cli",
            "check",
            str(ROOT / "strategies" / name),
            f"--{mode}",
            "--online",
            "--gateway-config",
            str(plan.operators / f"{name}.{mode}.yaml"),
            "--env-file",
            str(plan.env_file),
            "--normalized",
            "--json",
        ),
        plan.directory("checks") / f"{name}{suffix}.json",
        online=True,
        inputs=(
            ROOT / "strategies" / name / "config.yaml",
            ROOT / "strategies" / name / "strategy.py",
            plan.operators / f"{name}.{mode}.yaml",
        ),
    )


def candidate_step(plan: Plan, name: str) -> Step:
    return Step(
        f"refuse-{name}",
        (*python(), "-m", "almanak.cli", "check", str(ROOT / "strategies" / name), "--json"),
        plan.directory("checks") / f"{name}.json",
        expect_refusal=True,
        timeout=600,
        inputs=(
            ROOT / "strategies" / name / "config.yaml",
            ROOT / "strategies" / name / "strategy.py",
        ),
    )


def bench_step(plan: Plan, cases: list[str]) -> Step:
    selection = [item for case in cases for item in ("--case", case)]
    return Step(
        "bench",
        (
            *python(),
            "-m",
            "scripts.qa_bench",
            "--output",
            str(plan.directory("bench")),
            "--env-file",
            str(plan.env_file),
            "--timeout",
            str(plan.bench_timeout),
            *selection,
        ),
        plan.directory("bench") / "public" / "results.json",
        online=True,
        timeout=len(cases) * plan.bench_timeout,
        expected_cases=tuple(cases),
        package=plan.directory("bench"),
    )


def python() -> tuple[str, ...]:
    return (sys.executable,)


def describe(step: Step) -> dict:
    return {
        "name": step.name,
        "online": step.online,
        "expect_refusal": step.expect_refusal,
        "output": str(step.output),
        "command": list(step.command),
    }


def run_step(plan: Plan, step: Step, lock: Path, skip_done: bool) -> tuple[str, bool]:
    with locked(step_lock(plan, step)):
        identity = step_identity(step)
        if skip_done and completion_matches(step, identity):
            return step.name, True
        preserve_previous(plan, step)
        (step.package.parent if step.package else step.output.parent).mkdir(
            parents=True, exist_ok=True
        )
        started = time.monotonic()
        interrupted = False
        cleanup_failed = False
        try:
            with locked(lock) if step.online else nullcontext():
                process = run_owned(step.command, step.timeout)
        except subprocess.TimeoutExpired as error:
            process = subprocess.CompletedProcess(
                step.command,
                124,
                stdout=as_text(error.stdout),
                stderr=as_text(error.stderr) + f"\ntimed out after {step.timeout} seconds",
            )
        except ProcessInterrupted as error:
            interrupted = True
            process = subprocess.CompletedProcess(
                step.command, 130, stdout=error.stdout, stderr=error.stderr + "\ninterrupted"
            )
        except ProcessCleanupFailed as error:
            process = subprocess.CompletedProcess(
                step.command,
                125,
                stdout=error.stdout,
                stderr=(
                    error.stderr
                    + f"\ncleanup could not stop owned processes: {list(error.survivors)}"
                ),
            )
            cleanup_failed = True
        write_output(step, process)
        ok = outcome(step, process) and identity == step_identity(step) and valid_output(step)
        record(plan, step, process, ok, time.monotonic() - started)
        if ok:
            write_completion(step, identity)
        else:
            completion_path(step).unlink(missing_ok=True)
        if interrupted:
            raise KeyboardInterrupt
        if cleanup_failed:
            raise RuntimeError("proof process cleanup failed; provider queue halted")
        return step.name, ok


def run_owned(command: tuple[str, ...], timeout: int) -> subprocess.CompletedProcess:
    """Run one owned process tree and leave no observed descendants behind."""
    with tempfile.TemporaryFile(mode="w+") as stdout, tempfile.TemporaryFile(mode="w+") as stderr:
        process = subprocess.Popen(
            list(command),
            cwd=ROOT,
            stdout=stdout,
            stderr=stderr,
            text=True,
            start_new_session=True,
        )
        owned: set[tuple[int, str]] = set()
        group = owned_group(process)
        deadline = time.monotonic() + timeout
        try:
            while True:
                owned.update(descendants(process.pid, *matching_pids(owned)))
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(command, timeout)
                try:
                    returncode = process.wait(timeout=min(0.1, remaining))
                    break
                except subprocess.TimeoutExpired:
                    continue
        except subprocess.TimeoutExpired:
            survivors = stop_owned(process, owned, group)
            stdout.seek(0)
            stderr.seek(0)
            if survivors:
                raise ProcessCleanupFailed(stdout.read(), stderr.read(), survivors) from None
            raise subprocess.TimeoutExpired(
                command, timeout, output=stdout.read(), stderr=stderr.read()
            ) from None
        except KeyboardInterrupt:
            survivors = stop_owned(process, owned, group)
            stdout.seek(0)
            stderr.seek(0)
            if survivors:
                raise ProcessCleanupFailed(stdout.read(), stderr.read(), survivors) from None
            raise ProcessInterrupted(stdout.read(), stderr.read()) from None
        except BaseException:
            stop_owned(process, owned, group)
            raise
        survivors = stop_owned(process, owned, group)
        stdout.seek(0)
        stderr.seek(0)
        if survivors:
            raise ProcessCleanupFailed(stdout.read(), stderr.read(), survivors)
        return subprocess.CompletedProcess(command, returncode, stdout.read(), stderr.read())


def descendants(*root_pids: int) -> set[tuple[int, str]]:
    """Snapshot Linux descendants with start times to prevent PID-reuse signaling."""
    if sys.platform != "linux":
        return set()
    records = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        identity = process_identity(int(entry.name))
        if identity is not None:
            records[identity[0]] = identity
    found: set[tuple[int, str]] = set()
    parents = set(root_pids)
    while parents:
        children = {record for record in records.values() if record[2] in parents}
        found.update((pid, started) for pid, started, _, _ in children)
        parents = {pid for pid, _, _, _ in children}
    return found


def process_identity(pid: int) -> tuple[int, str, int, str] | None:
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        return pid, fields[19], int(fields[1]), fields[0]
    except (OSError, IndexError, ValueError):
        return None


def owned_group(process: subprocess.Popen) -> int | None:
    try:
        group = os.getpgid(process.pid)
    except ProcessLookupError:
        return None
    return group if group == process.pid else None


def matching_pids(owned: set[tuple[int, str]]) -> set[int]:
    return {pid for pid, started in owned if same_process((pid, started))}


def stop_owned(
    process: subprocess.Popen,
    owned: set[tuple[int, str]],
    group: int | None,
    grace: float = 2,
) -> tuple[int, ...]:
    owned.update(descendants(process.pid, *matching_pids(owned)))
    signal_group(group, signal.SIGTERM)
    signal_owned(owned, signal.SIGTERM)
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline and any(same_process(item) for item in owned):
        owned.update(descendants(*matching_pids(owned)))
        time.sleep(0.05)
    signal_group(group, signal.SIGKILL)
    signal_owned(owned, signal.SIGKILL)
    try:
        process.wait(timeout=grace)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        survivors = tuple(pid for pid, started in owned if same_process((pid, started)))
        if not survivors:
            return ()
        time.sleep(0.05)
    return tuple(pid for pid, started in owned if same_process((pid, started)))


def signal_group(group: int | None, signum: signal.Signals) -> None:
    if group is None:
        return
    try:
        os.killpg(group, signum)
    except ProcessLookupError:
        pass


def signal_owned(owned: set[tuple[int, str]], signum: signal.Signals) -> None:
    for pid, started in owned:
        if same_process((pid, started)):
            try:
                os.kill(pid, signum)
            except ProcessLookupError:
                pass


def same_process(identity: tuple[int, str]) -> bool:
    current = process_identity(identity[0])
    return current is not None and current[1] == identity[1] and current[3] != "Z"


def step_lock(plan: Plan, step: Step) -> Path:
    return plan.directory("proof-locks") / f"{step.name}.lock"


def as_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    return value.decode(errors="replace") if isinstance(value, bytes) else value


def preserve_previous(plan: Plan, step: Step) -> None:
    package = step.package or step.output
    marker = completion_path(step)
    if not package.exists() and not marker.exists():
        return
    attempts = plan.directory("attempts") / step.name
    attempts.mkdir(parents=True, exist_ok=True)
    sequence = max(
        (int(path.name) for path in attempts.iterdir() if path.name.isdigit()), default=0
    )
    destination = attempts / f"{sequence + 1:04d}"
    destination.mkdir()
    if package.exists():
        shutil.move(str(package), destination / package.name)
    if marker.exists():
        shutil.move(str(marker), destination / marker.name)


def outcome(step: Step, completed) -> bool:
    if step.expect_refusal:
        return completed.returncode != 0 and REFUSAL in (completed.stdout + completed.stderr)
    return completed.returncode == 0


def completion_path(step: Step) -> Path:
    return step.output.with_name(f"{step.output.name}.completion.json")


def completion_matches(step: Step, identity: str) -> bool:
    try:
        marker = json.loads(completion_path(step).read_text())
    except (OSError, json.JSONDecodeError):
        return False
    return marker == {
        "identity": identity,
        "output_sha256": file_digest(step.output),
    } and valid_output(step)


def write_completion(step: Step, identity: str) -> None:
    marker = completion_path(step)
    temporary = marker.with_name(f".{marker.name}.tmp")
    payload = {"identity": identity, "output_sha256": file_digest(step.output)}
    temporary.write_text(json.dumps(payload, sort_keys=True) + "\n")
    temporary.replace(marker)


def step_identity(step: Step) -> str:
    digest = hashlib.sha256()
    digest.update(json.dumps(list(step.command), separators=(",", ":")).encode())
    digest.update(source_digest().encode())
    for path in step.inputs:
        digest.update(str(path.resolve()).encode())
        digest.update(path_digest(path).encode())
    return digest.hexdigest()


def source_digest() -> str:
    listed = subprocess.run(
        ["git", "ls-files", "-co", "--exclude-standard", "-z"],
        cwd=ROOT,
        capture_output=True,
        check=True,
    ).stdout
    digest = hashlib.sha256()
    for relative in sorted(filter(None, listed.split(b"\0"))):
        path = ROOT / relative.decode()
        if path.is_file():
            digest.update(relative)
            digest.update(file_digest(path).encode())
    return digest.hexdigest()


def path_digest(path: Path) -> str:
    if path.is_file():
        return file_digest(path)
    if path.is_dir():
        digest = hashlib.sha256()
        for child in sorted(item for item in path.rglob("*") if item.is_file()):
            digest.update(str(child.relative_to(path)).encode())
            digest.update(file_digest(child).encode())
        return digest.hexdigest()
    return "missing"


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError:
        return "missing"
    return digest.hexdigest()


def valid_output(step: Step) -> bool:
    try:
        content = step.output.read_text()
        if step.expect_refusal:
            return REFUSAL in content
        payload = json.loads(content)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict):
        return False
    if step.name.startswith("resolve-"):
        return valid_resolution(payload)
    if step.name.startswith("capture-"):
        return valid_capture(payload)
    if step.name.startswith("compare-"):
        return valid_comparison(payload)
    if step.name.startswith("check-"):
        return valid_check(payload, step.name)
    if step.name == "bench":
        return valid_bench(payload, step.expected_cases)
    return True


def valid_resolution(payload: dict) -> bool:
    return all(payload.get(field) for field in ("binding", "result", "result_digest"))


def valid_capture(payload: dict) -> bool:
    return payload.get("schema") == "almanak.author-hook-baseline.v1" and bool(
        payload.get("content_sha256")
    )


def valid_comparison(payload: dict) -> bool:
    return (
        payload.get("schema") == "almanak.reference-conversion-comparison.v1"
        and payload.get("equal") is True
    )


def valid_check(payload: dict, step_name: str) -> bool:
    stages = payload.get("stages")
    if not isinstance(stages, list) or not all(isinstance(stage, dict) for stage in stages):
        return False
    expected = {
        "configuration",
        "operator_configuration",
        "resolution",
        "parameters",
        "sandbox",
        "strategy",
        "capabilities",
        "data_dependencies",
        "fixture" if step_name.startswith("check-fork-") else "policy",
        "online_admission",
    }
    names = {stage.get("stage") for stage in stages}
    admission = [stage for stage in stages if stage.get("stage") == "online_admission"]
    ordinary = [stage for stage in stages if stage.get("stage") != "online_admission"]
    return (
        payload.get("valid") is True
        and payload.get("ready") is False
        and isinstance(payload.get("normalized"), dict)
        and names == expected
        and all(stage.get("status") == "passed" for stage in ordinary)
        and len(admission) == 1
        and admission[0].get("status") == "not_checked"
        and "remain unadmitted" in admission[0].get("message", "")
    )


def valid_bench(payload: dict, expected_cases: tuple[str, ...]) -> bool:
    cases = payload.get("cases")
    selected = payload.get("selected_cases")
    if (
        payload.get("source_changed")
        or not isinstance(cases, list)
        or not isinstance(selected, list)
        or len(selected) != len(expected_cases)
        or len(set(selected)) != len(selected)
        or set(selected) != set(expected_cases)
    ):
        return False
    status = {row.get("id"): row.get("status") for row in cases if isinstance(row, dict)}
    return (
        bool(expected_cases)
        and len(status) == len(cases)
        and all(status.get(case) == "passed" for case in expected_cases)
    )


def write_output(step: Step, completed) -> None:
    if step.name.startswith(("check-", "refuse-")):
        step.output.write_text(completed.stdout or completed.stderr)


def record(plan: Plan, step: Step, completed, ok: bool, seconds: float):
    """Append every attempt to the summary, including failures retained for diagnosis."""
    log = plan.evidence / f"{plan.label}-proofs-{plan.revision}.log"
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("a") as handle:
        handle.write(
            json.dumps(
                {
                    "step": step.name,
                    "ok": ok,
                    "exit": completed.returncode,
                    "seconds": round(seconds, 1),
                    "stdout_tail": completed.stdout[-400:],
                    "stderr_tail": completed.stderr[-400:],
                }
            )
            + "\n"
        )


@contextmanager
def locked(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def resolve(name: str, directory: str, env_file: str, operators: str) -> int:
    """One read-gateway resolution through the test adapter; resolution.json is retained."""
    from tests.strategy_fork.adapter import resolve_reference

    config = ROOT / "strategies" / name / "config.yaml"
    operator = Path(operators) / f"{name}.fork.yaml"
    progress = resolve_reference(config, operator, Path(env_file), Path(directory))
    summary = [market.resolved.summary() for market in progress.result.markets]
    print(json.dumps({"facts": progress.result.facts_digest, "markets": summary}))
    return 0


def entry(argv=None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv[:1] == ["resolve"]:
        return resolve(argv[1], argv[2], argv[3], argv[4])
    return main(argv)


if __name__ == "__main__":
    sys.exit(entry())
