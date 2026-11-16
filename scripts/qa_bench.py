"""Run the same daily QA plan locally and in CI, retaining incomplete cases."""

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict
from pathlib import Path

from tests.qa.catalog import ROOT, Obligation, catalog, validate
from tests.qa.inputs import (
    frozen_origins,
    require_providers,
    resolve_origins,
    utc_now,
    write_json,
)
from tests.qa.process import pytest_workers, run
from tests.qa.results import digest, export_evidence, row, save_results, secrets_for
from tests.qa.source import changes, snapshot, tool_versions


def identity():
    paths = subprocess.check_output(
        ["git", "ls-files", "-co", "--exclude-standard"], cwd=ROOT, text=True
    ).splitlines()
    files = {name: digest(ROOT / name) for name in sorted(set(paths)) if (ROOT / name).is_file()}
    return {
        "commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "tree_sha256": hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest(),
        "files": files,
        "python": sys.version,
        "lock_sha256": digest(ROOT / "uv.lock"),
    }


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, help="explicit read-only provider credentials")
    parser.add_argument("--previous", type=Path, help="previous results.json for change reporting")
    parser.add_argument(
        "--plan", action="store_true", help="validate and emit the matrix without provider access"
    )
    parser.add_argument(
        "--case",
        action="append",
        default=[],
        help="exact case IDs; unselected cases remain skipped",
    )
    parser.add_argument(
        "--lane",
        action="append",
        choices=["offline", "pytest", "fork", "faults", "backtest", "blocked"],
    )
    parser.add_argument(
        "--frozen-inputs",
        type=Path,
        help="fresh fork origins and input time resolved once before a hosted fan-out",
    )
    parser.add_argument("--timeout", type=int, default=3600)
    parser.add_argument(
        "--workers",
        type=pytest_workers,
        default="auto",
        help="offline pytest workers: auto or a positive integer; provider cases remain serial",
    )
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    return args


def select(cases, args):
    unknown = set(args.case) - {c.id for c in cases}
    if unknown:
        raise ValueError(f"unknown case IDs: {sorted(unknown)}")
    return [
        c
        for c in cases
        if (not args.case or c.id in args.case) and (not args.lane or c.lane in args.lane)
    ]


def worker_command(case, directory, args, now):
    command = [
        sys.executable,
        "-m",
        "tests.qa.worker",
        "--case",
        case.id,
        "--directory",
        str(directory),
        "--origins",
        str(args.output / "private/origins.json"),
        "--now",
        str(now),
        "--workers",
        str(args.workers if case.lane == "offline" else 1),
        "--timeout",
        str(args.timeout),
    ]
    if args.env_file is not None:
        command.extend(["--env-file", str(args.env_file)])
    return command


def prerequisites(case, result, args, failures):
    if case.blocked_reason:
        result.update(status="blocked", category="unproved_candidate", error=case.blocked_reason)
        return False
    if case.lane in ("fork", "faults", "backtest"):
        try:
            require_providers(case, args.env_file)
        except ValueError as error:
            result.update(status="blocked", category="missing_provider_env", error=str(error))
            return False
    if (case.input_policy, case.lane) == ("fresh", "fork"):
        if any(c in failures for c in case.chains):
            result.update(
                status="blocked",
                category="source_resolution",
                error={str(c): failures[c] for c in case.chains if c in failures},
            )
            return False
    return True


def retain_case(result, args, directory):
    secrets = secrets_for(args.env_file, directory)
    errors = export_evidence(directory, args.output / "public" / result["artifact"], secrets)
    if errors:
        result.update(status="blocked", category="unsafe_evidence", omitted=errors)


def record_failure(result, directory, error):
    result.update(status="failed", category="case_failed", error=str(error))
    worker = directory / "worker.json"
    if worker.is_file() and json.loads(worker.read_text())["status"] == "blocked":
        result.update(status="blocked", category="timeout")


def execute(case, result, args, now, failures):
    if not prerequisites(case, result, args, failures):
        return
    directory = args.output / "private" / result["artifact"]
    started = time.monotonic()
    try:
        run(
            worker_command(case, directory, args, now),
            directory,
            "worker",
            timeout=args.timeout,
            grace=10,
        )
        result.update(status="passed", category="")
    except (OSError, ValueError, RuntimeError) as error:
        record_failure(result, directory, error)
    except KeyboardInterrupt:
        result.update(
            status="blocked", category="interrupted", error="case interrupted by operator"
        )
        raise
    finally:
        result["wall_seconds"] = round(time.monotonic() - started, 3)
        retain_case(result, args, directory)


def resolve_paths(args):
    """Absolute output, provider env and frozen-input paths; the optional ones may be absent."""
    args.output = args.output.resolve()
    for name in ("env_file", "frozen_inputs"):
        value = getattr(args, name)
        setattr(args, name, value.resolve() if value else None)


def initialize(args):
    resolve_paths(args)
    args.output.mkdir(parents=True, exist_ok=False)
    os.chmod(args.output, 0o700)
    cases = catalog()
    expected = validate(cases)
    selected = select(cases, args)
    if not selected:
        raise ValueError("selection contains no QA cases")
    started = utc_now()
    metadata = {
        "identity": identity(),
        "started_at": started,
        "input_time": started,
        "required_obligations": [asdict(o) for o in sorted(expected, key=repr)],
        "proof_mapping": proof_mapping(cases, expected),
        "selected_cases": [c.id for c in selected],
        "execution": "plan" if args.plan else "run",
        "provider_case_concurrency": 1,
    }
    rows = [{**row(case), "artifact": f"cases/{index:03d}"} for index, case in enumerate(cases)]
    (args.output / "public").mkdir()
    return cases, selected, metadata, rows


def proof_mapping(cases, expected):
    mapped: dict[Obligation, list[str]] = {obligation: [] for obligation in expected}
    for case in cases:
        for obligation in case.obligations() & expected:
            mapped[obligation].append(case.id)
    return [
        {"obligation": asdict(obligation), "case_ids": sorted(mapped[obligation])}
        for obligation in sorted(expected, key=repr)
    ]


def prepare_run(args, metadata, selected, secrets):
    metadata["tools"] = tool_versions()
    source = args.output / "source.tar.gz"
    snapshot(metadata["identity"], source, secrets)
    source.replace(args.output / "public/source.tar.gz")
    origins, failures = run_origins(args, metadata, selected)
    metadata["origins"] = {str(k): v for k, v in origins.items()}
    metadata["source_failures"] = {str(k): v for k, v in failures.items()}
    return failures


def run_origins(args, metadata, selected):
    """Resolve fresh origins here, or reuse the ones a hosted matrix froze before fan-out."""
    if args.frozen_inputs is None:
        return resolve_origins(selected, args.env_file, args.output / "private")
    input_time, origins, failures = frozen_origins(args.frozen_inputs, args.output / "private")
    metadata["input_time"] = input_time
    metadata["frozen_inputs_sha256"] = digest(args.frozen_inputs)
    return origins, failures


def invalidate_changed_sources(metadata, rows):
    if identity()["tree_sha256"] != metadata["identity"]["tree_sha256"]:
        metadata["source_changed"] = True
        for result in rows:
            if result["status"] == "passed":
                result.update(status="blocked", category="source_changed")


def finish(args, metadata, rows, secrets):
    metadata["finished_at"] = utc_now()
    invalidate_changed_sources(metadata, rows)
    previous = json.loads(args.previous.read_text()) if args.previous else {}
    write_json(args.output / "public/changes.json", changes(rows, previous))
    save_results(args.output / "public", metadata, rows, secrets)


def selected_failed(cases, rows, selected):
    return any(r["status"] != "passed" for c, r in zip(cases, rows, strict=True) if c in selected)


def main():
    args = arguments()
    cases, selected, metadata, rows = initialize(args)
    secrets = secrets_for(args.env_file, args.output)
    save_results(args.output / "public", metadata, rows, secrets)
    if args.plan:
        print(
            f"Validated {len(metadata['required_obligations'])} obligations in {len(cases)} cases."
        )
        print(args.output / "public/summary.md")
        return 0
    try:
        failures = prepare_run(args, metadata, selected, secrets)
        run_cases(cases, selected, rows, args, metadata, failures, secrets)
    finally:
        finish(args, metadata, rows, secrets)
    return int(selected_failed(cases, rows, selected))


def run_cases(cases, selected, rows, args, metadata, failures, secrets):
    for case, result in zip(cases, rows, strict=True):
        if case not in selected:
            result.update(status="skipped", category="not_selected")
        else:
            print(f"QA start: {case.id}", flush=True)
            execute(case, result, args, metadata["input_time"], failures)
            print(f"QA {result['status']}: {case.id}", flush=True)
        save_results(args.output / "public", metadata, rows, secrets)


if __name__ == "__main__":
    raise SystemExit(main())
