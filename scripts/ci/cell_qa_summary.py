"""Merge the hosted jobs of one cell QA matrix into a single results file and summary.

Every planned entry gets exactly one row. A job that left no record, a refused
evidence scan, a skipped test, a mixed source or input identity and a missing
reverse-fault node all keep the result incomplete: only every planned entry
passing on one commit yields `passed`. The summary also lists the cell's cases
this run did not attempt and the per-cell package gates that remain local work.
"""

import argparse
import json
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

from scripts.ci.cell_qa_evidence import identity, read_json, secret_values
from scripts.qa_bench import identity as bench_identity
from scripts.qa_bench import proof_mapping
from tests.qa.catalog import catalog, validate
from tests.qa.inputs import write_json
from tests.qa.results import proof_ledger
from tests.qa.source import snapshot

SOURCE_KEYS = ("commit", "tree_sha256", "lock_sha256")
RUNTIME_KEYS = ("python", "tools")
LOCAL_GATES = (
    "the full offline suite with the retained evidence tree, combined coverage, "
    "CRAP at most 30, Ruff, formatting and mypy on the same source",
    "the recovery-foundation nodes for the cell's connector, unless listed above as entries",
    "component proofs in the pytest lane that read retained evidence, listed above as not run",
    "append-only sanitized evidence archival with checksum verification",
    "the path-level renewal check when the integrated source differs from this commit",
)


def outcome(status, category, seconds=None):
    return {"status": status, "category": category, "wall_seconds": seconds}


def case_result(entry, public, expected):
    results = read_json(public / "results.json")
    if results is None:
        return outcome("failed", "no_result")
    if results.get("selected_cases") != [entry["id"]]:
        return outcome("blocked", "selection_mismatch")
    if results["identity"]["tree_sha256"] != expected["tree_sha256"]:
        return outcome("blocked", "source_identity_mismatch")
    frozen = expected["frozen"]
    if (
        results.get("input_time") != frozen["input_time"]
        or results.get("origins") != frozen["resolved"]
    ):
        return outcome("blocked", "input_mismatch")
    row = next(r for r in results["cases"] if r["id"] == entry["id"])
    return outcome(row["status"], row.get("category", ""), row["wall_seconds"])


def node_result(entry, public, expected):
    path = public / "node" / "junit.xml"
    if not path.is_file():
        return outcome("failed", "no_result")
    cases = ET.parse(path).getroot().findall(".//testcase")
    if len(cases) != 1:
        return outcome("failed", "collection")
    tags = {child.tag for child in cases[0]}
    seconds = round(float(cases[0].attrib.get("time", 0)), 3)
    if "skipped" in tags:
        return outcome("skipped", "pytest_skipped", seconds)
    if tags & {"failure", "error"}:
        return outcome("failed", "case_failed", seconds)
    return outcome("passed", "", seconds)


def identity_category(job, source, runtime):
    before, after = job.get("identity_before"), job.get("identity_after")
    if not before or not after:
        return "identity_missing"
    if before != after:
        return "source_changed"
    if any(before[key] != source[key] for key in SOURCE_KEYS):
        return "source_identity_mismatch"
    if any(before[key] != runtime[key] for key in RUNTIME_KEYS):
        return "runtime_mismatch"
    return ""


def entry_result(entry, directory, expected):
    job = read_json(directory / "job.json")
    if job is None:
        return outcome("failed", "no_result")
    if job["scan"] != "passed" or job["omitted"]:
        return outcome("blocked", "unsafe_evidence")
    evaluate = case_result if entry["kind"] == "case" else node_result
    result = evaluate(entry, directory / "public", expected)
    category = identity_category(job, expected, expected["runtime"])
    if category and result["status"] == "passed":
        result.update(status="blocked", category=category)
    return result


def entry_directory(directory, entry):
    """The entry's download directory.

    download-artifact extracts a pattern that matches a single artifact straight
    into the target path, so a one-entry plan finds its record there.
    """
    nested = directory / entry["artifact"]
    if nested.is_dir():
        return nested
    job = read_json(directory / "job.json")
    flat = job and {k: job["entry"][k] for k in ("id", "artifact")} == {
        "id": entry["id"],
        "artifact": entry["artifact"],
    }
    return directory if flat else nested


def runtime_identity(entries, directory):
    for entry in entries:
        job = read_json(entry_directory(directory, entry) / "job.json")
        if job and job.get("identity_before"):
            return {key: job["identity_before"][key] for key in RUNTIME_KEYS}
    return dict.fromkeys(RUNTIME_KEYS)


def rows(plan, directory, expected):
    return [
        {
            **{key: entry[key] for key in ("id", "kind", "lane", "reference", "artifact")},
            **entry_result(entry, entry_directory(directory, entry), expected),
        }
        for entry in plan["entries"]
    ]


def cell_ledger(cell, results, excluded):
    if cell is None:
        return None
    cases = catalog()
    mapping = [
        item
        for item in proof_mapping(cases, validate(cases))
        if str(item["obligation"]["connector"]) == cell["connector"]
        and str(item["obligation"]["chain"]) == cell["chain"]
    ]
    statuses = [*results, *({"id": e["id"], "status": "not_run"} for e in excluded)]
    return proof_ledger(mapping, statuses)


def problems_of(plan, results):
    problems = []
    if plan["reverse_fault"]["requested"] and not plan["reverse_fault"]["nodes"]:
        problems.append("no reverse-fault node exercises the cell's references")
    if not results:
        problems.append("the plan has no entries")
    return problems


def aggregate(plan, frozen, directory, source, run_url=""):
    expected = {**source, "frozen": frozen, "runtime": runtime_identity(plan["entries"], directory)}
    results = rows(plan, directory, expected)
    problems = problems_of(plan, results)
    passed = not problems and all(r["status"] == "passed" for r in results)
    ledger = cell_ledger(plan["cell"], results, plan["excluded"])
    return {
        "schema_version": 1,
        "execution": "hosted_cell_matrix",
        "status": "passed" if passed else "incomplete_or_failed",
        "cell": plan["cell"],
        "run_url": run_url,
        "source": {key: source[key] for key in SOURCE_KEYS},
        "runtime": expected["runtime"],
        "frozen_inputs": frozen,
        "lanes": plan["lanes"],
        "reverse_fault": plan["reverse_fault"],
        "problems": problems,
        "cases": results,
        "excluded": plan["excluded"],
        "cell_obligations": ledger,
        "local_gates_outstanding": list(LOCAL_GATES),
    }


def title(payload):
    cell = payload["cell"]
    if cell is None:
        return "## Hosted QA matrix: explicit selection"
    return f"## Hosted cell QA matrix: `{cell['connector']}` on `{cell['chain']}`"


def ledger_lines(ledger):
    if ledger is None:
        return []
    closed = sum(item["status"] == "passed" for item in ledger)
    lines = ["", f"### Cell obligations: {closed} of {len(ledger)} closed by this run", ""]
    for item in ledger:
        if item["status"] != "passed":
            obligation = item["obligation"]
            name = "/".join(
                str(obligation[k])
                for k in ("mode", "profile", "model", "action", "variant")
                if obligation[k]
            )
            lines.append(f"- `{name}` outstanding: {', '.join(item['outstanding_case_ids'])}")
    return lines


def excluded_lines(excluded):
    if not excluded:
        return []
    lines = ["", "### Cell cases not run by this workflow", "", "| Case | Lane | Reason |"]
    lines.append("| --- | --- | --- |")
    lines.extend(f"| `{e['id']}` | {e['lane']} | {e['reason']} |" for e in excluded)
    return lines


def summary(payload):
    source = payload["source"]
    lines = [
        title(payload),
        "",
        f"Result: **{payload['status']}**",
        "",
        f"Commit `{source['commit']}`, tree sha256 `{source['tree_sha256']}`, "
        f"lock sha256 `{source['lock_sha256']}`; "
        f"input time {payload['frozen_inputs']['input_time']}.",
        "Hosted runners, identified Anvil forks and read-only providers; no funds spent. "
        "A skipped entry is not a pass.",
        "",
        *(f"- Problem: {problem}" for problem in payload["problems"]),
        *([""] if payload["problems"] else []),
        "| Entry | Kind | Status | Category | Seconds |",
        "| --- | --- | --- | --- | ---: |",
    ]
    lines.extend(
        f"| `{r['id']}` | {r['kind']} | {r['status']} | {r['category']} | {r['wall_seconds']} |"
        for r in payload["cases"]
    )
    lines.extend(excluded_lines(payload["excluded"]))
    lines.extend(ledger_lines(payload["cell_obligations"]))
    lines.extend(["", "### Per-cell package gates this run does not establish", ""])
    lines.extend(f"- {gate}" for gate in payload["local_gates_outstanding"])
    return "\n".join(lines) + "\n"


def arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--frozen-inputs", type=Path, required=True)
    parser.add_argument("--entries", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--run-url", default="")
    parser.add_argument("--secret-env", action="append", default=[])
    return parser.parse_args(argv)


def main(argv):
    args = arguments(argv)
    plan = json.loads(args.plan.read_text())
    source = identity()
    if source["commit"] != plan["commit"]:
        raise SystemExit("the aggregate checkout differs from the planned commit")
    payload = aggregate(
        plan, json.loads(args.frozen_inputs.read_text()), args.entries, source, args.run_url
    )
    args.output.mkdir(parents=True, exist_ok=True)
    secrets = tuple(s.decode() for s in secret_values(args.secret_env, os.environ))
    snapshot(bench_identity(), args.output / "source.tar.gz", secrets)
    write_json(args.output / "results.json", payload)
    (args.output / "summary.md").write_text(summary(payload))
    return int(payload["status"] != "passed")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
