"""Plan the hosted QA matrix for one connector/chain cell, one job per entry.

A cell's cases are the catalog cases whose proof obligations name that connector
and CAIP-2 chain. The plan keeps every one of them visible: cases in the selected
hosted lanes become job entries, and the rest are listed with the reason they do
not run here. The reverse-fault nodes are the reversal tests under `tests/fork`
parametrized by one of the cell's references; they are not catalog cases, so a
job runs each one through pytest. The plan never widens a selection silently: an
empty cell, an unknown case or a lane that cannot pass on hosted runners refuses.

`freeze` resolves the fresh fork origins and the input time once, before the
fan-out, so every job of one run forks the same block per chain.
"""

import argparse
import importlib
import json
import re
import subprocess
import sys
from pathlib import Path

from almanak.caip import ChainId
from almanak.connectors.identifiers import ConnectorId
from tests.qa.catalog import ROOT, catalog
from tests.qa.inputs import freeze_inputs, preparation_timeout

HOSTED_LANES = ("fork", "backtest", "pytest")
DEFAULT_LANES = ("fork", "backtest")
LOCAL_LANES = {
    "offline": "the full offline gate needs the retained evidence tree and runs locally",
    "faults": "this case runs every connector's fork faults; the cell's reverse-fault "
    "nodes run as their own entries instead",
    "blocked": "an unpromoted candidate is refused before provider work and cannot pass",
}
# One case bound, as the local bench default. Backtests add the local runbook's
# two-hour bound for slow windows on top of the policy's cold-preparation budget.
CASE_SECONDS = 3600
BACKTEST_SECONDS = 7200
# Checkout, locked sync, Bubblewrap, Foundry, the source snapshot and the upload.
SETUP_MINUTES = 30
NODE_MINUTES = 150
# GitHub-hosted jobs stop at six hours; keep the bench bound inside that limit.
MAX_JOB_MINUTES = 355
MAX_ENTRIES = 256
REVERSAL_MODULES = "tests/fork/test_*reversal*.py"
NODE = re.compile(
    r"^tests/(fork|strategy_fork)/test_[a-z0-9_]+\.py(::[A-Za-z_][A-Za-z0-9_]*)+"
    r"(\[[A-Za-z0-9_.:-]+\])?$"
)
PARAMETER = re.compile(r"\[([^\]]+)\]$")


def split(value):
    return [item.strip() for item in re.split(r"[,\n]", value or "") if item.strip()]


def cell_cases(cases, connector, chain):
    """The cases whose declared obligations include this connector on this chain."""
    selected = [
        case
        for case in cases
        if any(o.connector == connector and o.chain == chain for o in case.obligations())
    ]
    if not selected:
        raise SystemExit(f"no QA case proves {connector} on {chain}")
    return selected


def explicit_cases(cases, ids):
    known = {case.id: case for case in cases}
    unknown = sorted(set(ids) - set(known))
    if unknown:
        raise SystemExit(f"unknown case IDs: {unknown}")
    return [known[case_id] for case_id in dict.fromkeys(ids)]


def lanes_of(value):
    lanes = tuple(split(value)) or DEFAULT_LANES
    local = sorted(set(lanes) & set(LOCAL_LANES))
    if local:
        raise SystemExit(f"lanes that cannot pass on hosted runners: {local}")
    unknown = sorted(set(lanes) - set(HOSTED_LANES))
    if unknown:
        raise SystemExit(f"unknown lanes: {unknown}")
    return lanes


def partition(cases, lanes):
    hosted, excluded = [], []
    for case in cases:
        if case.lane in LOCAL_LANES:
            excluded.append({"id": case.id, "lane": case.lane, "reason": LOCAL_LANES[case.lane]})
        elif case.lane not in lanes:
            excluded.append({"id": case.id, "lane": case.lane, "reason": "lane not selected"})
        else:
            hosted.append(case)
    return hosted, excluded


def bench_seconds(case):
    if case.lane != "backtest":
        return CASE_SECONDS
    preparation = preparation_timeout(case) if case.model_mode == "economic" else 0
    return BACKTEST_SECONDS + preparation


def job_minutes(seconds):
    minutes = -(-seconds // 60) + SETUP_MINUTES
    if minutes > MAX_JOB_MINUTES:
        raise SystemExit(f"a {seconds} s case cannot finish inside the hosted job limit")
    return minutes


def case_entry(case):
    seconds = bench_seconds(case)
    return {
        "id": case.id,
        "kind": "case",
        "lane": case.lane,
        "reference": case.reference,
        "bench_timeout": seconds,
        "timeout_minutes": job_minutes(seconds),
    }


def node_entry(node, reference):
    return {
        "id": node,
        "kind": "node",
        "lane": "faults",
        "reference": reference,
        "bench_timeout": 0,
        "timeout_minutes": NODE_MINUTES,
    }


def node_reference(node):
    """A parametrized reversal names its reference; a single-reference module declares it."""
    parameter = PARAMETER.search(node)
    if parameter is not None:
        return parameter.group(1)
    module = node.split("::", 1)[0].removesuffix(".py").replace("/", ".")
    return getattr(importlib.import_module(module), "REFERENCE", "")


def collect_reversal_nodes():
    modules = sorted(str(path.relative_to(ROOT)) for path in ROOT.glob(REVERSAL_MODULES))
    output = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-q", "-o", "addopts="]
        + ["-p", "no:cacheprovider", *modules],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    nodes = [line for line in output.splitlines() if NODE.match(line)]
    return [(node, node_reference(node)) for node in nodes]


def reversal_nodes(references, collect):
    return [(node, reference) for node, reference in collect() if reference in references]


def validated_nodes(value):
    # A repeated explicit node would fan out twice and report two rows for one test.
    nodes = list(dict.fromkeys(split(value)))
    invalid = [node for node in nodes if not NODE.match(node)]
    if invalid:
        raise SystemExit(
            f"node IDs must name tests under tests/fork or tests/strategy_fork: {invalid}"
        )
    return nodes


def selection(args, cases):
    """The selected cases and the cell they prove, from either selection form."""
    if bool(args.cases) == bool(args.connector or args.chain):
        raise SystemExit("select either a cell (connector and chain) or explicit case IDs")
    if args.cases:
        return explicit_cases(cases, split(args.cases)), None
    if not (args.connector and args.chain):
        raise SystemExit("a cell needs both a connector and a CAIP-2 chain")
    try:
        cell = {"connector": ConnectorId(args.connector), "chain": ChainId(args.chain)}
    except ValueError as error:
        raise SystemExit(f"invalid cell identity: {error}") from error
    return cell_cases(cases, cell["connector"], cell["chain"]), cell


def reverse_fault(args, cell, selected, collect):
    if cell is None or not args.reverse_fault:
        return {"requested": False, "nodes": []}
    references = {case.reference for case in selected if case.reference}
    return {
        "requested": True,
        "nodes": reversal_nodes(references, collect or collect_reversal_nodes),
    }


def plan(args, cases=None, collect=None):
    cases = catalog() if cases is None else cases
    selected, cell = selection(args, cases)
    lanes = lanes_of(args.lanes)
    hosted, excluded = partition(selected, lanes)
    faults = reverse_fault(args, cell, selected, collect)
    entries = [case_entry(case) for case in hosted]
    entries += [node_entry(node, reference) for node, reference in faults["nodes"]]
    discovered = {node for node, _ in faults["nodes"]}
    entries += [node_entry(n, "") for n in validated_nodes(args.nodes) if n not in discovered]
    if not entries:
        raise SystemExit("the selection leaves no entry to run on hosted runners")
    if len(entries) > MAX_ENTRIES:
        raise SystemExit(f"{len(entries)} entries exceed the {MAX_ENTRIES}-job matrix limit")
    for index, entry in enumerate(entries):
        entry.update(index=index, artifact=f"cell-qa-entry-{index:03d}")
    return {
        "schema_version": 1,
        "commit": args.commit,
        "cell": {k: str(v) for k, v in cell.items()} if cell else None,
        "references": sorted({case.reference for case in selected if case.reference}),
        "lanes": list(lanes),
        "reverse_fault": {
            "requested": faults["requested"],
            "nodes": sorted(discovered),
        },
        "entries": entries,
        "excluded": excluded,
    }


def freeze(plan_path, env_file, directory):
    ids = {e["id"] for e in json.loads(plan_path.read_text())["entries"] if e["kind"] == "case"}
    return freeze_inputs([c for c in catalog() if c.id in ids], env_file, directory)


def arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    selecting = commands.add_parser("plan", help="write the plan and print the job matrix")
    selecting.add_argument("--connector", default="")
    selecting.add_argument("--chain", default="")
    selecting.add_argument("--cases", default="", help="comma- or newline-separated case IDs")
    selecting.add_argument("--lanes", default=",".join(DEFAULT_LANES))
    selecting.add_argument("--reverse-fault", choices=("true", "false"), default="true")
    selecting.add_argument("--nodes", default="", help="additional fork pytest node IDs")
    selecting.add_argument("--commit", required=True)
    selecting.add_argument("--output", type=Path, required=True)
    freezing = commands.add_parser("freeze", help="resolve fresh origins once for every job")
    freezing.add_argument("--plan", type=Path, required=True)
    freezing.add_argument("--env-file", type=Path, required=True)
    freezing.add_argument("--directory", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.command == "plan":
        args.reverse_fault = args.reverse_fault == "true"
    return args


def main(argv):
    args = arguments(argv)
    if args.command == "freeze":
        freeze(args.plan, args.env_file, args.directory)
        return 0
    value = plan(args)
    if value["reverse_fault"]["requested"] and not value["reverse_fault"]["nodes"]:
        print("::warning::no reverse-fault node exercises this cell's references", file=sys.stderr)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    print(json.dumps(value["entries"], separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
