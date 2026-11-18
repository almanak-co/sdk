"""Render the per-obligation proof ledger from retained evidence manifests.

The ledger is derived from local evidence packages, not from one bench run. A
retained pass on an older source does not satisfy acceptance on current source
and nothing here promotes support; it only reports which scheduled QA case ids
have an explicit passing record somewhere in the retained evidence tree.
"""

import argparse
import gzip
import json
import re
import subprocess
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from functools import cache
from itertools import combinations
from pathlib import Path
from typing import Any

from scripts.qa_bench import identity, proof_mapping
from tests.qa.catalog import catalog, validate
from tests.qa.inputs import write_json
from tests.qa.results import ledger_entries

# Only these explicit values count as a pass. Everything else, including absent
# fields, is not a pass; file presence never establishes one.
PASSING_STATUSES = frozenset({"passed"})
COMPLETE_PHASES = frozenset(
    {
        "closed",
        "single_borrow_reconciled_delivered_and_closed",
        "replaced_runtime_and_gateway_revoked_without_economic_exposure",
        "replaced_runtime_and_gateway_revoked_without_trading",
    }
)
POLICIES = ("pinned", "fresh", "native")
RECORD_LIST_KEYS = ("cases", "results")
IDENTITY_KEYS = ("case", "id", "name", "directory", "path", "result_path", "checkpoint_path")
FILE_LISTING_KEYS = ("files", "artifacts", "records", "sources", "original_files")
ARTIFACT_KEYS = ("dataset", "partitions", "sdk_digest", "capabilities")
SOURCE_KEYS = (
    "baseline_commit",
    "base_commit",
    "source_commit",
    "actual_checkout_commit",
    "commit",
    "head",
    "base_revision",
    "source_revision",
    "tested_revision",
    "baseline_source_archive_sha256",
    "source_sha256",
    "fork_bundle_sha256",
    "source_hash",
)
NESTED_SOURCE_KEYS = (
    ("tested_source", "sha256"),
    ("source", "commit"),
    ("source", "sha256"),
    ("identity", "commit"),
)
SOURCE_ARCHIVES = ("tested-source.tar.gz", "source.tar.gz")
TIME_KEYS = ("recorded_at", "captured_at", "timestamp", "finished_at", "started_at")
RESULT_NAMES = ("result.json", "result.json.gz")
# The QA bench retains its complete matrix as results.json with rows keyed by id.
BENCH_RESULT_NAMES = ("results.json", "results.json.gz")
UNUSABLE = ("unreadable", "no_result", "incomplete", "unrecognised")
DATE = re.compile(r"^\d{4}-\d{2}-\d{2}")


@dataclass(frozen=True)
class Record:
    package: str
    recorded_id: str
    case_id: str | None
    reference: str
    verdict: str
    basis: str
    detail: str
    path: str
    source_key: str | None
    source_values: tuple[str, ...]
    captured: str | None


@dataclass
class Package:
    path: str
    kind: str
    source: dict[str, Any] | None
    captured: dict[str, str | None]
    records: list[Record]
    unrecognised_records: int
    error: str = ""


@dataclass(frozen=True)
class Catalog:
    case_ids: frozenset[str]
    references: frozenset[str]


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True, help="retained evidence root")
    parser.add_argument("--output", type=Path, required=True, help="directory for the ledger")
    args = parser.parse_args()
    if not args.evidence.is_dir():
        parser.error(f"evidence root is not a directory: {args.evidence}")
    return args


def load_json(path: Path) -> Any:
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as source:
            return json.load(source)
    return json.loads(path.read_text(encoding="utf-8"))


def plain(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def verdict(record: dict[str, Any]) -> tuple[str, str, str]:
    code = record.get("returncode")
    status = record.get("status", record.get("result"))
    if status is not None and not isinstance(status, str):
        return ("unrecognised", "status", f"status is {type(status).__name__}, not text")
    if isinstance(code, int) and not isinstance(code, bool):
        passed = code == 0 and (status is None or status in PASSING_STATUSES)
        detail = f"returncode={code}" + (f" status={status}" if status is not None else "")
        return ("passed" if passed else "failed", "returncode", detail)
    if isinstance(status, str):
        return ("passed" if status in PASSING_STATUSES else "failed", "status", status)
    phases = record.get("phases")
    if isinstance(phases, list):
        last = str(phases[-1]) if phases else ""
        outcome = "passed" if last in COMPLETE_PHASES else "incomplete"
        return (outcome, "phases", last or "no phases")
    return ("no_result", "none", "")


@cache
def suffix_index(ids: frozenset[str]) -> dict[str, str | None]:
    # A policy-prefixed suffix identifies a case only when exactly one scheduled
    # id ends with it; None marks suffixes shared by several lanes.
    index: dict[str, str | None] = {}
    for case_id in ids:
        parts = case_id.split("/")
        for start in range(1, len(parts)):
            if parts[start] in POLICIES:
                suffix = "/".join(parts[start:])
                index[suffix] = None if suffix in index else case_id
    return index


def translate(segment: str, ids: frozenset[str]) -> str | None:
    tokens = segment.split("-")
    candidates = ["/".join(tokens)]
    if len(tokens) >= 4 and tokens[1] in POLICIES:
        candidates.append(f"{tokens[1]}/{tokens[0]}/" + "/".join(tokens[2:]))
    for candidate in candidates:
        if candidate in ids:
            return candidate
        if len(tokens) >= 3 and suffix_index(ids).get(candidate):
            return suffix_index(ids)[candidate]
    return None


def resolve_case_id(candidates: list[str], ids: frozenset[str]) -> str | None:
    for candidate in candidates:
        if candidate in ids:
            return candidate
        for segment in candidate.split("/"):
            found = translate(segment, ids)
            if found:
                return found
    return None


def identity_candidates(record: dict[str, Any]) -> list[str]:
    return [record[key] for key in IDENTITY_KEYS if isinstance(record.get(key), str)]


def reference_of(record: dict[str, Any], candidates: list[str], references: frozenset[str]):
    for key in ("reference", "strategy"):
        if isinstance(record.get(key), str):
            return record[key]
    # Only a discovered strategy name counts; file names and chain labels do not.
    for candidate in candidates:
        for segment in candidate.split("/"):
            for token in segment.split("-"):
                if token in references:
                    return token
    return ""


def record_lists(manifest: dict[str, Any]) -> list[list[Any]]:
    found: list[list[Any]] = []
    for key in RECORD_LIST_KEYS:
        value = manifest.get(key)
        if isinstance(value, dict):
            found.extend(v for v in value.values() if isinstance(v, list))
        elif isinstance(value, list):
            found.append(value)
    return [items for items in found if any(isinstance(item, dict) for item in items)]


def result_file(package_dir: Path, relative: str) -> Path | None:
    # Packages list result.json while the retained copy may be gzip compressed.
    stem = relative.removesuffix(".gz")
    for name in (relative, stem, stem + ".gz"):
        if (package_dir / name).is_file():
            return package_dir / name
    return None


def result_file_verdict(package_dir: Path, relative: str) -> tuple[str, str, str, dict[str, Any]]:
    path = result_file(package_dir, relative)
    if path is None:
        return ("unreadable", "result_file", "missing result file", {})
    try:
        content = load_json(path)
    except (OSError, ValueError, EOFError) as error:
        return ("unreadable", "result_file", f"invalid result file: {error}", {})
    if not isinstance(content, dict):
        return ("unreadable", "result_file", "result file is not an object", {})
    outcome, basis, detail = verdict(content)
    if basis == "none":
        return ("no_result", "result_file", "result file carries no status", content)
    return (outcome, "result_file", f"{basis}: {detail}", content)


def build_record(
    package: Package,
    record: dict[str, Any],
    package_dir: Path,
    scheduled: Catalog,
    source: dict[str, Any] | None = None,
) -> Record:
    candidates = identity_candidates(record)
    outcome, basis, detail = verdict(record)
    content: dict[str, Any] = {}
    if basis == "none" and isinstance(record.get("result_path"), str):
        outcome, basis, detail, content = result_file_verdict(package_dir, record["result_path"])
    if isinstance(content.get("case"), str):
        candidates.insert(0, content["case"])
    source = package.source if source is None else source
    return Record(
        package=package.path,
        recorded_id=candidates[0] if candidates else "",
        case_id=resolve_case_id(candidates, scheduled.case_ids),
        reference=reference_of({**content, **record}, candidates, scheduled.references),
        verdict=outcome,
        basis=basis,
        detail=detail,
        path=str(
            record.get("result_path") or record.get("directory") or record.get("artifact") or ""
        ),
        source_key=source["key"] if source else None,
        source_values=tuple(source["values"]) if source else (),
        captured=package.captured["value"],
    )


def manifest_records(
    package: Package,
    manifest: dict[str, Any],
    package_dir: Path,
    scheduled: Catalog,
    source: dict[str, Any] | None = None,
) -> None:
    for items in record_lists(manifest):
        for item in items:
            if isinstance(item, dict) and identity_candidates(item):
                package.records.append(build_record(package, item, package_dir, scheduled, source))
            else:
                package.unrecognised_records += 1


def bench_records(package: Package, relative: str, package_dir: Path, scheduled: Catalog):
    # A retained bench matrix carries its own source identity and case rows;
    # worker batches retain the bare row list without an identity.
    path = result_file(package_dir, relative)
    try:
        content = load_json(path) if path else None
    except (OSError, ValueError, EOFError):
        content = None
    if isinstance(content, list):
        content = {"cases": content}
    if not isinstance(content, dict) or not record_lists(content):
        package.unrecognised_records += 1
        return
    source = source_identity(content, {}) or package.source
    manifest_records(package, content, package_dir, scheduled, source)


def listed_files(manifest: dict[str, Any]) -> dict[str, str | None]:
    files: dict[str, str | None] = {}
    for key in FILE_LISTING_KEYS:
        value = manifest.get(key)
        if isinstance(value, dict):
            files.update((k, digest_of(v)) for k, v in value.items() if isinstance(k, str))
        elif isinstance(value, list):
            for item in value:
                name = item.get("path", item.get("file")) if isinstance(item, dict) else None
                if isinstance(name, str):
                    files[name] = digest_of(item)
    files.update(
        (k, v["sha256"]) for k, v in manifest.items() if isinstance(v, dict) and "sha256" in v
    )
    return files


def digest_of(value: Any) -> str | None:
    if isinstance(value, str) and len(value) == 64:
        return value
    if isinstance(value, dict) and isinstance(value.get("sha256"), str):
        return value["sha256"]
    return None


def result_file_records(
    package: Package, files: dict[str, str | None], package_dir: Path, scheduled: Catalog
) -> None:
    for relative in sorted(files):
        name = relative.split("/")[-1]
        if name not in RESULT_NAMES + BENCH_RESULT_NAMES or nested_package(package_dir, relative):
            continue
        if name in BENCH_RESULT_NAMES:
            bench_records(package, relative, package_dir, scheduled)
            continue
        outcome, basis, detail, content = result_file_verdict(package_dir, relative)
        candidates = [relative]
        if isinstance(content.get("case"), str):
            candidates.insert(0, content["case"])
        package.records.append(
            Record(
                package=package.path,
                recorded_id=candidates[0],
                case_id=resolve_case_id(candidates, scheduled.case_ids),
                reference=reference_of(content, candidates, scheduled.references),
                verdict=outcome,
                basis=basis,
                detail=detail,
                path=relative,
                source_key=package.source["key"] if package.source else None,
                source_values=tuple(package.source["values"]) if package.source else (),
                captured=package.captured["value"],
            )
        )


def nested_package(package_dir: Path, relative: str) -> bool:
    # A directory with its own manifest is read as its own package; the parent
    # listing must not record the same result file a second time.
    parents = [p for p in Path(relative).parents if str(p) != "."]
    return any((package_dir / parent / "manifest.json").is_file() for parent in parents)


def source_identity(manifest: dict[str, Any], files: dict[str, str | None]):
    for key in SOURCE_KEYS:
        if isinstance(manifest.get(key), str) and manifest[key]:
            return {"key": key, "values": [manifest[key]]}
    for outer, inner in NESTED_SOURCE_KEYS:
        value = manifest.get(outer)
        if isinstance(value, dict) and isinstance(value.get(inner), str):
            return {"key": f"{outer}.{inner}", "values": [value[inner]]}
    archive = manifest.get("source_archive")
    if isinstance(archive, str):
        return {"key": "source_archive", "values": [archive]}
    hashes = sorted({v for k, v in files.items() if k.split("/")[-1] in SOURCE_ARCHIVES and v})
    if hashes:
        return {"key": "files:source_archive_sha256", "values": hashes}
    return None


def captured_at(manifest: dict[str, Any], relative: Path) -> dict[str, str | None]:
    for name in ("manifest", "results", "junit"):
        scope = manifest if name == "manifest" else manifest.get(name)
        if not isinstance(scope, dict):
            continue
        for key in TIME_KEYS:
            if isinstance(scope.get(key), str):
                return {"value": scope[key], "source": f"{name}:{key}"}
    match = DATE.match(relative.parts[0]) if relative.parts else None
    if match:
        return {"value": match.group(), "source": "directory_name"}
    return {"value": None, "source": "unrecorded"}


def package_kind(manifest: dict[str, Any], declared: int, files: int, listing: bool) -> str:
    if declared:
        return "case_records"
    if files:
        return "result_files"
    if "scope" in manifest or "status" in manifest or listing:
        return "no_case_records"
    if any(key in manifest for key in ARTIFACT_KEYS):
        return "nested_artifact"
    return "unrecognised"


def read_package(path: Path, evidence: Path, scheduled: Catalog) -> Package:
    relative = path.relative_to(evidence)
    try:
        manifest = load_json(path)
    except (OSError, ValueError) as error:
        return Package(str(relative), "unrecognised", None, {}, [], 0, f"unreadable: {error}")
    if isinstance(manifest, list):
        manifest = {"files": manifest}
    if not isinstance(manifest, dict):
        return Package(str(relative), "unrecognised", None, {}, [], 0, "manifest is not an object")
    files = listed_files(manifest)
    package = Package(
        str(relative),
        "",
        source_identity(manifest, files),
        captured_at(manifest, relative),
        [],
        0,
    )
    manifest_records(package, manifest, path.parent, scheduled)
    declared = len(package.records)
    if not declared:
        result_file_records(package, files, path.parent, scheduled)
    package.kind = package_kind(manifest, declared, len(package.records), bool(files))
    return package


def read_packages(evidence: Path, scheduled: Catalog) -> list[Package]:
    paths = sorted(evidence.rglob("manifest.json"))
    return [read_package(path, evidence, scheduled) for path in paths]


def pass_entry(record: Record) -> dict[str, Any]:
    return {
        "package": record.package,
        "path": record.path,
        "captured": record.captured,
        "source": (
            {"key": record.source_key, "values": list(record.source_values)}
            if record.source_key
            else None
        ),
        "basis": record.basis,
        "detail": record.detail,
    }


def summarize_cases(records: list[Record]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[Record]] = defaultdict(list)
    for record in records:
        if record.case_id:
            grouped[record.case_id].append(record)
    summary = {}
    for case_id, items in sorted(grouped.items()):
        passes = sorted(
            (r for r in items if r.verdict == "passed"), key=lambda r: (r.captured or "", r.package)
        )
        by_key: dict[str, set[frozenset[str]]] = defaultdict(set)
        for record in passes:
            if record.source_key:
                by_key[record.source_key].add(frozenset(record.source_values))
        summary[case_id] = {
            "retained_pass": bool(passes),
            "newest_pass": pass_entry(passes[-1]) if passes else None,
            "passing_packages": sorted({r.package for r in passes}),
            "non_passing_records": Counter(r.verdict for r in items if r.verdict != "passed"),
            "source_identities": [
                {"key": key, "values": sorted(set().union(*values))}
                for key, values in sorted(by_key.items())
            ],
            "source_divergent": source_divergent(by_key),
        }
    return summary


def source_divergent(by_key: dict[str, set[frozenset[str]]]) -> bool:
    # Identities of different kinds (a commit against an archive digest) are
    # not comparable; within one kind, packages sharing any hash agree.
    return any(a.isdisjoint(b) for values in by_key.values() for a, b in combinations(values, 2))


def obligation_ledger(mapping: list[dict[str, Any]], cases: dict[str, Any]) -> list[dict[str, Any]]:
    ledger = ledger_entries(mapping, lambda i: bool(cases.get(i, {}).get("retained_pass")))
    for item in ledger:
        ids, outstanding = item["case_ids"], item["outstanding_case_ids"]
        if ids and not outstanding:
            item["status"] = "proven"
        else:
            item["status"] = "partial" if len(outstanding) < len(ids) else "none"
    return ledger


def totals(ledger: list[dict[str, Any]], key: str) -> dict[str, dict[str, int]]:
    counts: dict[str, Counter] = defaultdict(Counter)
    for entry in ledger:
        counts[str(entry["obligation"][key])][entry["status"]] += 1
    return {
        name: {status: counter[status] for status in ("proven", "partial", "none")}
        for name, counter in sorted(counts.items())
    }


def unattributed_passes(records: list[Record]) -> dict[str, Any]:
    grouped: dict[str, list[Record]] = defaultdict(list)
    unnamed = 0
    for record in records:
        if record.case_id is not None or record.verdict != "passed":
            continue
        if record.reference:
            grouped[record.reference].append(record)
        else:
            unnamed += 1
    return {
        "by_reference": {
            name: {"records": len(items), "packages": sorted({r.package for r in items})}
            for name, items in sorted(grouped.items())
        },
        "without_reference": unnamed,
    }


def blocked_candidates(cases) -> list[dict[str, Any]]:
    return [
        {
            "case_id": case.id,
            "reference": case.reference,
            "connector": str(case.connector) if case.connector else None,
            "chains": [str(chain) for chain in case.chains],
            "reason": case.blocked_reason,
        }
        for case in cases
        if case.lane == "blocked"
    ]


def catalog_identity() -> dict[str, str] | None:
    # The bench's identity helper resolves the commit and the working tree hash
    # so the ledger records whether the catalog came from a clean checkout.
    try:
        found = identity()
    except (OSError, subprocess.CalledProcessError):
        return None
    return {"commit": found["commit"], "tree_sha256": found["tree_sha256"]}


def package_summary(package: Package) -> dict[str, Any]:
    # Passing records are reachable through the case summaries; only records
    # that could not be used are listed here so reviewers can locate them.
    return {
        **asdict(package),
        "records": Counter(r.verdict for r in package.records),
        "unusable_records": [
            {"recorded_id": r.recorded_id, "verdict": r.verdict, "detail": r.detail}
            for r in package.records
            if r.verdict in UNUSABLE
        ],
    }


def summary(ledger, scheduled: Catalog, case_summary, packages, records) -> dict[str, Any]:
    with_pass = [case_id for case_id, c in case_summary.items() if c["retained_pass"]]
    return {
        "obligations": Counter(entry["status"] for entry in ledger),
        "scheduled_cases": len(scheduled.case_ids),
        "cases_with_retained_pass": len(with_pass),
        "cases_with_retained_pass_by_lane": Counter(c.split("/")[0] for c in with_pass),
        "scheduled_cases_by_lane": Counter(c.split("/")[0] for c in scheduled.case_ids),
        "source_divergent_cases": sorted(
            case_id for case_id, c in case_summary.items() if c["source_divergent"]
        ),
        "packages": Counter(package.kind for package in packages),
        "packages_with_unrecognised_records": sum(1 for p in packages if p.unrecognised_records),
        "records": Counter(record.verdict for record in records),
    }


def build(evidence: Path) -> dict[str, Any]:
    cases = catalog()
    expected = validate(cases)
    mapping = plain(proof_mapping(cases, expected))
    scheduled = Catalog(
        frozenset(case.id for case in cases), frozenset(c.reference for c in cases if c.reference)
    )
    packages = read_packages(evidence, scheduled)
    records = [record for package in packages for record in package.records]
    case_summary = summarize_cases(records)
    ledger = obligation_ledger(mapping, case_summary)
    return {
        "schema_version": 1,
        "derived": "from retained evidence manifests; not a bench run and not a promotion",
        "catalog_identity": catalog_identity(),
        "evidence_root": str(evidence),
        "rules": rules(),
        "summary": summary(ledger, scheduled, case_summary, packages, records),
        "totals": {key: totals(ledger, key) for key in ("connector", "chain", "mode")},
        "obligations": ledger,
        "cases": case_summary,
        "unattributed_passes": unattributed_passes(records),
        "blocked_candidates": blocked_candidates(cases),
        "packages": [package_summary(package) for package in packages],
    }


def rules() -> dict[str, Any]:
    return {
        "passing_statuses": sorted(PASSING_STATUSES),
        "complete_phases": sorted(COMPLETE_PHASES),
        "record_lists": list(RECORD_LIST_KEYS),
        "identity_keys": list(IDENTITY_KEYS),
        "source_keys": [*SOURCE_KEYS, *(".".join(k) for k in NESTED_SOURCE_KEYS)],
        "shapes": [
            "returncode: passes only when returncode == 0 and any status is passing",
            "status/result: passes only when the value is in passing_statuses",
            "phases: passes only when the last phase is in complete_phases",
            "result_path: the referenced result file supplies status or returncode",
            "result files listed by packages without case records supply status or "
            "returncode; the case id comes from the file or a path segment matching exactly "
            "one scheduled id; files under a nested manifest belong to that package",
            "results.json from the QA bench supplies rows keyed by id with their status and "
            "the run's identity.commit as source",
            "source identities are compared per identity key; packages sharing any hash "
            "agree, and a commit is never compared with an archive digest",
        ],
    }


def obligation_key(obligation: dict[str, Any]) -> tuple[str, ...]:
    return tuple(
        str(obligation[key]) if obligation[key] is not None else ""
        for key in ("connector", "chain", "mode", "profile", "model", "variant")
    )


def grouped_rows(ledger: list[dict[str, Any]], status: str) -> list[str]:
    grouped: dict[tuple[str, ...], list[str]] = defaultdict(list)
    for entry in ledger:
        if entry["status"] == status:
            grouped[obligation_key(entry["obligation"])].append(str(entry["obligation"]["action"]))
    rows = [
        "| Connector | Chain | Mode | Profile | Model | Variant | Actions |",
        "| --- " * 7 + "|",
    ]
    for key, actions in sorted(grouped.items()):
        rows.append("| " + " | ".join(key) + " | " + ", ".join(sorted(actions)) + " |")
    return rows


def totals_rows(table: dict[str, dict[str, int]], label: str) -> list[str]:
    rows = [f"| {label} | Proven | Partial | None |", "| --- | ---: | ---: | ---: |"]
    rows.extend(
        f"| {name} | {c['proven']} | {c['partial']} | {c['none']} |" for name, c in table.items()
    )
    return rows


def markdown_header(payload: dict[str, Any]) -> list[str]:
    summary = payload["summary"]
    obligations = summary["obligations"]
    return [
        "# Obligation proof ledger",
        "",
        "A retained pass on an older source does not satisfy acceptance on current",
        "source. This ledger is derived from retained evidence manifests; it is not a",
        "bench run, not a promotion and does not add supported scope. Acceptance still",
        "requires every mapped case to pass on frozen integrated source.",
        "",
        f"Catalog identity: `{payload['catalog_identity']}`.",
        f"Evidence root: `{payload['evidence_root']}`. Regenerate with",
        "`uv run --locked python -m scripts.proof_ledger --evidence <root> --output <dir>`.",
        "",
        "## Headline",
        "",
        f"- Obligations: {obligations.get('proven', 0)} proven, "
        f"{obligations.get('partial', 0)} partial, {obligations.get('none', 0)} none "
        f"of {sum(obligations.values())}.",
        f"- Scheduled case ids with a retained explicit pass: "
        f"{summary['cases_with_retained_pass']} of {summary['scheduled_cases']}; by lane "
        f"{dict(summary['cases_with_retained_pass_by_lane'])} of "
        f"{dict(summary['scheduled_cases_by_lane'])}. Lanes absent from the first set are "
        "never named by a retained manifest.",
        f"- Packages by kind: {dict(summary['packages'])}; packages with unrecognised "
        f"records: {summary['packages_with_unrecognised_records']}.",
        f"- Records by verdict: {dict(summary['records'])}.",
        f"- Case ids whose passes come from differing source identities: "
        f"{len(summary['source_divergent_cases'])}.",
        "",
        "## Rules",
        "",
        *(f"- {shape}" for shape in payload["rules"]["shapes"]),
        f"- Passing statuses: {payload['rules']['passing_statuses']}; complete phases: "
        f"{payload['rules']['complete_phases']}.",
        "- File presence, checkpoints, dashboards and package-level status strings never "
        "establish a case pass. Source identity is the package-level declaration; the "
        "capture date falls back to the package directory name.",
        "- Records without a scheduled case id (older captures naming only a reference,",
        "  profile and operator) are listed separately and satisfy no obligation.",
    ]


def markdown_sections(payload: dict[str, Any]) -> list[str]:
    lines = ["", "## Totals", ""]
    for key, label in (("connector", "Connector"), ("chain", "Chain"), ("mode", "Mode")):
        lines.extend([*totals_rows(payload["totals"][key], label), ""])
    ledger = payload["obligations"]
    lines.extend(["## Obligations with no retained pass", "", *grouped_rows(ledger, "none")])
    lines.extend(["", "## Partial obligations", "", *grouped_rows(ledger, "partial")])
    lines.extend(["", "## Source divergence", ""])
    divergent = payload["summary"]["source_divergent_cases"]
    lines.extend(
        [f"- `{case}`: {payload['cases'][case]['source_identities']}" for case in divergent]
        or ["- none"]
    )
    unattributed = payload["unattributed_passes"]
    lines.extend(["", "## Passing records without a scheduled case id", ""])
    lines.extend(
        f"- {name}: {item['records']} records in {len(item['packages'])} packages"
        for name, item in unattributed["by_reference"].items()
    )
    lines.append(f"- Records naming no discovered reference: {unattributed['without_reference']}")
    return lines


def markdown_packages(payload: dict[str, Any]) -> list[str]:
    lines = ["", "## Blocked candidates", ""]
    lines.extend(["| Case | Connector | Chains | Reason |", "| --- | --- | --- | --- |"])
    lines.extend(
        f"| {b['case_id']} | {b['connector'] or ''} | {', '.join(b['chains'])} | {b['reason']} |"
        for b in payload["blocked_candidates"]
    )
    lines.extend(["", "## Unrecognised packages", ""])
    lines.extend(
        [
            f"- `{p['path']}`: {p['error'] or 'no recognised keys'}"
            for p in payload["packages"]
            if p["kind"] == "unrecognised"
        ]
        or ["- none"]
    )
    lines.extend(["", "## Packages with unrecognised records", ""])
    lines.extend(
        [
            f"- `{p['path']}`: {p['unrecognised_records']} records without a case identity"
            for p in payload["packages"]
            if p["unrecognised_records"]
        ]
        or ["- none"]
    )
    return lines


def render_markdown(payload: dict[str, Any]) -> str:
    lines = [*markdown_header(payload), *markdown_sections(payload), *markdown_packages(payload)]
    return "\n".join(lines) + "\n"


def main() -> int:
    args = arguments()
    payload = plain(build(args.evidence))
    args.output.mkdir(parents=True, exist_ok=True)
    write_json(args.output / "proof-ledger.json", payload)
    (args.output / "proof-ledger.md").write_text(render_markdown(payload), encoding="utf-8")
    obligations = payload["summary"]["obligations"]
    print(
        f"Obligations: {obligations.get('proven', 0)} proven, {obligations.get('partial', 0)} "
        f"partial, {obligations.get('none', 0)} none; unrecognised packages: "
        f"{payload['summary']['packages'].get('unrecognised', 0)}."
    )
    print(args.output / "proof-ledger.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
