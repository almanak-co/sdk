"""Freeze the legacy effective configurations of every reference as committed fixtures.

The harness loader is deleted at the end of the conversion program, so the
values it produces before conversion are frozen here, grouped by connector, and
compared against the public inputs by tests/test_reference_conversion.py. This
reads a local baseline capture only; it never loads a strategy or a provider.
"""

import argparse
import gzip
import hashlib
import json
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASELINE = ROOT / "docs/internal/reports/evidence/config-baseline-ce86d32.json"
OUTPUT = ROOT / "tests/fixtures/author/legacy"
SCHEMA = "almanak.legacy-effective-snapshot.v1"
BASELINE_SCHEMA = "almanak.config-baseline.v1"
COMPOSED = "composed"


def canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def compress(value) -> bytes:
    """A fixed modification time keeps a re-run byte-identical to the committed file."""
    return gzip.compress(canonical(value), mtime=0)


def connector_of(files: dict) -> str:
    """The authored connector of the legacy root; a composed strategy names none."""
    strategy = files["config.yaml"]["authored"].get("strategy") or {}
    return str(strategy.get("connector") or COMPOSED)


def file_entry(record: dict) -> dict:
    """A refused candidate keeps its refusal; everything else keeps its effective value."""
    effective = record["effective"]
    entry = {"sha256": record["sha256"], "status": effective["status"]}
    if effective["status"] == "failed":
        entry["message"] = effective["message"]
        return entry
    entry["effective_sha256"] = effective["sha256"]
    entry["value"] = effective["value"]
    return entry


def reference_entry(reference: dict) -> tuple[str, str, dict]:
    files = {Path(record["path"]).name: record for record in reference["files"]}
    name = Path(reference["strategy"]).parent.name
    entry = {
        "name": name,
        "strategy_sha256": reference["strategy_sha256"],
        "files": {key: file_entry(record) for key, record in files.items()},
    }
    return connector_of(files), name, entry


def grouped(baseline: dict) -> dict[str, dict]:
    result: dict[str, dict] = defaultdict(dict)
    for reference in baseline["references"]:
        connector, name, entry = reference_entry(reference)
        result[connector][name] = entry
    return result


def read_baseline(path: Path) -> tuple[dict, str]:
    raw = path.read_bytes()
    baseline = json.loads(raw)
    if baseline.get("schema") != BASELINE_SCHEMA:
        raise ValueError(f"{path} is not a {BASELINE_SCHEMA} capture")
    return baseline, hashlib.sha256(raw).hexdigest()


def document_for(baseline: dict, baseline_sha256: str, references: dict) -> dict:
    return {
        "schema": SCHEMA,
        "source_revision": baseline["source_revision"],
        "baseline_sha256": baseline_sha256,
        "references": references,
    }


def extract(baseline_path: Path, output: Path) -> dict:
    baseline, baseline_sha256 = read_baseline(baseline_path)
    output.mkdir(parents=True, exist_ok=True)
    summary = {}
    for connector, references in sorted(grouped(baseline).items()):
        path = output / f"{connector}.json.gz"
        path.write_bytes(compress(document_for(baseline, baseline_sha256, references)))
        summary[connector] = {"references": len(references), "bytes": path.stat().st_size}
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "baseline", nargs="?", type=Path, default=BASELINE, help="config baseline capture to read"
    )
    parser.add_argument("--output", type=Path, default=OUTPUT, help="snapshot fixture directory")
    args = parser.parse_args(argv)
    print(json.dumps(extract(args.baseline, args.output), sort_keys=True))


if __name__ == "__main__":
    main()
