"""Capture shipped YAML and effective configurations offline before author API changes.

This is an inventory, not lifecycle or readiness evidence. Failed normalization
is retained, including candidate inputs refused by the installed support matrix.
Strategy hooks, providers, signing and submission are never invoked.
"""

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
from collections import Counter
from pathlib import Path

from almanak.configuration.unresolved import parse_inputs, public_root
from almanak.configuration.yaml import load
from almanak.reports import support
from scripts.legacy_configuration.author import read_author
from scripts.legacy_configuration.backtest import read_backtest

ROOT = Path(__file__).resolve().parents[1]


def digest(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def leaf_paths(value, prefix=""):
    """Keep empty and null inputs in the ledger; absence and zero are distinct."""
    if isinstance(value, dict) and value:
        return [p for key, item in value.items() for p in leaf_paths(item, f"{prefix}.{key}")]
    if isinstance(value, list) and value:
        return [p for i, item in enumerate(value) for p in leaf_paths(item, f"{prefix}.{i}")]
    return [prefix.removeprefix(".")]


def effective(path):
    """Harness roots record their normalized form; public roots record their parsed inputs."""
    try:
        if public_root(load(path.with_name("config.yaml")).value):
            return parsed(path)
        if path.name.startswith("backtest"):
            model = read_backtest(path, read_author(path.with_name("config.yaml")))
        else:
            model = read_author(path)
        value = model.model_dump(mode="json")
        return {"status": "normalized", "sha256": digest(canonical(value)), "value": value}
    except (ValueError, KeyError) as error:
        return {"status": "failed", "message": str(error)}


def parsed(path):
    if path.name.startswith("backtest"):
        from almanak.configuration.backtest_inputs import read_backtest_inputs

        return parsed_record("backtest-inputs-v1", read_backtest_inputs(path).inputs)
    return parsed_record("author-inputs-v1", parse_inputs(load(path)).inputs)


def parsed_record(codec, model):
    value = model.model_dump(mode="json")
    return {"status": "parsed", "codec": codec, "sha256": digest(canonical(value)), "value": value}


def capture_file(path, root):
    before = path.read_bytes()
    document = load(path)
    normalized = effective(path)
    if before != path.read_bytes():
        raise ValueError(f"input changed during capture: {path}")
    return {
        "path": str(path.relative_to(root)),
        "sha256": digest(before),
        "lines": len(before.splitlines()),
        "schema_hint": "yaml-language-server:" in before.decode(),
        "fields": sorted(leaf_paths(document.value)),
        "authored": document.value,
        "effective": normalized,
    }


def capture(root, revision, declarations):
    directories = sorted(path.parent for path in (root / "strategies").glob("*/strategy.py"))
    if not directories:
        raise ValueError("no reference strategies discovered")
    references = []
    for directory in directories:
        if not (directory / "config.yaml").is_file():
            raise ValueError(f"reference has no config.yaml: {directory}")
        files = sorted(path for path in directory.iterdir() if path.suffix in {".yaml", ".yml"})
        references.append(
            {
                "strategy": str((directory / "strategy.py").relative_to(root)),
                "strategy_sha256": digest((directory / "strategy.py").read_bytes()),
                "files": [capture_file(path, root) for path in files],
            }
        )
    outcomes = Counter(f["effective"]["status"] for r in references for f in r["files"])
    return {
        "schema": "almanak.config-baseline.v1",
        "source_revision": revision,
        "collector_sha256": digest(Path(__file__).read_bytes()),
        "claims": "Offline normalization only; no hooks, providers or lifecycle proofs.",
        "support_sha256": digest(canonical(declarations)),
        "support": declarations,
        "normalization_counts": dict(outcomes),
        "references": references,
    }


def write_capture(path, value):
    """Publish a complete capture atomically without replacing retained evidence."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as temporary:
        name = Path(temporary.name)
        try:
            temporary.write(canonical(value) + b"\n")
            temporary.flush()
            os.fsync(temporary.fileno())
            os.link(name, path)
        finally:
            name.unlink()


def source_revision(root):
    paths = ("src", "strategies", "docs/schemas", "uv.lock", "pyproject.toml")
    result = subprocess.run(
        ["git", "status", "--porcelain", "--", *paths],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    if result.stdout:
        raise ValueError("commit configuration/runtime changes before capturing a baseline")
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="new ignored evidence JSON file")
    args = parser.parse_args(argv)
    revision = source_revision(ROOT)
    snapshot = capture(ROOT, revision, support())
    if source_revision(ROOT) != revision:
        raise ValueError("source revision changed during capture")
    write_capture(args.output, snapshot)
    print(
        json.dumps({"output": str(args.output), "normalization": snapshot["normalization_counts"]})
    )


if __name__ == "__main__":
    main()
