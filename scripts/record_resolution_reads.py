"""Record the retained reads of public reference resolutions as an offline fixture.

The resolution tests replay `{"block", "evidence"}` fixtures through
`RetainedResolutionReader`. This tool resolves one or more converted references
through the read gateway with their fork operator examples, merges the retained
read evidence at their shared anchor and writes the fixture, refusing any row
that carries provider text.
"""

import argparse
import gzip
import json
import sys
import tempfile
from pathlib import Path

from tests.strategy_fork.adapter import resolve_reference

ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN = ("alchemy", "api_key", "http")


def main(argv=None) -> int:
    args = arguments(argv)
    fixture = record(args.reference, args.operators, args.env_file)
    write_fixture(args.output, fixture)
    print(json.dumps({"output": str(args.output), "rows": len(fixture["evidence"])}))
    return 0


def arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reference", action="append", required=True)
    parser.add_argument("--operators", type=Path, default=ROOT / "examples/operators")
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args(argv)


def record(references, operators: Path, env_file: Path) -> dict:
    blocks, rows = [], []
    with tempfile.TemporaryDirectory(prefix="resolution-reads-") as temporary:
        for name in references:
            config = ROOT / "strategies" / name / "config.yaml"
            operator = operators / f"{name}.fork.yaml"
            result = resolve_reference(config, operator, env_file, Path(temporary) / name).result
            for market in result.markets:
                blocks.append(market.resolved.block)
                rows.extend(market.evidence)
    return {"block": shared_block(blocks).model_dump(mode="json"), "evidence": merged(rows)}


def shared_block(blocks):
    if not blocks or any(block != blocks[0] for block in blocks):
        raise ValueError("recorded references must resolve at one shared anchor")
    return blocks[0]


def merged(rows) -> list[dict]:
    """One row per distinct read; the same read must answer the same at one anchor."""
    seen: dict[str, dict] = {}
    for row in rows:
        record = row.model_dump(mode="json")
        key = json.dumps([record["method"], record["params"]], sort_keys=True)
        if key in seen and seen[key]["result"] != record["result"]:
            raise ValueError(f"retained reads disagree at the same anchor: {record['method']}")
        seen.setdefault(key, record)
    return list(seen.values())


def write_fixture(output: Path, fixture: dict):
    text = json.dumps(fixture, sort_keys=True)
    lowered = text.lower()
    if any(token in lowered for token in FORBIDDEN):
        raise ValueError("recorded reads carry provider text; nothing was written")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(gzip.compress(text.encode()))


if __name__ == "__main__":
    sys.exit(main())
