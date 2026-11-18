"""Export a prepared HyperCore backtest dataset as an offline test fixture (D4).

Reads a completed dataset directory from `almanak backtest prepare` and writes, gzipped,
every `/info` answer the dataset retains, in the recordings format of
`tests/fixtures/hyperliquid-mainnet-info.json.gz`, together with the market's retained
resolution reads and facts. The answers are the dataset's own evidence bodies, byte for
byte, so the offline tests rebuild the same records the preparation committed.

Reads only: nothing here contacts the venue or a provider.
"""

import argparse
import gzip
import json
import sqlite3
from pathlib import Path


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    fixture = {
        "recordings": recordings(args.dataset / "data.sqlite"),
        "resolution": resolution(args.dataset / "resolution.json"),
    }
    body = json.dumps(fixture, sort_keys=True).encode()
    args.output.write_bytes(gzip.compress(body, mtime=0))
    print(json.dumps({"output": str(args.output), "recordings": len(fixture["recordings"])}))
    return 0


def recordings(database: Path) -> list[dict]:
    with sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True) as db:
        rows = db.execute("SELECT body FROM evidence ORDER BY digest").fetchall()
    return [label(json.loads(body)) for (body,) in rows]


def label(result: dict) -> dict:
    info = result["request"]["info"]
    return {"label": f"{info['type']}.{info.get('coin') or info['req']['coin']}", **result}


def resolution(path: Path) -> dict:
    (market,) = json.loads(path.read_text())["result"]["markets"]
    return {"facts": market["resolved"], "evidence": market["evidence"]}


if __name__ == "__main__":
    raise SystemExit(main())
