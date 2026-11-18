"""Check every reference strategy offline against the connector support matrix.

Every strategy passes when `almanak check` validates its public author inputs. A candidate
strategy on a chain that one of its connectors does not publicly support must
be refused by exactly that unsupported-chain error; any other outcome fails.
"""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
COMPOSED_REFERENCES: frozenset[str] = frozenset(
    {
        "arbitrum_aave_borrow_and_trade",
        "arbitrum_aave_borrow_and_trade_gmx",
        "arbitrum_aave_borrow_and_trade_v4",
        "arbitrum_aave_borrow_lp",
        "arbitrum_aave_curve_borrow_and_trade",
        "arbitrum_aave_lending_short",
        "arbitrum_aave_leverage_loop",
        "arbitrum_aave_pancake_borrow_and_trade",
        "arbitrum_compound_borrow_and_trade",
        "arbitrum_morpho_borrow_and_trade",
        "avalanche_aave_borrow_and_trade",
        "avalanche_aave_borrow_and_trade_gmx",
        "avalanche_aave_borrow_and_trade_v3",
        "avalanche_aave_v4_borrow_and_trade",
        "avalanche_morpho_borrow_and_trade_v4",
        "base_aave_aerodrome_stable_borrow_and_trade",
        "base_aave_aerodrome_volatile_borrow_and_trade",
        "base_aave_curve_borrow_and_trade",
        "base_aave_pancake_borrow_and_trade",
        "base_aave_slipstream_borrow_and_trade",
        "base_borrow_and_trade",
        "base_borrow_and_trade_v4",
        "base_compound_borrow_and_trade",
        "base_morpho_leverage_loop",
        "base_morpho_borrow_and_trade",
        "bnb_aave_borrow_and_trade",
        "bnb_aave_borrow_and_trade_v3",
        "bnb_aave_borrow_and_trade_v4",
        "bnb_aave_pancake_borrow_and_trade",
        "bnb_morpho_curve_borrow_and_trade",
        "ethereum_aave_pancake_borrow_and_trade",
        "ethereum_aave_v4_borrow_and_trade",
        "ethereum_base_borrow_and_trade",
        "ethereum_compound_curve_borrow_and_trade",
        "ethereum_morpho_curve_borrow_and_trade",
        "ethereum_spark_curve_borrow_and_trade",
        "optimism_aave_borrow_and_trade",
        "optimism_aave_borrow_and_trade_v4",
        "optimism_compound_curve_borrow_and_trade",
        "optimism_morpho_curve_borrow_and_trade",
        "polygon_aave_borrow_and_trade",
        "polygon_aave_borrow_and_trade_v3",
        "polygon_aave_borrow_and_trade_v4",
        "polygon_morpho_curve_borrow_and_trade",
        "robinhood_morpho_borrow_and_trade_v3",
        "robinhood_morpho_borrow_and_trade_v4",
        "robinhood_morpho_curve_borrow_and_trade",
    }
)
CONVERTED: frozenset[str] = frozenset(
    path.parent.name for path in (ROOT / "strategies").glob("*/strategy.py")
)
REFUSAL = re.compile(r"connector does not support (eip155:\d+)$")


def named_connectors(config):
    """Connector names a strategy configuration binds, top-level or per market."""
    document = yaml.safe_load(config.read_text(encoding="utf-8")) or {}
    names = []
    strategy = document.get("strategy") or {}
    if isinstance(strategy, dict) and strategy.get("connector"):
        names.append(str(strategy["connector"]))
    markets = document.get("markets") or {}
    if isinstance(markets, dict):
        for market in markets.values():
            if isinstance(market, dict) and market.get("connector"):
                names.append(str(market["connector"]))
    return sorted(set(names))


def support_matrix():
    from almanak.reports import support

    return {name: {entry["chain"] for entry in entries} for name, entries in support().items()}


def check_command(directory):
    entry = "from almanak.cli import main; raise SystemExit(main())"
    return [sys.executable, "-c", entry, "check", str(directory), "--json"]


def run_check(directory, timeout=300):
    """Run one check per process; the report is on stdout when valid and on stderr when not."""
    try:
        result = subprocess.run(
            check_command(directory),
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as error:
        raise ValueError(f"almanak check exceeded {timeout} seconds for {directory}") from error
    for stream in (result.stdout, result.stderr):
        try:
            return json.loads(stream)
        except json.JSONDecodeError:
            continue
    raise ValueError(f"almanak check produced no JSON report for {directory}: {result.stderr}")


def classify(report, connectors, matrix):
    """Return ("valid" | "refused" | "failed", detail)."""
    if report.get("valid"):
        return "valid", ""
    message = str(report.get("message", ""))
    refusal = REFUSAL.search(message)
    if refusal is None:
        return "failed", message
    chain = refusal.group(1)
    unsupported = [name for name in connectors if chain not in matrix.get(name, set())]
    if not connectors or not unsupported:
        return "failed", f"{message}; connectors {connectors or 'unknown'} declare {chain}"
    return "refused", f"{chain} not declared by {unsupported}"


def main(argv=None, root=ROOT, matrix=None, check=run_check):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, help="append a Markdown summary to this file")
    args = parser.parse_args(argv)
    matrix = support_matrix() if matrix is None else matrix
    rows = []
    for directory in sorted(p for p in (root / "strategies").iterdir() if p.is_dir()):
        if not (directory / "strategy.py").is_file():
            continue
        try:
            outcome, detail = classify(
                check(directory), named_connectors(directory / "config.yaml"), matrix
            )
        except (OSError, ValueError, yaml.YAMLError) as error:
            outcome, detail = "failed", str(error)
        rows.append((directory.name, outcome, detail))
        print(f"{outcome:8} {directory.name}  {detail}")
    counts = {
        name: sum(1 for row in rows if row[1] == name) for name in ("valid", "refused", "failed")
    }
    lines = [
        "## Reference strategies checked offline",
        "",
        f"{counts['valid']} valid, {counts['refused']} refused by the public support matrix, "
        f"{counts['failed']} failed.",
        "",
        *(
            f"- `{name}`: {outcome} {detail}".rstrip()
            for name, outcome, detail in rows
            if outcome != "valid"
        ),
    ]
    if args.summary is not None:
        with args.summary.open("a", encoding="utf-8") as summary:
            summary.write("\n".join(lines) + "\n")
    print(lines[2])
    for name, outcome, detail in rows:
        if outcome == "failed":
            print(f"::error::strategy {name} failed check: {detail}")
    return int(counts["failed"] > 0 or not rows)


if __name__ == "__main__":
    sys.exit(main())
