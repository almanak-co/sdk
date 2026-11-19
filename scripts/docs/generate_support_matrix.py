"""Generate docs/support.md from the declared connector and chain support.

The source is the same structure `almanak support --json` prints, followed by the
strategy actions each connector binds to `ctx.actions`. Output is gitignored and
rebuilt by `make docs-support`.
"""

import argparse
import json
import sys
from pathlib import Path

try:
    from scripts.docs import author_actions
except ModuleNotFoundError:  # run as a file: python scripts/docs/generate_support_matrix.py
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from scripts.docs import author_actions

ROOT = Path(__file__).resolve().parents[2]
HEADER = "# Supported connectors and chains"


def load_support(path):
    if path is None:
        from almanak.reports import support

        return support()
    return json.loads(Path(path).read_text(encoding="utf-8"))


def joined(values, formatter=str):
    return ", ".join(formatter(v) for v in values) if values else "none"


def historical_cell(entries):
    return joined(entries, lambda h: f"`{h['model']}` ({h['mode']})")


def matrix(support):
    chains = sorted({row["chain"] for rows in support.values() for row in rows})
    lines = ["| Connector | " + " | ".join(f"`{c}`" for c in chains) + " |"]
    lines.append("|---|" + "---|" * len(chains))
    for name in sorted(support):
        by_chain = {row["chain"]: row for row in support[name]}
        cells = [
            joined(by_chain[c]["modes"]) if c in by_chain else ""  # empty means undeclared
            for c in chains
        ]
        lines.append(f"| `{name}` | " + " | ".join(cells) + " |")
    return lines


def connector_section(name, rows):
    lines = [f"## {name}", ""]
    lines.append(
        "| Chain | Modes | Actions | Direct actions | Account profiles | Restrictions"
        " | Backtest models | Variants |"
    )
    lines.append("|---|---|---|---|---|---|---|---|")
    for row in sorted(rows, key=lambda r: r["chain"]):
        lines.append(
            f"| `{row['chain']}` | {joined(row['modes'])} | {joined(row['actions'])} | "
            f"{joined(row.get('direct_actions', []))} | "
            f"{joined(row['account_profiles'])} | {joined(row['restrictions'])} | "
            f"{historical_cell(row.get('historical', []))} | {joined(row.get('variants', []))} |"
        )
    lines.append("")
    lifecycle = next((row["gas_lifecycle"] for row in rows if row.get("gas_lifecycle")), None)
    return lines + (gas_lifecycle_section(lifecycle) if lifecycle else [])


def increments(counts, prefix=""):
    added = [f"+{n} per item of `{name}`" for name, n in counts[prefix + "per_item"].items()]
    added += [
        f"+{n} when `{name}` is granted" for name, n in counts[prefix + "when_granted"].items()
    ]
    return joined(added)


def gas_lifecycle_section(lifecycle):
    """Entry and exit transaction counts per allocation kind, the same on every chain."""
    lines = [
        "| Allocation kind | Entry | Each further entry slice | Entry increments | Exit | "
        "Exit increments | Each unwind cycle |",
        "|---|---|---|---|---|---|---|",
    ]
    for kind, counts in lifecycle.items():
        lines.append(
            f"| `{kind}` | {counts['entry']} | {counts['per_entry_slice'] or 'none'} | "
            f"{increments(counts)} | {counts['exit']} | {increments(counts, 'exit_')} | "
            f"{counts['exit_per_unwind_cycle'] or 'none'} |"
        )
    return [*lines, ""]


def render(support, actions=None):
    if not support:
        raise ValueError("no connector declares chain support")
    lines = [
        HEADER,
        "",
        "Connectors advertise immutable per-chain support for execution modes, economic",
        "actions, account profiles and historical coverage. A declaration does not",
        "establish provider availability, wallet funding or installed Safe authority.",
        "",
        *matrix(support),
        "",
    ]
    for name in sorted(support):
        lines += connector_section(name, support[name])
        if actions and name in actions:
            lines += [
                f"The `ctx.actions` methods for `{name}` and what each does are listed",
                f'under "{name} actions" in Strategy actions below.',
                "",
            ]
    if actions:
        lines += author_actions.render(actions)
    source = "`almanak support --json`"
    if actions:
        source += " and the connector action bindings"
    lines += [f"Generated from {source}; do not edit by hand.", ""]
    return "\n".join(lines)


def main(argv=None):
    arguments = argparse.ArgumentParser(description=__doc__)
    arguments.add_argument("--output", type=Path, default=ROOT / "docs/support.md")
    arguments.add_argument("--input", type=Path, help="support JSON instead of the package")
    args = arguments.parse_args(argv)
    actions = None if args.input else author_actions.collect()
    text = render(load_support(args.input), actions)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(text, encoding="utf-8")
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
