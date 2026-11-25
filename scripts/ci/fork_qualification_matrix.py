"""Emit the hosted fork qualification matrix for named references.

Each entry names a converted reference, whether the production EOA rehearsal
applies to it, and whether it is still UNREHEARSED. A reference whose connectors
declare no production support on its chains, on the checked-out revision, only
qualifies through the Safe/Roles fork lifecycle, and the workflow must not report
a rehearsal it never ran. An already rehearsed reference is in the default
rehearsal selection, so the workflow passes no qualification override for it.

Each entry also lists the exact pytest node IDs the workflow runs. Node IDs,
unlike `-k` substring expressions, cannot select a reference whose name ends
with the requested one, such as `ethereum_base_borrow_and_trade` for
`base_borrow_and_trade`, and pytest refuses a node ID it does not collect. An
entry with no node IDs has nothing to run, and its workflow job fails.

`--production-cells` replaces the named references for the weekly scheduled
run: every converted reference with a market on a connector/chain cell that
the checked-out connectors declare for production. The selection refuses when
no cell is declared or when a declared cell has no reference to exercise it, so
a scheduled run can never pass on an empty or partial selection.

Keeper-settled GMX references are selected like any other: their fork drivers
settle each request with authentic signed reports read from the pinned evidence
copies (tests/fixtures/evidence.json), which the workflow fetches before pytest.
"""

import argparse
import json
import re
import sys
from pathlib import Path

from almanak.configuration.unresolved import read_inputs
from almanak.configuration.yaml import ConfigurationError
from scripts.ci.check_strategies import CONVERTED, ROOT, classify, named_connectors
from tests.strategy_fork.rehearsal import UNREHEARSED, declares_production

NAME = re.compile(r"^[a-z0-9_]+$")
LIFECYCLE_TEST = "tests/strategy_fork/test_strategies.py::test_strategy_lifecycle"
OPERATOR_MODES = ("configured", "distinct")
REHEARSAL_TEST = (
    "tests/strategy_fork/test_production_rehearsals.py"
    "::test_production_reference_path_on_identified_fork"
)

Cell = tuple[str, str]
# Production cells whose venue Anvil cannot run: HyperCore on HyperEVM is verified against
# a synthesized model, which is never Fork Qualification evidence (blueprint 30, D7), so its
# own fork rehearsal stands in and the weekly run leaves the cell out.
MODEL_VERIFIED: frozenset[Cell] = frozenset({("hyperliquid", "eip155:999")})


def node_ids(reference: str, lifecycle: bool, rehearsal: bool) -> list[str]:
    nodes = [f"{LIFECYCLE_TEST}[{mode}-{reference}]" for mode in OPERATOR_MODES if lifecycle]
    return nodes + ([f"{REHEARSAL_TEST}[{reference}]"] if rehearsal else [])


def matrix(names: list[str], rehearsal: bool, lifecycle: bool) -> list[dict[str, object]]:
    selected = sorted(set(names))
    unknown = [name for name in selected if not NAME.match(name) or name not in CONVERTED]
    if unknown:
        raise SystemExit(f"not converted references under strategies/: {unknown}")
    for name in selected:
        try:
            read_inputs(ROOT / "strategies" / name / "config.yaml")
        except ConfigurationError as error:
            raise SystemExit(f"{name} does not load on this revision: {error}") from error
    entries = []
    for name in selected:
        rehearses = rehearsal and declares_production(name)
        entries.append(
            {
                "reference": name,
                "rehearsal": rehearses,
                "qualify": name in UNREHEARSED,
                "nodes": node_ids(name, lifecycle, rehearses),
            }
        )
    return entries


def support_rows() -> dict[str, list[dict]]:
    """The public support matrix of the checked-out connectors, as `almanak support` reports it."""
    from almanak.reports import support

    return support()


def declared_without_proof() -> frozenset[Cell]:
    """Across cells declared on mainnet known answers and offline tests alone.

    An exception accepted for Across only: no reference sends from these chains, so the
    weekly run has nothing to rehearse there and leaves them out by name, never by an
    empty selection. Each such cell says so in `almanak support`.
    """
    from almanak.connectors.across.support import declared_without_proof as chains

    return frozenset(("across", str(chain)) for chain in chains())


def production_cells(rows: dict[str, list[dict]]) -> set[Cell]:
    return (
        {
            (name, row["chain"])
            for name, entries in rows.items()
            for row in entries
            if "production" in row["modes"]
        }
        - MODEL_VERIFIED
        - declared_without_proof()
    )


def reference_scope(config: Path, rows: dict[str, list[dict]]) -> set[Cell]:
    """The connector/chain cells a reference's markets bind; empty for a refused candidate.

    Only a candidate refused because one of its connectors does not declare its
    chain is empty, as `check_strategies` classifies it; any other load failure
    stops the selection rather than silently dropping a reference.
    """
    try:
        inputs = read_inputs(config).inputs
    except ConfigurationError as error:
        declared = {name: {row["chain"] for row in entries} for name, entries in rows.items()}
        outcome, _ = classify({"message": str(error)}, named_connectors(config), declared)
        if outcome == "refused":
            return set()
        raise SystemExit(f"{config.parent.name} does not load on this revision: {error}") from error
    return {(str(market.connector), str(market.chain)) for market in inputs.markets.values()}


def production_selection(cells: set[Cell], scopes: dict[str, set[Cell]]) -> dict[Cell, list[str]]:
    """Each production cell with the references that exercise it; a cell without one refuses."""
    if not cells:
        raise SystemExit("no connector declares production support on this revision")
    covering = {
        cell: sorted(name for name, scope in scopes.items() if cell in scope)
        for cell in sorted(cells)
    }
    uncovered = [
        f"{connector} on {chain}" for (connector, chain), names in covering.items() if not names
    ]
    if uncovered:
        raise SystemExit(f"production cells no converted reference exercises: {uncovered}")
    return covering


def production_references(root: Path = ROOT) -> dict[Cell, list[str]]:
    rows = support_rows()
    scopes = {
        name: reference_scope(root / "strategies" / name / "config.yaml", rows)
        for name in sorted(CONVERTED)
    }
    return production_selection(production_cells(rows), scopes)


def summary(covering: dict[Cell, list[str]]) -> str:
    names = sorted({name for names in covering.values() for name in names})
    lines = [
        "## Weekly fork qualification selection",
        "",
        f"{len(covering)} connector/chain cells declare production support on this revision; "
        f"{len(names)} references exercise them.",
        "",
        "| Connector | Chain | References |",
        "| --- | --- | --- |",
        *(
            f"| `{connector}` | `{chain}` | {', '.join(f'`{n}`' for n in names)} |"
            for (connector, chain), names in covering.items()
        ),
    ]
    return "\n".join(lines) + "\n"


def selected_names(arguments) -> list[str]:
    names = [name.strip() for name in arguments.references.split(",") if name.strip()]
    if arguments.production_cells:
        if names:
            raise SystemExit("name references or select --production-cells, not both")
        covering = production_references()
        if arguments.summary is not None:
            with arguments.summary.open("a", encoding="utf-8") as output:
                output.write(summary(covering))
        names = sorted({name for names in covering.values() for name in names})
    if not names:
        raise SystemExit("no references selected")
    return names


def main(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "references", nargs="?", default="", help="comma-separated strategy directory names"
    )
    parser.add_argument("--rehearsal", action="store_true", help="request the production rehearsal")
    parser.add_argument("--lifecycle", action="store_true", help="request the fork lifecycle")
    parser.add_argument(
        "--production-cells",
        action="store_true",
        help="select every reference on a cell with declared production support",
    )
    parser.add_argument("--summary", type=Path, help="append the selection as Markdown here")
    arguments = parser.parse_args(argv)
    entries = matrix(selected_names(arguments), arguments.rehearsal, arguments.lifecycle)
    for entry in entries:
        if arguments.rehearsal and not entry["rehearsal"]:
            outcome = (
                "only the fork lifecycle runs for it"
                if entry["nodes"]
                else "the lifecycle is off, so no case is selected and its job fails"
            )
            print(
                f"::notice::{entry['reference']} declares no production EOA support here; "
                f"{outcome}",
                file=sys.stderr,
            )
    print(json.dumps(entries))


if __name__ == "__main__":
    main(sys.argv[1:])
