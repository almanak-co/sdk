"""Collect measured Safe/Roles gas references from retained fork run journals.

Each successful outcome of a fork run journal carries its mined receipt. This tool
reads the `gasUsed` of every receipt, groups the receipts by connector, chain,
account profile and operation kind, and keeps the largest one with its provenance
(run label, transaction hash and block). The result merges into the reviewed data
that `almanak check` compares with `strategy.gas` `max_gas_limit`:

    uv run --locked python -m scripts.gas_references \
        --run bbands=/path/to/run --run gmxma=/path/to/other/run \
        --output src/almanak/connectors/gas_references.json

Existing rows stay unless a new receipt for the same key used more gas. With
`--estimate`, a run whose fork node still answers also records `gas_estimate`: the
same call estimated at the receipt's parent block. Execution sizes the gas limit
from the estimate, which exceeds `gasUsed` when the call earns a storage refund
(a GMX order creation estimates about 8% above its receipt).
"""

import argparse
import json
from pathlib import Path

import httpx

from almanak.check_warnings import measured_gas
from almanak.connectors.composition import CONNECTOR_IDS
from almanak.reports import checkpoint

SCHEMA = "almanak-gas-references-v1"
CONNECTORS = frozenset(map(str, CONNECTOR_IDS))


def main(argv=None) -> int:
    args = arguments(argv)
    existing = read_references(args.output)
    measured = [row for label, path in args.run for row in journal_rows(label, path, args.estimate)]
    merged = merge(existing, measured)
    write_references(args.output, merged)
    print(json.dumps({"output": str(args.output), "references": len(merged)}))
    return 0


def arguments(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="append", type=labelled, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--estimate", action="store_true", help="re-estimate each call on the run's live fork"
    )
    return parser.parse_args(argv)


def labelled(value: str) -> tuple[str, Path]:
    label, separator, path = value.partition("=")
    if not separator or not label or not path:
        raise argparse.ArgumentTypeError("expected LABEL=RUN_DIRECTORY")
    return label, Path(path)


def read_references(path: Path) -> list[dict]:
    if not path.exists():
        return []
    document = json.loads(path.read_text())
    if document.get("schema") != SCHEMA:
        raise ValueError(f"{path} is not an {SCHEMA} document")
    return list(document["references"])


def write_references(path: Path, references: list[dict]) -> None:
    document = {
        "schema": SCHEMA,
        "description": (
            "Largest of gasUsed and the parent estimate per connector, chain, account profile"
            " and operation, from mined fork receipts of supervised runs. Regenerate with"
            " scripts/gas_references.py."
        ),
        "references": references,
    }
    path.write_text(json.dumps(document, indent=2) + "\n")


def journal_rows(label: str, run: Path, estimate: bool = False) -> list[dict]:
    body = checkpoint(run)
    if body is None:
        return []
    rows = [
        reference
        for outcome in body.get("outcomes", ())
        if (reference := outcome_reference(label, outcome)) is not None
    ]
    if estimate:
        endpoints = fork_endpoints(run)
        for reference in rows:
            endpoint = endpoints.get(reference["chain"])
            if endpoint is None:
                continue
            try:
                reference["gas_estimate"] = parent_estimate(endpoint, reference)
            except (httpx.HTTPError, RuntimeError, KeyError):
                # The fork node may be gone or pruned; the receipt's gasUsed still stands.
                continue
    return rows


def fork_endpoints(run: Path) -> dict[str, str]:
    processes = json.loads((run / "workspace.json").read_text())["processes"]
    return {
        name.removeprefix("fork-"): entry["endpoint"]
        for name, entry in processes.items()
        if name.startswith("fork-")
    }


def parent_estimate(endpoint: str, reference: dict) -> int:
    from almanak.gateway.fork_fixture import Rpc

    rpc = Rpc(endpoint)
    tx = rpc("eth_getTransactionByHash", [reference["source"]["transaction"]])
    call = {"from": tx["from"], "to": tx["to"], "data": tx["input"], "value": tx["value"]}
    return int(rpc("eth_estimateGas", [call, hex(reference["source"]["block"] - 1)]), 16)


def outcome_reference(label: str, outcome: dict) -> dict | None:
    receipt = outcome.get("receipt") or {}
    operation = outcome.get("operation") or {}
    execution = outcome.get("execution") or {}
    if not outcome.get("success") or not receipt.get("gasUsed"):
        return None
    connector = target_connector(operation.get("target"))
    profile = ((execution.get("binding") or {}).get("profile") or {}).get("kind")
    if connector is None or profile is None:
        return None
    fund = (operation.get("target") or {}).get("fund") or {}
    variant = "native" if native_pool(operation.get("target")) else None
    return {
        **({"variant": variant} if variant else {}),
        "connector": connector,
        "chain": fund["chain"]["chain_id"],
        "profile": profile,
        "action": operation["kind"],
        "gas_used": int(receipt["gasUsed"], 16),
        "source": {
            "kind": "fork_receipt",
            "run": label,
            "transaction": receipt["transactionHash"],
            "block": int(receipt["blockNumber"], 16),
            "block_timestamp": quantity(receipt.get("blockTimestamp")),
        },
    }


def native_pool(target) -> bool:
    """A pool whose PoolKey names the chain's native coin, which the fund holds wrapped.

    Its liquidity actions add the PositionManager's unwrap and wrap, so they are measured
    apart from the ERC-20 pools' and bound only strategies that use a native pool.
    """
    pool = (target or {}).get("pool") or {}
    return any(
        ":slip44:" in f":{pool.get(c, '')}".replace("/", ":") for c in ("currency0", "currency1")
    )


def quantity(value) -> int | None:
    """A receipt field that some nodes report as a hex string and others as a number."""
    if value is None:
        return None
    return int(value, 16) if isinstance(value, str) else int(value)


def target_connector(target) -> str | None:
    """The connector kind of the first nested market, pool or reserve of a target."""
    if not isinstance(target, dict):
        return None
    for value in target.values():
        if isinstance(value, dict) and value.get("kind") in CONNECTORS:
            return value["kind"]
    return None


def merge(existing: list[dict], measured: list[dict]) -> list[dict]:
    selected: dict[tuple, dict] = {}
    for reference in (*existing, *measured):
        key = (
            reference["connector"],
            reference["chain"],
            reference["profile"],
            reference["action"],
            reference.get("variant", ""),
        )
        if key not in selected or measured_gas(reference) > measured_gas(selected[key]):
            selected[key] = reference
    return [selected[key] for key in sorted(selected)]


if __name__ == "__main__":
    raise SystemExit(main())
