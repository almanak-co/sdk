"""Compare hook captures of one reference before and after its author-root conversion.

Both captures come from scripts/capture_author_hooks.py: the harness capture with
explicit account bindings, and the public capture over a retained resolution with
the same fund addresses. Economic choices, resolved targets, capabilities and data
dependencies must be identical; everything else is reported, never judged.

The split is generic over every connector. A position carries its resolved market
under a named sub-object (reserve, pool, contracts, market, and the GMX-style keys
whose names end in market or pool); everything else the position carries is an
economic choice.
"""

import argparse
import json
from pathlib import Path

from scripts.capture_config_baseline import canonical, digest, write_capture

TARGETS = ("reserve", "pool", "contracts", "market")
TARGET_SUFFIXES = ("market", "pool")
BINDINGS = ("name", "account")


def is_target(key, value):
    """Resolved markets are sub-objects named for the venue they point at."""
    return isinstance(value, dict) and (key in TARGETS or key.endswith(TARGET_SUFFIXES))


def positions(parameters):
    return {p["name"]: p for p in (*parameters["allocations"], *parameters.get("debts", ()))}


def economic_keys(position):
    return {
        key: value
        for key, value in position.items()
        if key not in BINDINGS and not is_target(key, value)
    }


def target_keys(position):
    return {key: value for key, value in position.items() if is_target(key, value)}


def economics(parameters):
    return {
        "decision_trigger": parameters["decision_trigger"],
        "gas": parameters.get("gas", []),
        "inventory": parameters.get("inventory", []),
        "positions": {
            name: economic_keys(position) for name, position in positions(parameters).items()
        },
    }


def targets(parameters):
    return {
        "positions": {
            name: target_keys(position) for name, position in positions(parameters).items()
        },
        "decimals": {
            asset: metadata["decimals"]
            for asset, metadata in parameters.get("asset_metadata", {}).items()
        },
    }


def normalized_accounts(capture):
    """Fund addresses become their account names so bindings compare by role."""
    names = {
        binding["profile"]["fund"]["address"].lower(): f"<fund:{binding['name']}>"
        for binding in capture["bound_parameters"]["accounts"]["bindings"]
    }
    text = json.dumps(capture["capabilities"], sort_keys=True)
    for address, name in names.items():
        text = text.replace(address, name).replace(address.upper(), name)
    return json.loads(text)


def differences(before, after, prefix=""):
    if isinstance(before, dict) and isinstance(after, dict):
        keys = sorted(set(before) | set(after))
        return [
            d
            for key in keys
            for d in differences(before.get(key), after.get(key), f"{prefix}.{key}")
        ]
    if before != after:
        return [{"path": prefix.removeprefix("."), "before": before, "after": after}]
    return []


def compare(before, after):
    groups = {
        "economics": (
            economics(before["bound_parameters"]),
            economics(after["bound_parameters"]),
        ),
        "targets": (targets(before["bound_parameters"]), targets(after["bound_parameters"])),
        "capabilities": (normalized_accounts(before), normalized_accounts(after)),
        "dependencies": (before["dependencies"], after["dependencies"]),
    }
    result = {}
    for name, (left, right) in groups.items():
        found = differences(left, right)
        result[name] = {"equal": not found, "differences": found}
    return {
        "schema": "almanak.reference-conversion-comparison.v1",
        "before_sha256": digest(canonical(before)),
        "after_sha256": digest(canonical(after)),
        "before_revision": before["source_revision"],
        "after_revision": after["source_revision"],
        "equal": all(group["equal"] for group in result.values()),
        "groups": result,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True, help="harness hook capture")
    parser.add_argument("--after", type=Path, required=True, help="public root hook capture")
    parser.add_argument("--output", type=Path, required=True, help="new comparison JSON file")
    args = parser.parse_args(argv)
    result = compare(
        json.loads(args.before.read_text()),
        json.loads(args.after.read_text()),
    )
    write_capture(args.output, result)
    print(json.dumps({"output": str(args.output), "equal": result["equal"]}))
    return 0 if result["equal"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
