"""A test run's `run_metadata` line: what its amounts and venues read as.

Printed once when a test run's coordinator starts, before any decision: each asset the
resolved configuration verified, with its symbol and decimals as resolution recorded them
(a chain's native token carries decimals only), and each allocation's connector. A progress
view reads amounts and venues from it instead of re-reading the workspace's files.
"""

import json
import sys

EVENT = "run_metadata"


def log(state) -> None:
    """Print the metadata of the run whose journal state this is; neither a fact nor the
    write ever stops the runtime."""
    try:
        print(json.dumps(facts(state.config)), file=sys.stderr, flush=True)
    except Exception:  # noqa: BLE001 - diagnostics only
        return


def facts(config) -> dict:
    metadata = getattr(config, "asset_metadata", None) or {}
    assets = {}
    for asset, meta in metadata.items():
        known = {"symbol": meta.symbol, "decimals": meta.decimals}
        assets[str(asset)] = {key: value for key, value in known.items() if value is not None}
    venues = {}
    for allocation in getattr(config, "allocations", None) or ():
        connector = connector_of(allocation)
        if connector is not None:
            venues[allocation.name] = connector
    return {"event": EVENT, "assets": assets, "venues": venues}


def connector_of(allocation) -> str | None:
    """The connector whose allocation model this is (`almanak.connectors.<id>.models`)."""
    parts = type(allocation).__module__.split(".")
    if "connectors" not in parts or parts.index("connectors") + 1 >= len(parts):
        return None
    return parts[parts.index("connectors") + 1]
