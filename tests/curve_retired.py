"""Retained Curve captures predate scope-only Safe authority (blueprint 27).

Their allocations and capabilities still carry the per-coin `maximums` that sized Roles
quotas, and their permission partitions carry those quotas. Those values are archived
history: the current models load a capture without them, and every other retained fact
stays exactly as captured. A liquidity intent keeps its own `maximums`, the deposit
amounts it requests; intents are the only retained values that carry a `kind`.
"""

import json


def current(value):
    if isinstance(value, list):
        return [current(item) for item in value]
    if not isinstance(value, dict):
        return value
    retired = set() if "kind" in value else {"maximums"}
    if "grants" in value and "profile" in value:
        retired.add("quotas")
    return {name: current(item) for name, item in value.items() if name not in retired}


def current_json(raw: bytes | str) -> str:
    return json.dumps(current(json.loads(raw)))
