"""Recorded lending captures read through today's scope-only author contract.

Retained lifecycle captures predate scope-only authority (blueprint 27). Component
tests reuse their observations and outcomes, so their lending allocations and
capabilities are read in the current interface. Each translation keeps the
recorded economic meaning:

- `capital` is the amount the allocation started with, which the old funding
  statement required: `entry_amount`, else `supply_limit`.
- `borrow` is true where the old allocation could borrow (`borrow_limit` above zero).
- Aave V4 supply `amount` was its entry supply, so it becomes `capital`; a debt's
  `amount` was only a budget and is dropped.
- Capabilities lose their amount bounds; reserve capabilities gain `borrow`.
- `repayment_ceiling` (removed by blueprint 25) is dropped where a capture still has it.
- Captures that predate `asset_metadata` gain the verified precision of their lending
  reserve assets, which the reference strategies read to size their borrowing.
- Captures that predate `strategy.parameters` gain exactly the knobs their reference's
  model declares, at its `config.yaml` values, which are the constants the capture ran
  with. A caller names the reference; a single-connector capture otherwise gets its
  connector's reference.
- Permission partitions lose their quotas. Their recorded grants stay as captured, so
  a test that compares them with today's derivation still needs a fresh capture.

The raw captures are unchanged on disk.
"""

import gzip
import json
from pathlib import Path

import yaml

from almanak.typed_parameters import model_fields

STRATEGIES = Path(__file__).parents[1] / "strategies"

POOL_RESERVES = {"aave_v3_reserve", "sparklend_reserve"}
LIMITED_RESERVES = POOL_RESERVES | {"compound_v3_reserve", "morpho_blue_reserve"}
AAVE_V4_CAPABILITIES = {"aave_supply", "aave_borrow"}
# On-chain `decimals()` of the reserve assets the retained lending captures use, as their
# resolved author files recorded them.
CAPTURED_DECIMALS = {
    "eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": 6,
    "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": 18,
    "eip155:43114/erc20:0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7": 18,
    "eip155:43114/erc20:0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e": 6,
}

# The reference a single-connector capture replays through when its caller names none.
CONNECTOR_REFERENCES = {
    "aave_v4": "aave_v4_weth_supply",
    "aave_v3": "aave_v3_ethereum_lending",
    "sparklend": "sparklend_ethereum_lending",
    "compound_v3": "compound_v3_ethereum_lending",
    "morpho_blue": "morpho_blue_ethereum_lending",
}


def captured_parameters(reference):
    """The fields `reference`'s model declares, at its `config.yaml` values."""
    fields = model_fields(STRATEGIES / reference / "strategy.py") or ()
    raw = yaml.safe_load((STRATEGIES / reference / "config.yaml").read_text())
    values = raw["strategy"].get("parameters") or {}
    return {name: values[name] for name in sorted(fields)}


def current_capture(wire, reference=None):
    """Return a recorded checkpoint wire value in the scope-only lending interface."""
    config = wire.get("config")
    if isinstance(config, dict):
        wire = {**wire, "config": current_config(config, reference)}
    return current_values(wire)


def current_config(config, reference=None):
    result = dict(config)
    if "allocations" in config:
        result["allocations"] = [current_allocation(a) for a in config["allocations"]]
    if "debts" in config:
        result["debts"] = [current_debt(d) for d in config["debts"]]
    reference = reference or CONNECTOR_REFERENCES.get(captured_connector(config))
    if reference and not config.get("parameters"):
        result["parameters"] = captured_parameters(reference)
    return with_captured_metadata(result)


def captured_connector(config):
    """The capture's connector; Aave V4 captures predate the field and name a spoke."""
    if config.get("connector"):
        return config["connector"]
    reserves = (p.get("reserve") for p in config.get("allocations", ()) if isinstance(p, dict))
    if any(isinstance(r, dict) and "spoke" in r for r in reserves):
        return "aave_v4"
    return None


def with_captured_metadata(config):
    """Add the recorded precision of lending reserve assets a capture does not name."""
    metadata = dict(config.get("asset_metadata") or {})
    for position in (*config.get("allocations", ()), *config.get("debts", ())):
        reserve = position.get("reserve") if isinstance(position, dict) else None
        asset = reserve.get("asset") if isinstance(reserve, dict) else None
        if asset in CAPTURED_DECIMALS and asset not in metadata:
            metadata[asset] = {"decimals": CAPTURED_DECIMALS[asset]}
    if not metadata:
        return config
    return {**config, "asset_metadata": metadata}


def current_allocation(allocation):
    if not isinstance(allocation, dict):
        return allocation
    if "supply_limit" in allocation:
        return current_limited(allocation)
    if not is_aave_v4(allocation):
        return allocation
    if "debt_price_source" in allocation:
        return current_debt(allocation)
    result = {k: v for k, v in allocation.items() if k != "amount"}
    return {**result, "capital": allocation["amount"]}


def current_debt(debt):
    return {k: v for k, v in debt.items() if k not in ("amount", "repayment_ceiling")}


def is_aave_v4(allocation):
    reserve = allocation.get("reserve")
    return isinstance(reserve, dict) and "spoke" in reserve and "amount" in allocation


def current_limited(allocation):
    entry = allocation.get("entry_amount")
    capital = allocation["supply_limit"] if entry is None else entry
    result = {
        k: v
        for k, v in allocation.items()
        if k not in ("supply_limit", "borrow_limit", "entry_amount", "repayment_ceiling")
    }
    return {**result, "capital": capital, "borrow": int(allocation["borrow_limit"]) > 0}


def current_values(value):
    """Capabilities and permission partitions anywhere in the capture."""
    if isinstance(value, list):
        return [current_values(item) for item in value]
    if not isinstance(value, dict):
        return value
    result = {k: current_values(v) for k, v in value.items()}
    kind = value.get("kind")
    if kind in LIMITED_RESERVES and "supply_limit" in value:
        result.pop("supply_limit")
        result.pop("repayment_ceiling", None)
        borrowing = result.pop("borrow_limit")
        result["borrow"] = int(borrowing["amount"]) > 0
    elif kind in AAVE_V4_CAPABILITIES:
        result.pop("maximum", None)
        result.pop("repayment_ceiling", None)
    if "grants" in value and "quotas" in value:
        result.pop("quotas")
    return result


def current_record(wire, reference=None):
    """A recorded checkpoint or run manifest, lending or composed, in today's interface.

    Its config goes through `current_effective`, so a composed capture's exchange and
    GMX allocations lose their retired budgets too. Outside the config only capabilities
    and permission quotas change, because the same field names are live amounts on
    recorded intents and operations.
    """
    from tests.exchange_scope import without_exchange_budgets
    from tests.legacy_references import current_effective

    wire = current_capture(wire, reference)
    if isinstance(wire.get("config"), dict):
        wire = {**wire, "config": current_effective(wire["config"])}
    return without_capability_budgets(without_exchange_budgets(wire))


def without_capability_budgets(value):
    """Exchange capabilities that held a target and retired budgets keep only the target.

    A Curve capability was its target and the `maximums` ceilings; scope-only authority
    (blueprint 27) left the target. A recorded intent also names its kind, so it is kept.
    """
    from tests.legacy_references import RETIRED_FIELDS

    if isinstance(value, list):
        return [without_capability_budgets(item) for item in value]
    if not isinstance(value, dict):
        return value
    budgets = set(value) - {"target"}
    if "target" in value and budgets and budgets <= RETIRED_FIELDS:
        return {"target": value["target"]}
    return {name: without_capability_budgets(item) for name, item in value.items()}


def current_json(raw):
    """`current_record` of recorded JSON text or bytes, as JSON text."""
    return json.dumps(current_record(json.loads(raw)))


def current_manifest(directory):
    """Rewrite the copy of a retained run manifest in `directory` in today's interface."""
    path = directory / "manifest.json"
    path.write_text(current_json(path.read_bytes()))


def read_capture(path, reference=None):
    """Read a recorded `.json` or `.json.gz` checkpoint in the current interface."""
    raw = path.read_bytes()
    if path.suffix == ".gz":
        raw = gzip.decompress(raw)
    return json.dumps(current_capture(json.loads(raw), reference))
