"""Public GMX editor inputs preserve the runtime's financial bounds."""

from copy import deepcopy

import pytest
import yaml

from almanak.configuration.unresolved import parse_inputs
from almanak.configuration.yaml import parse
from tests.test_dx_schemas import ROOT, validator
from tests.test_reference_conversion import (
    effective,
    legacy_snapshot,
    resolved_positions,
    snapshot_index,
)

# Author configurations only; the economic backtest files follow the backtest schema.
REFERENCES = sorted((ROOT / "strategies").glob("gmx_v2_*/config.yaml"))
# Resolution replays frozen market facts; authoring templates postdate the legacy snapshots.
SNAPSHOTTED = [path for path in REFERENCES if path.parent.name in snapshot_index()]


def resolve_public(raw, name):
    """Exercise current inputs with frozen market facts, without provider access."""
    inputs = parse_inputs(parse(yaml.safe_dump(raw))).inputs
    return resolved_positions(inputs, effective(legacy_snapshot(name), "config.yaml"))


@pytest.mark.parametrize("path", SNAPSHOTTED, ids=lambda path: path.parent.name)
def test_gmx_editor_and_runtime_accept_current_reference(path):
    raw = yaml.safe_load(path.read_text())
    validator("strategy.schema.json").validate(raw)
    assert resolve_public(raw, path.parent.name)


@pytest.mark.parametrize("path", SNAPSHOTTED, ids=lambda path: path.parent.name)
@pytest.mark.parametrize(
    "mutation",
    [
        "unknown_allocation_field",
        "unquoted_quantity",
        "negative_quantity",
        "zero_duration",
        "unknown_market_field",
        "unknown_contract",
        "bare_chain",
        "missing_event_bound",
    ],
)
def test_gmx_editor_and_runtime_reject_malformed_reference(path, mutation):
    raw = deepcopy(yaml.safe_load(path.read_text()))
    allocation = next(iter(raw["strategy"]["allocations"].values()))
    market = next(iter(raw["markets"].values()))
    if mutation == "unknown_allocation_field":
        allocation["unexpected"] = 1
    elif mutation == "unquoted_quantity":
        allocation["execution_fee"]["quantity"] = 0.1
    elif mutation == "negative_quantity":
        allocation["execution_fee"]["quantity"] = {"base_units": -1}
    elif mutation == "zero_duration":
        allocation["hold_seconds"] = "0s"
    elif mutation == "unknown_market_field":
        market["unexpected"] = 1
    elif mutation == "unknown_contract":
        market["contracts"] = {"unknown_handler": "0x" + "12" * 20}
    elif mutation == "bare_chain":
        market["chain"] = "42161"
    else:
        allocation.pop("maximum_event_block_span")
    assert not validator("strategy.schema.json").is_valid(raw)
    with pytest.raises((ValueError, KeyError)):
        resolve_public(raw, path.parent.name)


@pytest.mark.parametrize("kind", ["gmx_swap", "gmx_liquidity"])
def test_gmx_exchange_variants_require_their_author_discriminator(kind):
    suffix = "swap" if kind == "gmx_swap" else "liquidity"
    name = f"gmx_v2_arbitrum_{suffix}"
    raw = yaml.safe_load((ROOT / "strategies" / name / "config.yaml").read_text())
    next(iter(raw["strategy"]["allocations"].values())).pop("kind")
    assert not validator("strategy.schema.json").is_valid(raw)
    with pytest.raises((ValueError, KeyError)):
        resolve_public(raw, name)


def test_position_editor_rejects_an_unrepresentable_entry_size():
    name = "gmx_v2_arbitrum_position"
    raw = yaml.safe_load((ROOT / "strategies" / name / "config.yaml").read_text())
    next(iter(raw["strategy"]["allocations"].values()))["entry_size_usd_e30"] = 2**128
    assert not validator("strategy.schema.json").is_valid(raw)
    with pytest.raises(ValueError):
        resolve_public(raw, name)


@pytest.mark.parametrize(
    "name,field,value",
    [
        ("gmx_v2_arbitrum_position", "maximum_input", {"base_units": 1}),
        ("gmx_v2_arbitrum_position", "maximum_size_delta_usd_e30", 1),
        ("gmx_v2_arbitrum_position", "maximum_price_per_base_unit", 1),
        ("gmx_v2_arbitrum_collateral_path", "maximum_collateral_withdrawal_base_units", 1),
        ("gmx_v2_arbitrum_swap", "maximum_market_token_input", {"base_units": 0}),
        ("gmx_v2_arbitrum_liquidity", "maximum_market_token_input", {"base_units": 1}),
    ],
)
def test_editor_and_runtime_refuse_retired_budget_fields(name, field, value):
    """Grants are scope-only (blueprint 27); the budget fields that sized quotas are unknown."""
    raw = yaml.safe_load((ROOT / "strategies" / name / "config.yaml").read_text())
    next(iter(raw["strategy"]["allocations"].values()))[field] = value
    assert not validator("strategy.schema.json").is_valid(raw)
    with pytest.raises(ValueError):
        resolve_public(raw, name)


@pytest.mark.parametrize("path", REFERENCES, ids=lambda path: path.parent.name)
def test_public_registries_accept_the_shipped_reference(path):
    raw = yaml.safe_load(path.read_text())
    validator("strategy.schema.json").validate(raw)
    validator("markets.schema.json").validate(raw["markets"])
    for position in raw["strategy"]["allocations"].values():
        validator("positions.schema.json").validate(position)
