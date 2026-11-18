"""Retained Safe trading calls bind generated authority to exact protocol effects."""

import gzip
import json
from pathlib import Path

import pytest
from pydantic import TypeAdapter

from almanak.connectors.uniswap_v3 import abi, permissions, receipts
from almanak.connectors.uniswap_v3.authority import declared
from almanak.connectors.uniswap_v3.models import Parameters
from almanak.connectors.uniswap_v3.observations import Snapshot
from almanak.connectors.uniswap_v3.operations import ProtocolOperation
from tests.exchange_scope import assert_scope_only, without_exchange_budgets
from tests.scope_trees import assert_same_scope, manifest_grants

ROOT = Path(__file__).parents[1]


def captures():
    return json.loads(
        gzip.decompress((ROOT / "tests/fixtures/uniswap-v3-safe-trading.json.gz").read_bytes())
    )


@pytest.mark.parametrize("index", [0, 1])
def test_generated_grants_match_installed_safe_trading_and_protocol_effects(index):
    row = captures()[index]
    config = Parameters.model_validate_json(json.dumps(without_exchange_budgets(row["config"])))
    profile = config.accounts.bindings[0].profile
    grants = permissions.derive(profile, declared(config))
    # The installed capture predates scope-only grants (blueprint 27): every scope word
    # matches by value, and only its author amount bounds and quotas are history.
    assert_same_scope(grants, manifest_grants(row["manifest"]))
    assert_scope_only(grants)
    swaps = []
    for step in row["steps"]:
        operation = TypeAdapter(ProtocolOperation).validate_json(json.dumps(step["operation"]))
        assert abi.call(operation).model_dump(mode="json") == step["call"]
        assert (
            receipts.movement(step["receipt"], operation).model_dump(mode="json") == step["effects"]
        )
        assert step["receipt"]["status"] == "0x1"
        assert step["receipt"]["from"].lower() == profile.operator.address
        if operation.kind == "swap":
            swaps.append(operation)
    assert len(swaps) == 2
    assert swaps[0].amount.asset == swaps[1].minimum.asset
    assert swaps[0].minimum.asset == swaps[1].amount.asset
    assert all(operation.target.fund == profile.fund for operation in swaps)
    assert not Snapshot.model_validate_json(json.dumps(row["final"])).needs_cleanup


def test_liquidity_authority_uses_install_once_templates_and_ignores_other_funds():
    row = captures()[0]
    config = Parameters.model_validate_json(json.dumps(without_exchange_budgets(row["config"])))
    profile = config.accounts.bindings[0].profile
    capability = declared(config)
    other = profile.model_copy(
        update={"fund": profile.fund.model_copy(update={"address": "0x" + "ab" * 20})}
    )
    assert permissions.derive(other, capability) == ()
    from almanak.connectors.uniswap_v3.domain import TickRange

    allocation = config.allocations[0]
    config = config.model_copy(
        update={
            "allocations": (
                allocation.model_copy(update={"range": TickRange(lower=-500, upper=500)}),
            )
        }
    )
    grants = permissions.derive(profile, declared(config))
    manager = config.allocations[0].contracts.position_manager
    signatures = {g.signature for g in grants if g.target == manager}
    assert signatures == {
        f"mint({abi.MINT_PARAMS})",
        f"increaseLiquidity({abi.INCREASE_PARAMS})",
        f"collect({abi.COLLECT_PARAMS})",
        "multicall(bytes[])",
    }
    assert all(len(g.conditions) <= 256 for g in grants)
    from almanak.connectors.uniswap_v3.liquidity_permissions import BROAD_SCOPE

    assert sum(any(BROAD_SCOPE in e for e in g.explanation) for g in grants) == 3


def test_banded_allocation_receives_the_liquidity_grants():
    from almanak.connectors.v3_pool_domain import TickBand

    row = captures()[0]
    config = Parameters.model_validate_json(json.dumps(without_exchange_budgets(row["config"])))
    profile = config.accounts.bindings[0].profile
    allocation = config.allocations[0]
    spacing = allocation.pool.tick_spacing
    band = TickBand(lower=-60 * spacing, upper=60 * spacing, width=20 * spacing)
    config = config.model_copy(
        update={"allocations": (allocation.model_copy(update={"range": None, "band": band}),)}
    )
    grants = permissions.derive(profile, declared(config))
    manager = config.allocations[0].contracts.position_manager
    signatures = {g.signature for g in grants if g.target == manager}
    assert f"mint({abi.MINT_PARAMS})" in signatures and "multicall(bytes[])" in signatures
    assert_scope_only(grants)
